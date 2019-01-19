package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"

	promapi "github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	kubeclient "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"

	netv1a1 "k8s.io/examples/staging/kos/pkg/apis/network/v1alpha1"
	kosclientset "k8s.io/examples/staging/kos/pkg/client/clientset/versioned"
	netclientv1a1 "k8s.io/examples/staging/kos/pkg/client/clientset/versioned/typed/network/v1alpha1"
	kosinformers "k8s.io/examples/staging/kos/pkg/client/informers/externalversions"

	kosutil "k8s.io/examples/staging/kos/pkg/util"
)

const (
	slotKey = "slot"
)

const (

	// The HTTP port under which the scraping endpoint ("/metrics") is served.
	// Pick an unusual one because the host's network namespace is used.
	// See https://github.com/prometheus/prometheus/wiki/Default-port-allocations .
	MetricsAddr = ":9376"

	// The HTTP path under which the scraping endpoint ("/metrics") is served.
	MetricsPath = "/metrics"

	// The namespace, subsystem and name of the histogram collected by this controller.
	HistogramNamespace = "kos"
	HistogramSubsystem = "driver"

	rcLabel   = "RC"
	fullLabel = "full"
)

var (
	createLatencyHistogram     TrackingHistogram
	createToAddressedHistogram TrackingHistogram
	createToReadyHistogram     TrackingHistogram
	createToBrokenHistogram    TrackingHistogram
	createToTestedHistogram    TrackingHistogram
	readyToTestedHistogram     TrackingHistogram
	deleteLatencyHistogram     TrackingHistogram
	testESs                    *prometheus.CounterVec
	successfulCreates          prometheus.Counter
	failedCreates              prometheus.Counter
	successfulDeletes          prometheus.Counter
	failedDeletes              prometheus.Counter
)

type TrackingHistogram interface {
	prometheus.Histogram
	ObserveAt(x float64, ns, name string)
	DumpToLog()
}

type trackingHistogram struct {
	prometheus.Histogram
	name string

	statMu  sync.Mutex
	maxX    float64
	maxNS   string
	maxName string
}

var _ prometheus.Histogram = &trackingHistogram{}
var _ TrackingHistogram = &trackingHistogram{}

func (th *trackingHistogram) ObserveAt(x float64, ns, name string) {
	th.Histogram.Observe(x)
	th.statMu.Lock()
	defer func() { th.statMu.Unlock() }()
	if x > th.maxX {
		th.maxX = x
		th.maxNS = ns
		th.maxName = name
		// fmt.Printf("%s: maxX=%g, maxAt=%s/%s\n", th.name, th.maxX, th.maxNS, th.maxName)
	}
}

func (th *trackingHistogram) DumpToLog() {
	glog.Warningf("TrackingHistogram stats: histogram=%s, maxX=%g, maxAt=%s/%s\n", th.name, th.maxX, th.maxNS, th.maxName)
}

func NewTrackingHistogram(opts prometheus.HistogramOpts) TrackingHistogram {
	h := prometheus.NewHistogram(opts)
	return &trackingHistogram{Histogram: h, name: opts.Name}
}

var theKubeNS string

type NamespacedName struct{ Namespace, Name string }

var multiOwnerWarnings uint32

type VirtNet struct {
	ID        uint32
	size      int
	addr0I    uint32
	addrFI    uint32
	prefixLen int
	subnets   []*netv1a1.Subnet
	theCD     *ConnectivityDomain
	slots     []Slot

	// createdCount is the number of attachments created so far in this VirtNet.
	// Access only with atomic.
	createdCount uint32

	addrsMutex sync.Mutex

	// addrs maps IPv4 address to the set of attachments that have it.
	// Maps address to set of NetworkAttachment name.
	// Access only while holding addrsMutex.
	addrs map[string]map[string]struct{}

	// nodeMutex must be held while accessing any variable thing reachable from the following fields.
	// Do not try to acquire an Slot's mutex while holding this mutex.
	nodeMutex sync.Mutex

	// nodeMap maps node name to node data relevant to this namespace
	nodeMap map[string]*NodeData
}

type NodeData struct {
	// ipToName maps attachment IPv4 address (as it appears in a NetworkAttachment.Status) to attachment name
	ipToName map[string]string
}

// ConnectivityDomain holds the testing information tracked for a virtual
// network.  Its lock may be acquired while holding a Slot's lock, not
// the other way around.
type ConnectivityDomain struct {
	//	name string
	VNI uint32
	sync.Mutex
	change *sync.Cond

	// numWaiters is the number of threads waiting for a slot that passed test
	numWaiters int

	// startOfFirstWait is when the first of the current set of waiters began waiting
	startOfFirstWait time.Time

	// latestIndex is the index of the slot that most recently passed test
	latestIndex int

	// pendingIndices is the indices of the slots between create and successfully tested
	pendingIndices map[int]struct{}
}

// NoteTested records the given index.
// Called while holding the Slot's mutex.
func (cd *ConnectivityDomain) NoteTested(slotIndex int) {
	if cd == nil {
		return
	}
	cd.Lock()
	delete(cd.pendingIndices, slotIndex)
	cd.latestIndex = slotIndex
	cd.change.Broadcast()
	cd.Unlock()
}

// NotePending removes the given index.
// Called while holding the Slot's mutex.
func (cd *ConnectivityDomain) NotePending(slotIndex int) {
	if cd == nil {
		return
	}
	cd.Lock()
	cd.pendingIndices[slotIndex] = struct{}{}
	cd.Unlock()
}

// NoteNoTest removes the given index.
// Called while holding the Slot's mutex.
func (cd *ConnectivityDomain) NoteNoTest(slotIndex int) {
	if cd == nil {
		return
	}
	cd.Lock()
	delete(cd.pendingIndices, slotIndex)
	if len(cd.pendingIndices) == 0 {
		cd.change.Broadcast()
	}
	if cd.latestIndex == slotIndex {
		cd.latestIndex = -1
	}
	cd.Unlock()
}

// GetReadyAttachment returns, if reasonably possible, the most recently
// ready NetworkAttachment in the connectivity domain.  If there is none
// at entry time, but there are some NetworkAttachments that have been
// created but not yet reached their main state, then this procedure
// will wait up to a configured limit for one to become ready.  Called
// while holding the Slot's mutex.
func (cd *ConnectivityDomain) GetReadyAttachment(virtNet *VirtNet) (natt *netv1a1.NetworkAttachment, delay, totalDelay time.Duration) {
	cd.Lock()
	defer func() { cd.Unlock() }()
	if cd.latestIndex == -1 && len(cd.pendingIndices) > 0 {
		cd.numWaiters++
		if cd.numWaiters == 1 {
			cd.startOfFirstWait = time.Now()
		}
		tBefore := time.Now()
		tLimit := tBefore.Add(*pendingWait)
		for cd.latestIndex == -1 && len(cd.pendingIndices) > 0 {
			now := time.Now()
			if !tLimit.After(now) {
				break
			}
			TimeLimitedWait(cd.change, tLimit.Sub(now))
		}
		delay = time.Since(tBefore)
		cd.numWaiters--
		if cd.numWaiters == 0 {
			wait := time.Since(cd.startOfFirstWait)
			totalDelay = time.Duration(atomic.AddInt64(&xtraDelayI, int64(wait)))
		}
	}
	if cd.latestIndex == -1 {
		return
	}
	natt = virtNet.slots[cd.latestIndex].natt
	return
}

func TimeLimitedWait(cond *sync.Cond, maxDelta time.Duration) {
	timer := time.AfterFunc(maxDelta, func() { cond.Broadcast() })
	cond.Wait()
	timer.Stop()
}

type Slot struct {
	nextIndex uint32 // access only in RunThread

	// Hold this mutex while accessing any of the following fields.
	// It is allowed to hold this mutex while trying to acquire
	// a VirtNet.nodeMutex, but not the other way around.
	sync.Mutex
	currentAttachmentName string
	currentNodeName       string
	preCreateTime         time.Time
	postCreateTime        time.Time
	addressedTime         time.Time
	readyTime             time.Time
	fullTest              bool
	testES                int32
	testedTime            time.Time
	brokenTime            time.Time
	natt                  *netv1a1.NetworkAttachment
}

// setCurrentName prepares an attachment slot for use for a new attachment
func (slot *Slot) setCurrentName(slotIndex int, currentAttachmentName, currentNodeName string, cd *ConnectivityDomain) {
	slot.Mutex.Lock()
	defer func() { slot.Mutex.Unlock() }()
	slot.currentAttachmentName = currentAttachmentName
	slot.currentNodeName = currentNodeName
	slot.preCreateTime = time.Time{}
	slot.postCreateTime = time.Time{}
	slot.addressedTime = time.Time{}
	slot.readyTime = time.Time{}
	slot.testedTime = time.Time{}
	slot.brokenTime = time.Time{}
	slot.natt = nil
	if currentAttachmentName == "" {
		cd.NoteNoTest(slotIndex)
	} else {
		cd.NotePending(slotIndex)
	}
	slot.testES = -5
}

// setNetAtt updates an attachment slot for a freshly created attachment
func (slot *Slot) setNetAtt(virtNet *VirtNet, slotIndex int, preCreateTime, postCreateTime time.Time, natt *netv1a1.NetworkAttachment, fullTest bool) {
	slot.Mutex.Lock()
	defer func() { slot.Mutex.Unlock() }()
	slot.preCreateTime = preCreateTime
	slot.postCreateTime = postCreateTime
	slot.natt = natt
	slot.fullTest = fullTest
}

// observeState updates an attachment slot with a fresh notification
func (slot *Slot) observeState(virtNet *VirtNet, slotIndex int, natt *netv1a1.NetworkAttachment) {
	now := time.Now()
	slot.Mutex.Lock()
	defer func() { slot.Mutex.Unlock() }()
	cd := virtNet.theCD
	if natt.Name != slot.currentAttachmentName || natt.Namespace != theKubeNS {
		glog.Infof("Tardy notification: attachment=%s/%s, RV=%s, currentAttachmentName=%s, subnet=%s, addressVNI=%06x, ipv4=%s, ifcName=%q, errors=%#+v\n", natt.Namespace, natt.Name, natt.ResourceVersion, slot.currentAttachmentName, natt.Spec.Subnet, natt.Status.AddressVNI, natt.Status.IPv4, natt.Status.IfcName, natt.Status.Errors)
		return
	}
	oldIPv4 := ""
	if slot.natt != nil {
		oldIPv4 = slot.natt.Status.IPv4
	}
	if oldIPv4 != natt.Status.IPv4 {
		if oldIPv4 == "" {
			glog.Infof("Attachment got IP address: attachment=%s/%s, VNI=%06x, subnet=%s, RV=%s, preCreateTime=%s, now=%s, node=%s, ipv4=%s\n", theKubeNS, slot.currentAttachmentName, virtNet.ID, natt.Spec.Subnet, natt.ResourceVersion, slot.preCreateTime.Format(kosutil.RFC3339Milli), now.Format(kosutil.RFC3339Milli), natt.Spec.Node, natt.Status.IPv4)
		} else {
			glog.Infof("Attachment changed IP address: attachment=%s/%s, VNI=%06x, subnet=%s, RV=%s, node=%s, oldIPv4=%s, newIPv4=%s\n", theKubeNS, slot.currentAttachmentName, virtNet.ID, natt.Spec.Subnet, natt.ResourceVersion, natt.Spec.Node, oldIPv4, natt.Status.IPv4)
		}
		if natt.Status.IPv4 != "" {
			addr := net.ParseIP(natt.Status.IPv4)
			if addr == nil {
				glog.Infof("Error parsing NetworkAttachment.Status.IPv4: attachment=%s/%s, VNI=%06x, subnet=%s, node=%s, ipv4=%s\n", theKubeNS, natt.Name, virtNet.ID, natt.Spec.Subnet, natt.Spec.Node, natt.Status.IPv4)
			} else if addrI := kosutil.MakeUint32FromIPv4(addr); addrI < virtNet.addr0I+2 || addrI >= virtNet.addrFI {
				glog.Infof("NetworkAttachment.Status.IPv4 is not in range: attachment=%s/%s, VNI=%06x, subnet=%s, node=%s, ipv4=%s\n", theKubeNS, natt.Name, virtNet.ID, natt.Spec.Subnet, natt.Spec.Node, natt.Status.IPv4)
			}
		}
		virtNet.ipAddressChanged(slot, slot.currentNodeName, oldIPv4, natt.Status.IPv4, slot.currentAttachmentName)
	}
	if natt.Status.IPv4 != "" {
		if slot.addressedTime == (time.Time{}) {
			slot.addressedTime = now
		}
	}
	if natt.Status.IfcName != "" {
		if slot.readyTime == (time.Time{}) {
			slot.readyTime = now
			glog.Infof("Attachment became ready: attachment=%s/%s, VNI=%06x, subnet=%s, RV=%s, node=%s, preCreateTime=%s, addressedTime=%s, readyTime=%s, ifcName=%s, MAC=%s, ipv4=%s\n", theKubeNS, slot.currentAttachmentName, virtNet.ID, natt.Spec.Subnet, natt.ResourceVersion, natt.Spec.Node, slot.preCreateTime.Format(kosutil.RFC3339Milli), slot.addressedTime.Format(kosutil.RFC3339Milli), slot.readyTime.Format(kosutil.RFC3339Milli), natt.Status.IfcName, natt.Status.MACAddress, natt.Status.IPv4)
			if *waitAfterCreate != 0 {
				count := atomic.AddUint32(&readyCount, 1)
				rem := uint32(*num_attachments) - count
				if rem&(rem-1) == 0 {
					glog.Warningf("Number remaining = %d\n", rem)
				}
			}
		}
		if slot.testedTime == (time.Time{}) && natt.Status.PostCreateExecReport != nil {
			slot.testedTime = now
			cr := natt.Status.PostCreateExecReport
			slot.testES = cr.ExitStatus
			if cr.ExitStatus != 0 {
				glog.Infof("Non-zero test result code: attachment=%s/%s, VNI=%06x, subnet=%s, RV=%s, node=%s, testES=%d, StartTime=%s, StopTime=%s, StdOut=%q, StdErr=%q\n", theKubeNS, slot.currentAttachmentName, virtNet.ID, natt.Spec.Subnet, natt.ResourceVersion, natt.Spec.Node, cr.ExitStatus, cr.StartTime, cr.StopTime, cr.StdOut, cr.StdErr)
			}
			if slot.testES == 0 {
				cd.NoteTested(slotIndex)
			} else {
				cd.NoteNoTest(slotIndex)
				if *stopOnPingFail {
					atomic.AddUint32(&stoppers, 1)
				}
			}
			glog.Infof("Attachment was tested: attachment=%s/%s, VNI=%06x, subnet=%s, RV=%s, node=%s, preCreateTime=%s, postCreateTime=%s, addressedTime=%s, readyTime=%s, testedTime=%s, fullTest=%v, testES=%d, ipv4=%s\n", theKubeNS, slot.currentAttachmentName, virtNet.ID, natt.Spec.Subnet, natt.ResourceVersion, natt.Spec.Node, slot.preCreateTime.Format(kosutil.RFC3339Milli), slot.postCreateTime.Format(kosutil.RFC3339Milli), slot.addressedTime.Format(kosutil.RFC3339Milli), slot.readyTime.Format(kosutil.RFC3339Milli), slot.testedTime.Format(kosutil.RFC3339Milli), slot.fullTest, slot.testES, natt.Status.IPv4)
		}
	} else if len(natt.Status.Errors.IPAM) > 0 || len(natt.Status.Errors.Host) > 0 {
		if slot.brokenTime == (time.Time{}) {
			cd.NoteNoTest(slotIndex)
			slot.brokenTime = now
			glog.Infof("Observed broken state: attachment=%s/%s, VNI=%06x, subnet=%s, RV=%s, node=%s, preCreateTime=%s, postCreateTime=%s, addressedTime=%s, ipv4=%s, errors=%#+v\n", theKubeNS, natt.Name, virtNet.ID, natt.Spec.Subnet, natt.ResourceVersion, natt.Spec.Node, slot.preCreateTime.Format(kosutil.RFC3339Milli), slot.postCreateTime.Format(kosutil.RFC3339Milli), slot.addressedTime.Format(kosutil.RFC3339Milli), natt.Status.IPv4, natt.Status.Errors)
			if *stopOnBreak {
				atomic.AddUint32(&stoppers, 1)
			}
		}
	}
	slot.natt = natt
}

// ipAddressChanged changes the IPv4 address of a NetworkAttachment.
// Caller may hold Slot's mutex.
func (virtNet *VirtNet) ipAddressChanged(slot *Slot, currentNodeName, oldIPv4, newIPv4, currentAttachmentName string) {
	virtNet.ipAddressChangedAtNode(slot, currentNodeName, oldIPv4, newIPv4, currentAttachmentName)
	virtNet.ipAddressChangedInNS(slot, oldIPv4, newIPv4, currentAttachmentName)
}

func (virtNet *VirtNet) ipAddressChangedInNS(slot *Slot, oldIPv4, newIPv4, currentAttachmentName string) {
	var warnSet string
	virtNet.addrsMutex.Lock()
	defer func() {
		virtNet.addrsMutex.Unlock()
		if warnSet != "" {
			glog.Infof("IP address has multiple owners: ipv4=%s, VNI=%06x, owners=%s\n", newIPv4, virtNet.ID, warnSet)
			atomic.AddUint32(&multiOwnerWarnings, 1)
		}
	}()
	if oldIPv4 != "" {
		oldSet := virtNet.addrs[oldIPv4]
		if oldSet != nil {
			delete(oldSet, currentAttachmentName)
			if len(oldSet) == 0 {
				delete(virtNet.addrs, oldIPv4)
			}
		}
	}
	if newIPv4 != "" {
		newSet := virtNet.addrs[newIPv4]
		if newSet == nil {
			newSet = make(map[string]struct{})
			virtNet.addrs[newIPv4] = newSet
		}
		newSet[currentAttachmentName] = struct{}{}
		if len(newSet) > 1 {
			warnSet = fmt.Sprintf("%s", newSet)
		}
	}
}

func (virtNet *VirtNet) ipAddressChangedAtNode(slot *Slot, currentNodeName, oldIPv4, newIPv4, currentAttachmentName string) {
	virtNet.nodeMutex.Lock()
	defer func() { virtNet.nodeMutex.Unlock() }()
	nd := virtNet.nodeMap[currentNodeName]
	if nd == nil {
		nd = &NodeData{
			ipToName: make(map[string]string)}
		virtNet.nodeMap[currentNodeName] = nd
	}
	if oldIPv4 != "" {
		oldName := nd.ipToName[oldIPv4]
		if oldName == currentAttachmentName {
			delete(nd.ipToName, oldIPv4)
		} else {
			glog.Infof("Old address already has new owner: oldEdpoint=%s/%s, newAttachment=%s/%s, VNI=%06x, node=%s, oldIPv4=%s, newIPv4=%s\n", theKubeNS, oldName, theKubeNS, currentAttachmentName, virtNet.ID, currentNodeName, oldIPv4, newIPv4)
		}
	}
	if newIPv4 != "" {
		currentOwner := nd.ipToName[newIPv4]
		if currentOwner != "" {
			glog.Infof("New address already has owner: attachment=%s/%s, VNI=%06x, node=%s, owner=%s, newIPv4=%s\n", theKubeNS, currentAttachmentName, virtNet.ID, currentNodeName, currentOwner, newIPv4)
		}
		nd.ipToName[newIPv4] = currentAttachmentName
	}
}

// close resets an attachment slot after the current attachment is deleted
func (slot *Slot) close(VNI uint32, nsName string) *netv1a1.NetworkAttachment {
	slot.Mutex.Lock()
	defer func() {
		slot.currentAttachmentName = ""
		slot.currentNodeName = ""
		slot.natt = nil
		slot.preCreateTime = time.Time{}
		slot.Mutex.Unlock()
	}()
	if slot.preCreateTime == (time.Time{}) {
		return slot.natt
	}
	virtNet := idToVirtNet[VNI]
	// now we know slot.natt != nil
	if slot.addressedTime != (time.Time{}) {
		createToAddressedHistogram.ObserveAt(slot.addressedTime.Sub(slot.preCreateTime).Seconds(), nsName, slot.natt.Name)
	}
	if slot.natt.Status.IPv4 != "" {
		virtNet.ipAddressChanged(slot, slot.currentNodeName, slot.natt.Status.IPv4, "", slot.currentAttachmentName)
	}
	if slot.readyTime != (time.Time{}) {
		createToReadyHistogram.ObserveAt(slot.readyTime.Sub(slot.preCreateTime).Seconds(), nsName, slot.natt.Name)
	}
	if slot.brokenTime != (time.Time{}) {
		createToBrokenHistogram.ObserveAt(slot.brokenTime.Sub(slot.preCreateTime).Seconds(), nsName, slot.natt.Name)
	}
	if slot.testedTime != (time.Time{}) {
		createToTestedHistogram.ObserveAt(slot.testedTime.Sub(slot.preCreateTime).Seconds(), nsName, slot.natt.Name)
		readyToTestedHistogram.ObserveAt(slot.testedTime.Sub(slot.readyTime).Seconds(), nsName, slot.natt.Name)
		testESs.With(prometheus.Labels{rcLabel: strconv.FormatInt(int64(slot.testES), 10), fullLabel: strconv.FormatBool(slot.fullTest)}).Add(1)
	}
	if slot.addressedTime == (time.Time{}) {
		glog.Infof("Attachment got no address: attachment=%s/%s, VNI=%06x, node=%s\n", nsName, slot.currentAttachmentName, VNI, slot.currentNodeName)
	} else if slot.readyTime == (time.Time{}) && slot.brokenTime == (time.Time{}) {
		glog.Infof("Attachment got no state: attachment=%s/%s, VNI=%06x, node=%s\n", nsName, slot.currentAttachmentName, VNI, slot.currentNodeName)
	} else if slot.testedTime == (time.Time{}) && !*omitTest {
		glog.Infof("Attachment test did not complete: attachment=%s/%s, VNI=%06x, node=%s\n", nsName, slot.currentAttachmentName, VNI, slot.currentNodeName)
	}
	return slot.natt
}

type VirtNetAttachment struct {
	vnIndex, slotIndex int
}

var num_subnets int

var virtNets []VirtNet
var idToVirtNet = make(map[uint32]*VirtNet)
var subnetToVirtNet = make(map[string]*VirtNet)
var initializedSubnets = map[string]struct{}{}
var initializedSubnetsMutex sync.Mutex
var initializedSubnetsChanged = sync.NewCond(&initializedSubnetsMutex)

var vnAttachments []VirtNetAttachment

type subnetEventHandler struct{}

func (neh subnetEventHandler) OnAdd(obj interface{}) {
	subnet := obj.(*netv1a1.Subnet)
	virtNet := idToVirtNet[subnet.Spec.VNI]
	if virtNet == nil {
		return
	}
	glog.Infof("Notified about subnet %s, VNI=%06x\n", subnet.Name, subnet.Spec.VNI)
	initializedSubnetsMutex.Lock()
	defer func() { initializedSubnetsMutex.Unlock() }()
	initializedSubnets[subnet.Name] = struct{}{}
	initializedSubnetsChanged.Broadcast()
}

func (neh subnetEventHandler) OnUpdate(oldObj, newObj interface{}) {
	neh.OnAdd(newObj)
}

func (neh subnetEventHandler) OnDelete(obj interface{}) {
	return
}

type attachmentEventHandler struct{}

func (eeh *attachmentEventHandler) OnAdd(obj interface{}) {
	natt := obj.(*netv1a1.NetworkAttachment)
	if natt.Annotations == nil {
		return
	}
	virtNet := subnetToVirtNet[natt.Spec.Subnet]
	if virtNet == nil {
		return
	}
	slotIndex, err := strconv.Atoi(natt.Annotations[slotKey])
	if err != nil {
		return
	}
	slot := &virtNet.slots[slotIndex]
	slot.observeState(virtNet, slotIndex, natt)
}

func (eeh *attachmentEventHandler) OnUpdate(oldObj, newObj interface{}) {
	eeh.OnAdd(newObj)
}

func (eeh *attachmentEventHandler) OnDelete(obj interface{}) {
	natt := obj.(*netv1a1.NetworkAttachment)
	glog.Infof("Notified of attachment deletion: attachment=%s/%s, subnet=%s, RV=%s, node=%s, addressVNI=%06x, ipv4=%s, ifcName=%q\n", natt.Namespace, natt.Name, natt.Spec.Subnet, natt.ResourceVersion, natt.Spec.Node, natt.Status.AddressVNI, natt.Status.IPv4, natt.Status.IfcName)
}

func waitForInitializedSubnets() {
	initializedSubnetsMutex.Lock()
	defer func() { initializedSubnetsMutex.Unlock() }()
	for len(initializedSubnets) < num_subnets {
		initializedSubnetsChanged.Wait()
	}
	return
}

var addr0S = flag.String("base-address", "172.24.0.0", "Start of IP address range to use")
var num_nets = flag.Int("num-nets", 10, "Number of namespaces to use")
var top_net_size = flag.Int("top-net-size", 100, "Largest number of slots in a virtual network")
var law_power = flag.Float64("exponent", 1, "exponent in power law")
var law_bias = flag.Int("bias", 0, "bias in power law")
var just_count = flag.Bool("estimate", false, "only characterize the network size distribution")
var roundRobin = flag.Bool("round-robin", false, "pick Nodes round-robin")
var singleNetwork = flag.Bool("single-network", false, "indicates whether to make only one Subnet in each VirtNet")
var omitTest = flag.Bool("omit-test", false, "indicates whether to avoid functional testing of the created attachments")
var pendingWait = flag.Duration("pending-wait", time.Minute, "max time a thread will wait for a pending attachment to become ready")
var pingCount = flag.Int("ping-count", 10, "number of ping requests in a full test")
var stopOnPingFail = flag.Bool("stop-on-ping-fail", true, "stop diriving as soon as one ping test fails")

var stopOnBreak = flag.Bool("stop-on-break", true, "stop driving as soon as breakage is observed")
var waitAfterCreate = flag.Duration("wait-after-create", 0, "if non-zero, wait this amount of time and then exit after creating all the attachments")
var kubeconfigPath = flag.String("kubeconfig", "", "Path to kubeconfig file")
var num_attachments = flag.Int("num-attachments", 450, "Total number of attachments to create")
var threads = flag.Uint64("threads", 1, "Total number of threads to use")
var targetRate = flag.Float64("rate", 10, "Target aggregate rate, in ops/sec")
var subnetSizeFactor = flag.Float64("subnet-size-factor", 1.0, "size each subnet for this factor times the number of addresses needed")
var onlyNode = flag.String("only-node", "", "node, if any, to be the exclusive location of attachments")
var nodeLabelSelector = flag.String("node-label-selector", "", "label-selector, if any, to restrict which nodes get attachments")

var runID = flag.String("runid", "", "unique ID of this run (default is randomly generated)")

var stoppers uint32
var readyCount uint32
var nnsCount uint32
var xtraDelayI int64 // a time.Duration

func main() {

	flag.Set("stderrthreshold", "WARNING")
	flag.Parse()

	if *runID == "" {
		now := time.Now()
		rand.Seed(now.UnixNano())
		rand.Int63()
		rand.Int63()
		_, M, D := now.Date()
		h, m, _ := now.Clock()
		*runID = fmt.Sprintf("%02d%02d-%02d%02d-%04d", M, D, h, m, rand.Intn(10000))
	} else if good, _ := regexp.MatchString("^[-a-zA-Z0-9!@#$%^&()+=][-a-zA-Z0-9!@#$%^&()+=.]*$", *runID); !good {
		glog.Errorf("runid=%q does not match regular expression ^[-a-zA-Z0-9!@#$%%^&()+=][-a-zA-Z0-9!@#$%%^&()+=.]*$\n", *runID)
		os.Exit(2)
	}
	theKubeNS = *runID
	outputDir := *runID
	err := os.MkdirAll(outputDir, os.ModePerm|os.ModeDir)
	if err != nil {
		glog.Errorf("Failed to create output directory %q: %s\n", outputDir, err.Error())
		os.Exit(3)
	}

	addr0 := net.ParseIP(*addr0S)
	if addr0 == nil {
		glog.Errorf("Failed to parse base address %q\n", *addr0S)
		os.Exit(5)
	}

	glog.Warningf("Driver parameters: num_nets=%d, top_net_size=%d, subnetSizeFactor=%g, law_powr=%g, law_bias=%d, just_count=%v, roundRobin=%v, pendingWait=%s, stopOnPingFail=%v, singleNetwork=%v, kubeconfigPath=%q, num_attachments=%d, threads=%d, targetRate=%g, waitAfterCreate=%s, runID=%q\n", *num_nets, *top_net_size, *subnetSizeFactor, *law_power, *law_bias, *just_count, *roundRobin, *pendingWait, *stopOnPingFail, *singleNetwork, *kubeconfigPath, *num_attachments, *threads, *targetRate, *waitAfterCreate, *runID)

	vni0 := rand.Intn(32)*65536 + 1 // allow 64K VNIs in a run without overflowing the 21 bit limit
	glog.Warningf("First VNI is %06x\n", vni0)

	distributionFilename := filepath.Join(outputDir, "size-distribution.csv")
	distributionOutlineFilename := filepath.Join(outputDir, "size-distribution-outline.csv")

	distributionCSVFile, err := os.Create(distributionFilename)
	if err != nil {
		panic(err)
	}

	var urClientset *kubeclient.Clientset
	var kClientset *kosclientset.Clientset
	var netsDirect netclientv1a1.SubnetInterface

	if !*just_count {
		/* connect to the API server */
		config, err := clientcmd.BuildConfigFromFlags("", *kubeconfigPath)
		if err != nil {
			glog.Errorf("Unable to get kube client config: %s\n", err.Error())
			os.Exit(20)
		}
		config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

		urClientset, err = kubeclient.NewForConfig(config)
		if err != nil {
			glog.Errorf("Failed to create a k8s clientset: %s\n", err.Error())
			os.Exit(21)
		}
		kClientset, err = kosclientset.NewForConfig(config)
		if err != nil {
			glog.Errorf("Failed to create a KOS clientset: %s\n", err.Error())
			os.Exit(22)
		}
		_, err = urClientset.CoreV1().Namespaces().Create(
			&k8sv1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   theKubeNS,
					Labels: map[string]string{"app": "attachment-tput-driver", "runid": *runID},
				},
			})
		if err != nil {
			glog.Errorf("Failed to create namespace %q: %s\n", theKubeNS, err.Error())
			os.Exit(23)
		}
		netsDirect = kClientset.NetworkV1alpha1().Subnets(theKubeNS)
	}

	virtNets = make([]VirtNet, *num_nets)
	breakPoint := 0
	var sizeSum, squareSum float64
	addr01I := kosutil.MakeUint32FromIPv4(addr0)
	for i := 0; i < *num_nets; i++ {
		sz := math.Ceil(float64(1+*law_bias) * float64(*top_net_size) / math.Pow(float64(i+1+*law_bias), *law_power))
		if sz < 1 {
			sz = 1
		}
		if sz == 1 && breakPoint == 0 {
			breakPoint = i + 1
		}
		nsz := int(sz)
		nsz2 := nsz / 2
		nsz1 := nsz - nsz2
		if *singleNetwork {
			nsz1, nsz2 = nsz, 0
		}
		suffixBits := int(math.Ceil(math.Log2(float64(nsz1)*
			*subnetSizeFactor + 3.0)))
		ssz := uint32(1) << uint(suffixBits)
		addrF1I := addr01I + ssz - 1
		addrF2I := addrF1I
		VNI := uint32(vni0+i) & 0x1FFFFF
		var subnets []*netv1a1.Subnet
		var cd *ConnectivityDomain
		if !*just_count {
			cd = &ConnectivityDomain{VNI: VNI, pendingIndices: make(map[int]struct{}), latestIndex: -1}
			cd.change = sync.NewCond(cd)
			net1name := fmt.Sprintf("%06x-a", VNI)
			net2name := fmt.Sprintf("%06x-b", VNI)
			addr01 := kosutil.MakeIPv4FromUint32(addr01I)
			net1, err := netsDirect.Create(
				&netv1a1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:   net1name,
						Labels: map[string]string{"app": "attachment-tput-driver", "runid": *runID},
					},
					Spec: netv1a1.SubnetSpec{
						VNI:  VNI,
						IPv4: fmt.Sprintf("%s/%d", addr01, 32-suffixBits),
					},
				})
			if err != nil {
				glog.Errorf("Failed to create Subnet %s/%s, VNI=%06x, err=%s\n", theKubeNS, net1name, VNI, err.Error())
				os.Exit(26)
			}
			num_subnets++
			subnets = []*netv1a1.Subnet{net1}
			netRefs := []apimachinerytypes.NamespacedName{{theKubeNS, net1name}}
			subnetCIDRs := []string{net1.Spec.IPv4}
			var net2 *netv1a1.Subnet
			if nsz2 > 0 {
				addr02I := addrF1I + 1
				addrF2I = addrF1I + ssz
				addr02 := kosutil.MakeIPv4FromUint32(addr02I)
				net2, err = netsDirect.Create(
					&netv1a1.Subnet{
						ObjectMeta: metav1.ObjectMeta{
							Name:   net2name,
							Labels: map[string]string{"app": "attachment-tput-driver", "runid": *runID},
						},
						Spec: netv1a1.SubnetSpec{
							VNI:  VNI,
							IPv4: fmt.Sprintf("%s/%d", addr02, 32-suffixBits),
						},
					})
				if err != nil {
					glog.Errorf("Failed to create Subnet %s/%s, VNI=%06x, err=%s\n", theKubeNS, net2name, VNI, err.Error())
					os.Exit(27)
				}
				num_subnets++
				subnets = append(subnets, net2)
				netRefs = append(netRefs, apimachinerytypes.NamespacedName{theKubeNS, net2name})
				subnetCIDRs = append(subnetCIDRs, net2.Spec.IPv4)
			}
			glog.Infof("VirtNet created: VNI=%06x, size=%d, subnetCIDRs=%#+v\n", VNI, nsz, subnetCIDRs)
		}
		virtNets[i] = VirtNet{
			ID:        VNI,
			addr0I:    addr01I,
			addrFI:    addrF2I,
			prefixLen: 32 - suffixBits,
			subnets:   subnets,
			theCD:     cd,
			size:      nsz,
			slots:     make([]Slot, nsz),
			nodeMap:   make(map[string]*NodeData),
			addrs:     make(map[string]map[string]struct{}),
		}
		idToVirtNet[VNI] = &virtNets[i]
		for _, subnet := range subnets {
			subnetToVirtNet[subnet.Name] = &virtNets[i]
		}
		for j := 0; j < nsz; j++ {
			virtNets[i].slots[j].nextIndex = uint32(j + 1)
		}
		sizeSum += sz
		squareSum += sz * sz
		distributionCSVFile.Write([]byte(fmt.Sprintf("%d,%d\n", i+1, virtNets[i].size)))
		addr01I = addrF2I + 1
	}
	distributionCSVFile.Close()
	distributionOutlineCSVFile, err := os.Create(distributionOutlineFilename)
	if err != nil {
		panic(err)
	}
	for i := 0; i < *num_nets; i++ {
		if i == 0 || virtNets[i].size != virtNets[i-1].size ||
			i+1 == *num_nets || virtNets[i].size != virtNets[i+1].size {
			distributionOutlineCSVFile.Write([]byte(fmt.Sprintf("%d,%d\n", i+1, virtNets[i].size)))
		}
	}
	distributionOutlineCSVFile.Close()
	nneI := int(sizeSum)
	avgPeers := squareSum / sizeSum
	glog.Warningf("VirtNet size distribution: numSlots=%d, avgPeers=%g, breakPoint=%d\n", nneI, avgPeers, breakPoint)

	if *just_count {
		return
	}

	if *waitAfterCreate != 0 {
		if *num_attachments > nneI {
			glog.Errorf("Requested too many attachments, can not create them all without deleting: numSlots=%d, numAttachments=%d\n", nneI, *num_attachments)
			os.Exit(27)
		}
	}

	createLatencyHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_create_latency_seconds",
			Help:        "Latency from start to return from call to create NetworkAttachment",
			Buckets:     []float64{-1, 0, 0.0625, 0.125, 0.25, 0.5, 1, 1.5, 2, 3, 4, 8, 16, 32},
			ConstLabels: map[string]string{"runID": *runID},
		})
	createToAddressedHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_create_to_addressed_latency_seconds",
			Help:        "Latency from start of create call to notification of address",
			Buckets:     []float64{-1, 0, 0.125, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256},
			ConstLabels: map[string]string{"runID": *runID},
		})
	createToReadyHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_create_to_ready_latency_seconds",
			Help:        "Latency from start of create call to notification of Ready",
			Buckets:     []float64{-1, 0, 0.125, 0.25, 0.5, 1, 1.5, 2, 3, 4, 6, 8, 16, 24, 32, 48, 64, 96, 128, 192, 256, 512},
			ConstLabels: map[string]string{"runID": *runID},
		})
	createToBrokenHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_create_to_broken_latency_seconds",
			Help:        "Latency from start of create call to notification of broken",
			Buckets:     []float64{-1, 0, 0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
			ConstLabels: map[string]string{"runID": *runID},
		})
	createToTestedHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_create_to_tested_latency_seconds",
			Help:        "Latency from start of create call to completion of test",
			Buckets:     []float64{-1, 0, 0.125, 0.25, 0.5, 1, 1.5, 2, 3, 4, 6, 8, 16, 24, 32, 48, 64, 96, 128, 192, 256, 512},
			ConstLabels: map[string]string{"runID": *runID},
		})
	readyToTestedHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_ready_to_tested_latency_seconds",
			Help:        "Latency from readiness to completion of test",
			Buckets:     []float64{-1, 0, 0.125, 0.25, 0.5, 1, 1.5, 2, 3, 4, 6, 8, 16, 24, 32, 48, 64, 128, 256},
			ConstLabels: map[string]string{"runID": *runID},
		})
	deleteLatencyHistogram = NewTrackingHistogram(
		prometheus.HistogramOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "attachment_delete_latency_seconds",
			Help:        "Latency from start to return from call to delete NetworkAttachment",
			Buckets:     []float64{-1, 0, 0.125, 0.25, 0.5, 1, 1.5, 2, 3, 4, 8, 16, 32},
			ConstLabels: map[string]string{"runID": *runID},
		})
	testESs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "test_count",
			Help:        "Count of tests, by result code",
			ConstLabels: map[string]string{"runID": *runID},
		},
		[]string{rcLabel, fullLabel})
	successfulCreates = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "successful_creates",
			Help:        "Number of successful attempts to create a NetworkAttachment",
			ConstLabels: map[string]string{"runID": *runID},
		})
	failedCreates = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "failed_creates",
			Help:        "Number of failed attempts to create a NetworkAttachment",
			ConstLabels: map[string]string{"runID": *runID},
		})
	successfulDeletes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "successful_deletes",
			Help:        "Number of successful attempts to delete a NetworkAttachment",
			ConstLabels: map[string]string{"runID": *runID},
		})
	failedDeletes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "failed_deletes",
			Help:        "Number of failed attempts to delete a NetworkAttachment",
			ConstLabels: map[string]string{"runID": *runID},
		})

	prometheus.MustRegister(createLatencyHistogram, deleteLatencyHistogram,
		createToAddressedHistogram, createToReadyHistogram,
		createToBrokenHistogram,
		createToTestedHistogram, readyToTestedHistogram, testESs,
		successfulCreates, failedCreates, successfulDeletes, failedDeletes)

	if *threads > uint64(nneI) {
		glog.Warningln("Reduced number of threads to match number of attachment slots")
		*threads = uint64(nneI)
	}
	vnAttachments = make([]VirtNetAttachment, nneI)
	k := 0
	for i := 0; i < *num_nets; i++ {
		for j := 0; j < virtNets[i].size; j++ {
			vnAttachments[k] = VirtNetAttachment{i, j}
			k++
		}
	}
	vnAttachments = shuffle(vnAttachments)
	vnAttachments = shuffle(vnAttachments)

	nodeList, err := urClientset.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: *nodeLabelSelector})
	if err != nil {
		glog.Errorf("Failed to get list of nodes: %s\n", err.Error())
		os.Exit(30)
	}
	nodeNames := make([]string, len(nodeList.Items))
	nodeMap := make(map[string]bool)
	for idx, node := range nodeList.Items {
		nodeNames[idx] = node.Name
		nodeMap[node.Name] = true
	}
	sort.Strings(nodeNames)
	glog.Warningf("Got node list: nodeLabelSelector=%q, nodes=%#v, numNodes=%d\n", *nodeLabelSelector, nodeNames, len(nodeList.Items))
	if *onlyNode != "" && !nodeMap[*onlyNode] {
		glog.Errorf("Only-node is not a node: onlyNode=%q\n", *onlyNode)
		os.Exit(31)
	}
	stopCh := setupSignalHandler()

	genctlInformerFactory := kosinformers.NewSharedInformerFactory(kClientset, 0)
	informerGen := genctlInformerFactory.Network().V1alpha1()
	networkInformer := informerGen.Subnets().Informer()
	networkInformer.AddEventHandler(subnetEventHandler{})
	attachmentInformer := informerGen.NetworkAttachments().Informer()
	attachmentInformer.AddEventHandler(&attachmentEventHandler{})
	go func() { networkInformer.Run(stopCh) }()
	go func() { attachmentInformer.Run(stopCh) }()
	glog.Warningf("Namespace and Subnets created, waiting for notifications; num_subnets=%d\n", num_subnets)
	waitForInitializedSubnets()
	glog.Warningln("Notified of all subnets")
	nattDigits := 1 + int(math.Floor(math.Log10(float64(*num_attachments))))
	nattNameFmt := fmt.Sprintf("%%06x-%%0%dd", nattDigits)

	// Serve Prometheus metrics
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		glog.Errorf("In-process HTTP server crashed: %#+v\n", http.ListenAndServe(MetricsAddr, nil))
	}()

	var wg sync.WaitGroup
	opPeriod := 1 / *targetRate
	effN := nneI
	if effN > *num_attachments {
		effN = *num_attachments
	}
	if *waitAfterCreate == 0 {
		minLifetime := float64((uint64(effN) / *threads)*(*threads)) * opPeriod
		if minLifetime < 3 {
			glog.Warningf("Minimum nominal lifetime = %g sec is short, attachments may not mature before being deleted\n", minLifetime)
		} else {
			glog.Warningf("Minimum nominal lifetime = %g sec\n", minLifetime)
		}
	}
	t0 := time.Now()
	for i := uint64(0); i < *threads; i++ {
		wg.Add(1)
		go func(thd uint64) {
			defer wg.Done()
			numHere := (uint64(*num_attachments) + *threads - 1 - thd) / (*threads)
			work := selectWork(int(thd), int(*threads), vnAttachments)
			RunThread(kClientset, stopCh, work, nodeList.Items, nattNameFmt, *runID, t0, numHere, thd+1, *threads, opPeriod, *roundRobin, *waitAfterCreate != 0)
		}(i)
	}
	wg.Wait()
	if *waitAfterCreate != 0 {
		glog.Warningln("All attachments have been created; now waiting for count of remaining to drop to zero")
		wt := time.NewTimer(*waitAfterCreate)
		select {
		case <-wt.C:
		case <-stopCh:
		}
		for _, virtNet := range virtNets {
			for idx := range virtNet.slots {
				virtNet.slots[idx].close(virtNet.ID, theKubeNS)
			}
		}
	} else {
		time.Sleep(10 * time.Second) // wait for straggler notifications
	}
	saveProMetrics(promapi.Config{"http://localhost" + MetricsAddr, nil}, outputDir)
	createLatencyHistogram.DumpToLog()
	createToAddressedHistogram.DumpToLog()
	createToReadyHistogram.DumpToLog()
	createToBrokenHistogram.DumpToLog()
	createToTestedHistogram.DumpToLog()
	readyToTestedHistogram.DumpToLog()
	deleteLatencyHistogram.DumpToLog()
	glog.Warningf("Address checks: multiOwnerWarnings=%d\n", multiOwnerWarnings)
	fmt.Println()
	fmt.Print("Don't forget to `kubectl delete Namespace -l app=attachment-tput-driver`!\n")
	return
}

func selectWork(thd, num_threads int, frum []VirtNetAttachment) []VirtNetAttachment {
	// thd+i*num_threads < len(frum)
	// i < (len(frum)-thd)/num_threads
	ans := make([]VirtNetAttachment, (len(frum)+num_threads-1-thd)/num_threads)
	for i := 0; i < len(ans); i++ {
		ans[i] = frum[thd+i*num_threads]
	}
	return ans
}

func shuffle(x []VirtNetAttachment) []VirtNetAttachment {
	if x == nil || len(x) < 2 {
		return x
	}
	ans := make([]VirtNetAttachment, len(x))
	rem := len(x)
	for i := 0; i+1 < len(x); i++ {
		j := rand.Intn(rem)
		ans[i] = x[j]
		rem--
		if j != rem {
			x[j] = x[rem]
		}
	}
	ans[len(x)-1] = x[0]
	return ans
}

func RunThread(kClientset *kosclientset.Clientset, stopCh <-chan struct{}, work []VirtNetAttachment, nodes []k8sv1.Node, nattNameFmt, runID string, tbase time.Time, n, thd, stride uint64, opPeriod float64, roundRobin, justCreate bool) {
	glog.Warningf("Thread start: thd=%d, numAttachments=%d, stride=%d\n", thd, n, stride)
	var iCreate, iDelete uint64
	var workLen = uint64(len(work))
	attachmentsGetter := kClientset.NetworkV1alpha1()
	attachmentsDirect := attachmentsGetter.NetworkAttachments(theKubeNS)
	createAllowed := true
	for iDelete < n {
		for {
			xd := atomic.AddInt64(&xtraDelayI, 0)
			dt := float64((iCreate+iDelete)*stride+thd) * opPeriod * float64(time.Second)
			targt := tbase.Add(time.Duration(int64(dt) + xd))
			now := time.Now()
			if !targt.After(now) {
				break
			}
			gap := targt.Sub(now)
			time.Sleep(gap)
		}
		if atomic.LoadUint32(&stoppers) > 0 {
			glog.Warningf("Thread stopping early: thd=%d\n", thd)
			break
		}
		if createAllowed {
			select {
			case <-stopCh:
				createAllowed = false
			default:
			}
		}
		if createAllowed && iCreate < iDelete+workLen && iCreate < n {
			virtNet := &virtNets[work[iCreate%workLen].vnIndex]
			slotIndex := work[iCreate%workLen].slotIndex
			slotIndexS := fmt.Sprintf("%d", slotIndex)
			slot := &virtNet.slots[slotIndex]
			subnet := virtNet.subnets[0]
			if len(virtNet.subnets) > 1 {
				indexInVirtNet := atomic.AddUint32(&virtNet.createdCount, 1)
				subnet = virtNet.subnets[indexInVirtNet%uint32(len(virtNet.subnets))]
			}
			objname := fmt.Sprintf(nattNameFmt, virtNet.ID, slot.nextIndex)
			var nodeName string
			if *onlyNode != "" {
				nodeName = *onlyNode
			} else if roundRobin {
				node := &nodes[(thd+stride*iCreate)%uint64(len(nodes))]
				nodeName = node.Name
			} else {
				node := &nodes[rand.Intn(len(nodes))]
				nodeName = node.Name
			}
			notes := map[string]string{slotKey: slotIndexS}
			var postCreateExec, postDeleteExec []string
			var cd *ConnectivityDomain
			cd = virtNet.theCD
			fullTest := false
			if !*omitTest {
				peer, delay, totalDelay := cd.GetReadyAttachment(virtNet)
				nnsi := atomic.AddUint32(&nnsCount, 1)
				if fullTest = peer != nil; fullTest {
					postCreateExec = strings.Split(fmt.Sprintf("/usr/local/kos/bin/TestByPing ${ifname} %s-%d ${ipv4}/%d %s %d %s", runID, nnsi, virtNet.prefixLen, peer.Status.IPv4, *pingCount, peer.Name), " ")
				} else {
					postCreateExec = strings.Split(fmt.Sprintf("/usr/local/kos/bin/TestByPing ${ifname} %s-%d ${ipv4}/%d", runID, nnsi, virtNet.prefixLen), " ")
				}
				postDeleteExec = strings.Split(fmt.Sprintf("/usr/local/kos/bin/RemoveNetNS %s-%d", runID, nnsi), " ")
				if delay > 0 {
					peerName := ""
					if peer != nil {
						peerName = peer.Name
					}
					glog.Warningf("Waited for ready peer: VNI=%06x, waiter=%s, peerName=%s, wait=%s, totalDelay=%s\n", cd.VNI, objname, peerName, delay, totalDelay)
				}
			}
			slot.setCurrentName(slotIndex, objname, nodeName, cd)
			ti0 := time.Now()
			ti0S := ti0.Format(kosutil.TimestampLayout)
			notes[kosutil.CreateTimestampAnnotationKey] = ti0S
			obj := netv1a1.NetworkAttachment{
				ObjectMeta: metav1.ObjectMeta{
					Name:        objname,
					Namespace:   theKubeNS,
					Labels:      map[string]string{"app": "attachment-tput-driver", "runid": runID},
					Annotations: notes,
				},
				Spec: netv1a1.NetworkAttachmentSpec{
					Node:           nodeName,
					Subnet:         subnet.Name,
					PostCreateExec: postCreateExec,
					PostDeleteExec: postDeleteExec,
				},
			}
			retObj, err := attachmentsDirect.Create(&obj)
			tif := time.Now()
			opLatency := tif.Sub(ti0).Seconds()
			createLatencyHistogram.ObserveAt(opLatency, theKubeNS, objname)
			if err != nil {
				slot.setCurrentName(slotIndex, "", "", cd)
				failedCreates.Inc()
				glog.Infof("Failed to create NetworkAttachment: attachment=%s/%s, VNI=%06x, subnet=%s, node=%s, err=%s\n", theKubeNS, objname, virtNet.ID, subnet.Name, nodeName, err.Error())
			} else {
				slot.setNetAtt(virtNet, slotIndex, ti0, tif, retObj, fullTest)
				successfulCreates.Inc()
				glog.Infof("Created NetworkAttachment: attachment=%s/%s, VNI=%06x, subnet=%s, node=%s, RV=%s, preCreateTime=%s, postCreateTime=%s\n", theKubeNS, objname, virtNet.ID, subnet.Name, nodeName, retObj.ResourceVersion, ti0.Format(kosutil.RFC3339Milli), tif.Format(kosutil.RFC3339Milli))
			}
			iCreate++
		} else if justCreate {
			break
		} else {
			virtNet := &virtNets[work[iDelete%workLen].vnIndex]
			slot := &virtNet.slots[work[iDelete%workLen].slotIndex]
			natt := slot.close(virtNet.ID, theKubeNS)
			if natt != nil {
				ti0 := time.Now()
				delopts := metav1.DeleteOptions{}
				err := attachmentsDirect.Delete(natt.Name, &delopts)
				tif := time.Now()
				opLatency := tif.Sub(ti0).Seconds()
				deleteLatencyHistogram.ObserveAt(opLatency, theKubeNS, natt.Name)
				if err != nil {
					failedDeletes.Inc()
					glog.Infof("Failed to delete NetworkAttachment: attachment=%s/%s, VNI=%06x, subnet=%s, node=%s, ipv4=%s, err=%s\n", theKubeNS, natt.Name, virtNet.ID, natt.Spec.Subnet, natt.Spec.Node, natt.Status.IPv4, err.Error())
				} else {
					successfulDeletes.Inc()
					glog.Infof("Deleted NetworkAttachment: attachment=%s/%s, VNI=%06x, subnet=%s, node=%s, preDeleteTime=%s, ipv4=%s\n", theKubeNS, natt.Name, virtNet.ID, natt.Spec.Subnet, natt.Spec.Node, ti0.Format(kosutil.RFC3339Milli), natt.Status.IPv4)
				}
			}
			slot.nextIndex += uint32(virtNet.size)
			iDelete++
		}
	}
}

func setupSignalHandler() (stopCh <-chan struct{}) {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func saveProMetrics(config promapi.Config, outputDir string) {
	client, err := promapi.NewClient(config)
	if err != nil {
		glog.Errorf("Failed to create Prometheus client: address=%q: %s\n", config.Address, err.Error())
		return
	}
	url := client.URL("/metrics", map[string]string{})
	request, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		glog.Errorf("Failed to form Prometheus request: url=%q: %s\n", url, err.Error())
		return
	}
	var ctx context.Context = context.Background()
	_, body, err := client.Do(ctx, request)
	if err != nil {
		glog.Errorf("Failed to fetch Prometheus metrics from self: url=%q: %s\n", url, err.Error())
		return
	}
	metricsFilename := filepath.Join(outputDir, "driver.metrics")
	metricsFile, err := os.Create(metricsFilename)
	if err != nil {
		glog.Errorf("Failed to create metrics file: filename=%q: %s\n", metricsFilename, err.Error())
		return
	}
	_, err = metricsFile.Write(body)
	if err != nil {
		glog.Errorf("Failed to write metrics file: filename=%q: %s\n", metricsFilename, err.Error())
		return
	}
	err = metricsFile.Close()
	if err != nil {
		glog.Errorf("Failed to close metrics file: filename=%q: %s\n", metricsFilename, err.Error())
		return
	}
	glog.Warningf("Wrote metrics file: filename=%q\n", metricsFilename)
}
