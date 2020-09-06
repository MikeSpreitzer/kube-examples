/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package network

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExtendedObjectMeta has extra metadata for an API object.
// This is maintained by the server, clients can not modify it.
type ExtendedObjectMeta struct {
	// Writes identifies the latest write to each part of the object.
	// +listType=map
	// +listMapKey=section
	// +optional
	Writes WriteSet

	// LastClientWrite identifies the latest client write among those needed to
	// implement the API object. Unlike `Writes`, it might describe a write on
	// an API object other than the owning API object.
	// +optional
	LastClientWrite ClientWrite

	// LastControllerStart identifies the relevant controller that started last.
	// Relevant controllers are those directly or indirectly responsible for
	// implementing the API object.
	// +optional
	LastControllerStart ControllerStart
}

// WriteSet represents a map from section to time
type WriteSet []ObjectWrite

// ObjectWrite describes a write to part of an object
type ObjectWrite struct {
	// Section identifies the part of the object that was written.
	// Each type of object is broken down into a type-specific set of
	// sections.
	Section string

	// ServerTime is the time when the write was recorded at the apiserver
	ServerTime metav1.MicroTime
}

// GetServerWriteTime returns the server time of the write to the
// given section, or zero if there was none.
func (writes WriteSet) GetServerWriteTime(section string) metav1.MicroTime {
	wr, _ := writes.GetWrite(section)
	return wr.ServerTime
}

// GetServerWriteTimeUnwrapped is like GetServerWriteTime but returns the inner time.Time
func (writes WriteSet) GetServerWriteTimeUnwrapped(section string) time.Time {
	return writes.GetServerWriteTime(section).Time
}

// GetWrite returns the write to the given section, and a bool
// indicating whether there is one.
func (writes WriteSet) GetWrite(section string) (ObjectWrite, bool) {
	for _, wr := range writes {
		if wr.Section == section {
			return wr, true
		}
	}
	return ObjectWrite{}, false
}

// SetWrite produces a revised slice that includes the given write.
// The input is not side-effected.
func (writes WriteSet) SetWrite(section string, serverTime metav1.MicroTime) WriteSet {
	n := len(writes)
	var i int
	for i = 0; i < n && writes[i].Section != section; i++ {
	}
	if i == n {
		return append(WriteSet{{Section: section, ServerTime: serverTime}}, writes...)
	}
	return append(append(WriteSet{{Section: section, ServerTime: serverTime}}, writes[:i]...), writes[i+1:]...)
}

// Diff produces the subset of the given writes that do not overlap
// with the other writes
func (writes WriteSet) Diff(others WriteSet) WriteSet {
	ans := make(WriteSet, 0, len(writes))
	for _, wr := range writes {
		_, found := others.GetWrite(wr.Section)
		if !found {
			ans = append(ans, wr)
		}
	}
	return ans
}

// UnionLeft produces the union of the receiver and the other writes
// that do not overlap with the receiver
func (writes WriteSet) UnionLeft(others WriteSet) WriteSet {
	ans := append(WriteSet{}, writes...)
	for _, owr := range others {
		_, found := ans.GetWrite(owr.Section)
		if !found {
			ans = append(ans, owr)
		}
	}
	return ans
}

// UnionMin produces the union of the two write sets, keeping the
// earlier time for sections written in both sets
func (writes WriteSet) UnionMin(others WriteSet) WriteSet {
	ans := others.Diff(writes)
	for _, wr := range writes {
		owr, found := others.GetWrite(wr.Section)
		if found {
			owr.ServerTime = TimeMin(owr.ServerTime, wr.ServerTime)
			ans = append(ans, owr)
		} else {
			ans = append(ans, wr)
		}
	}
	return ans
}

// UnionMax produces the union of the two write sets, keeping the
// later time for sections written in both sets
func (writes WriteSet) UnionMax(others WriteSet) WriteSet {
	ans := others.Diff(writes)
	for _, wr := range writes {
		owr, found := others.GetWrite(wr.Section)
		if found {
			owr.ServerTime = TimeMax(owr.ServerTime, wr.ServerTime)
			ans = append(ans, owr)
		} else {
			ans = append(ans, wr)
		}
	}
	return ans
}

// TimeMin returns the earlier of the two given times
func TimeMin(a, b metav1.MicroTime) metav1.MicroTime {
	if (&b).Before(&a) {
		return b
	}
	return a
}

// TimeMax returns the later of the two given times
func TimeMax(a, b metav1.MicroTime) metav1.MicroTime {
	if b.After(a.Time) {
		return b
	}
	return a
}

// Now returns time.Now() in the form used here
func Now() metav1.MicroTime {
	return metav1.NowMicro()
}

// ClientWrite models a write by a client. A "client" is any entity that is not
// part of the KOS control plane.
type ClientWrite struct {
	// Name identifies the client write.
	Name string

	// The time at which the client write happened.
	Time metav1.MicroTime
}

// ControllerStart carries information on the start of a KOS controller.
type ControllerStart struct {
	// Controller is the name of the controller which started.
	Controller string

	// ControllerTime is the time at which the controller started, as recorded
	// by the controller itself.
	ControllerTime metav1.MicroTime
}

const (
	SubnetClientWrite = "subnet"
	NAClientWrite     = "na"

	SubnetValidator      = "subnet_validator"
	IPAMController       = "ipam_controller"
	LocalConnectionAgent = "local_connection_agent"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkAttachmentList is a list of NetworkAttachment objects.
type NetworkAttachmentList struct {
	metav1.TypeMeta

	// +optional
	metav1.ListMeta

	Items []NetworkAttachment
}

type NetworkAttachmentSpec struct {
	// Node is the name of the node where the attachment should appear.
	// It is immutable: attempts to update it will fail.
	Node string

	// Subnet is the object name of the subnet of this attachment.
	// It is immutable: attempts to update it will fail.
	Subnet string

	// PostCreateExec is a command to exec inside the attachment
	// host's connection agent container after a new Linux network
	// interface for the attachment is created with a network fabric
	// operation. By definition, it is not guaranteed to execute;
	// if the Linux network interface for the attachment previously
	// belonged to another attachment and was recycled as opposed to
	// being created, PostCreateExec will not execute. After PostCreateExec
	// has executed the results of the execution are reported through the
	// NetworkAttachmentStatus PostCreateExecReport field.
	// The connection agent is configured with a set of allowed programs
	// to invoke. If a non-allowed program is requested then the result
	// will report an error.  Each argument is subjected to a very
	// restricted form of variable expansion.  The only allowed syntax
	// is `${variableName}` and the only variables are `ifname`, `ipv4`,
	// and `mac`.
	// PostCreateExec is immutable: attempts to update it will fail.
	// +optional
	// +patchStrategy=replace
	PostCreateExec []string

	// PostDeleteExec is a command to exec inside the attachment
	// host's connection agent container after the attachment's Linux
	// network interface is deleted.  Precisely: if a local
	// NetworkAttachment is not in the network fabric, has a
	// PostCreateExec that has been started, has a non-empty
	// PostDeleteExec, and the PostCreateExec has not yet been
	// launched then that command will be launched.  The result is not
	// reported in the status of the NetworkAttachment (it may be
	// deleted by then).  The same restrictions and variable
	// expansions as for PostCreateExec are applied.
	// +optional
	// +patchStrategy=replace
	PostDeleteExec []string
}

type NetworkAttachmentStatus struct {
	// +optional
	Errors NetworkAttachmentErrors

	// AddressContention indicates whether the address assignment was
	// delayed due to not enough addresses being available at first.
	AddressContention bool

	// LockUID is the UID of the IPLock object holding this attachment's
	// IP address, or the empty string when there is no address.
	// This field is a private detail of the implementation, not really
	// part of the public API.
	// +optional
	LockUID string

	// AddressVNI is the VNI associated with this attachment's
	// IP address assignment, or the empty string when there is no address.
	// +optional
	AddressVNI uint32

	// IPv4 is non-empty when an address has been assigned.
	// +optional
	IPv4 string

	// MACAddress is non-empty while there is a corresponding Linux
	// network interface on the host.
	// +optional
	MACAddress string

	// IfcName is the name of the network interface that implements this
	// attachment on its node, or the empty string to indicate no
	// implementation.
	// +optional
	IfcName string
	// HostIP is the IP address of the node the attachment is bound to.
	// +optional
	HostIP string

	// PostCreateExecReport, if non-nil, reports on the run of the
	// PostCreateExec that was launched when the Linux network
	// interface owned by the attachment was created. Notice that
	// such PostCreateExec might differ from the one in the
	// NetworkAttachmentSpec PostCreateExec field of the attachment;
	// precisely, if the Linux network interface for the attachment
	// was recycled as opposed to being created with a network fabric
	// operation, PostCreateExecReport reports on the run of the
	// PostCreateExec of the attachment for whom the Linux network
	// interface was first created.
	// +optional
	PostCreateExecReport *ExecReport
}

type NetworkAttachmentErrors struct {
	// IPAM holds errors about the IP Address Management for this attachment.
	// +optional
	// +patchStrategy=replace
	IPAM []string

	// Host holds errors from the node where this attachment is placed.
	// +optional
	// +patchStrategy=replace
	Host []string
}

// ExecReport reports on what happened when a command was execd.
type ExecReport struct {
	// Command is the command whose execution is summarized by this ExecReport.
	// +patchStrategy=replace
	Command []string

	// ExitStatus is the Linux exit status from the command, or a
	// negative number to signal a prior problem (detailed in StdErr).
	ExitStatus int32

	StartTime metav1.Time

	StopTime metav1.Time

	StdOut string
	StdErr string
}

// Equiv tests whether the two referenced ExecReports say the same
// thing within the available time precision.  The apiservers only
// store time values with seconds precision.
func (x *ExecReport) Equiv(y *ExecReport) bool {
	if x == y {
		return true
	}
	if x == nil || y == nil {
		return false
	}
	return x.ExitStatus == y.ExitStatus &&
		x.StdOut == y.StdOut &&
		x.StdErr == y.StdErr &&
		x.StartTime.Time.Truncate(time.Second).Equal(y.StartTime.Time.Truncate(time.Second)) &&
		x.StopTime.Time.Truncate(time.Second).Equal(y.StopTime.Time.Truncate(time.Second))
}

// The ExtendedObjectMeta sections for a NetworkAttachment
const (
	NASectionSpec       = "spec"
	NASectionAddr       = "status.address"
	NASectionImpl       = "status.impl"
	NASectionExecReport = "status.execReport"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkAttachment is about a Linux network interface connected to a
// Subnet.  The sections recorded in ExtendedObjectMeta are: spec,
// status.address, status.impl, status.execReport.
type NetworkAttachment struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

	// `extendedMetadata` adds non-standard object metadata
	// +optional
	ExtendedObjectMeta

	Spec NetworkAttachmentSpec

	// +optional
	Status NetworkAttachmentStatus
}

// SubnetSpec is the desired state of a subnet.
// For a given VNI, all the subnets having that VNI:
// - have disjoint IP ranges, and
// - are in the same Kubernetes API namespace.
type SubnetSpec struct {
	// IPv4 is the CIDR notation for the v4 address range of this subnet.
	// It is immutable: attempts to update it will fail.
	IPv4 string

	// VNI identifies the virtual network.
	// Valid values are in the range [1,2097151].
	// It is immutable: attempts to update it will fail.
	VNI uint32
}

type SubnetStatus struct {
	// Validated tells users and consumers whether the subnet spec has passed
	// validation or not. The fields that undergo validation are spec.ipv4 and
	// spec.vni. If Validated is true it is guaranteed to stay true for the
	// whole lifetime of the subnet. If Validated is false or unset, there are
	// three possible reasons:
	// 	(1) Validation has not been performed yet.
	// 	(2) The subnet CIDR overlaps with the CIDR of another subnet with the
	//		same VNI.
	//	(3) The subnet Namespace is different than that of another subnet with
	// 		the same VNI.
	// If for a subnet X Validated is false because of other conflicting
	// subnets, deletion of the conflicting subnets will cause a transition to
	// true.
	// +optional
	Validated bool

	// Errors holds the complaints, if any, from the subnet validator. It is
	// non-empty if and only if Validated is false for reasons (2) or (3).
	// +optional
	// +patchStrategy=replace
	Errors []string
}

// The ExtendedObjectMeta sections for a Subnet
const (
	SubnetSectionSpec   = "spec"
	SubnetSectionStatus = "status"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Subnet is about an IP subnet on a virtual network.  For
// ExtendedObjectMeta the sections are: spec, status.
type Subnet struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

	// `extendedMetadata` adds non-standard object metadata
	// +optional
	ExtendedObjectMeta

	Spec SubnetSpec

	// +optional
	Status SubnetStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SubnetList is a list of Subnet objects.
type SubnetList struct {
	metav1.TypeMeta

	// +optional
	metav1.ListMeta

	Items []Subnet
}

type IPLockSpec struct {
	SubnetName string
}

// The ExtendedObjectMeta sections for an IPLock.
const (
	IPLockSectionSpec = "spec"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type IPLock struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

	// `extendedMetadata` adds non-standard object metadata.
	// +optional
	ExtendedObjectMeta

	Spec IPLockSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IPLockList is a list of IPLock objects.
type IPLockList struct {
	metav1.TypeMeta

	// +optional
	metav1.ListMeta

	Items []IPLock
}
