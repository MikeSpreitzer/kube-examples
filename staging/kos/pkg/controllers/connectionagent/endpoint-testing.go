/*
Copyright 2018 The Kubernetes Authors.

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

package connectionagent

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"

	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	netv1a1 "k8s.io/examples/staging/kos/pkg/apis/network/v1alpha1"
	netfabric "k8s.io/examples/staging/kos/pkg/networkfabric"
)

const (
	FailProgramForbidden  = -1
	FailErrNotExit        = -2
	FailSysUnexpectedType = -3
)

// LaunchCommand normally forks a goroutine to exec the given command.
// If the given command is empty this function does nothing and
// returns nil.  If a problem is discovered during preparation then an
// ExecReport reflecting that problem is returned and there is no
// exec.  Otherwise the fork and exec are done and, if `saveReport`,
// the attachment's local state is updated with the ExecReport and the
// attachment is requeued so that the ExecReport gets stored into the
// attachment's status iff it still should be.  If `!saveReport` then
// the ExecReport is just logged (but probably should be emitted in an
// Event).
func (c *ConnectionAgent) LaunchCommand(attNSN k8stypes.NamespacedName, localIfc *netfabric.LocalNetIfc, cmd []string, what string, saveReport bool) *netv1a1.ExecReport {
	var cr *netv1a1.ExecReport
	now := time.Now()
	if len(cmd) == 0 {
		return nil
	}
	if _, allowed := c.allowedPrograms[cmd[0]]; !allowed {
		cr = &netv1a1.ExecReport{
			ExitStatus: FailProgramForbidden,
			StartTime:  k8smetav1.Time{now},
			StopTime:   k8smetav1.Time{now},
			StdErr:     fmt.Sprintf("%s specifies non-allowed path %s", what, cmd[0]),
		}
	}
	if cr != nil {
		glog.V(4).Infof("Invalid attachment command spec: vni=%06x, att=%s, what=%s, cmd=%#v, error=%q\n", localIfc.VNI, attNSN, cmd, what, cr.StdErr)
		return cr
	}
	glog.V(4).Infof("Will launch attachment command: vni=%06x, att=%s, what=%s, cmd=%#v\n", localIfc.VNI, attNSN, what, cmd)
	go func() { c.RunCommand(attNSN, localIfc, cmd, what, saveReport) }()
	return nil
}

func (c *ConnectionAgent) RunCommand(attNSN k8stypes.NamespacedName, localIfc *netfabric.LocalNetIfc, urcmd []string, what string, saveReport bool) {
	expanded := make([]string, len(urcmd)-1)
	for i, argi := range urcmd[1:] {
		argi = strings.Replace(argi, "${ifname}", localIfc.Name, -1)
		argi = strings.Replace(argi, "${ipv4}", localIfc.GuestIP.String(), -1)
		argi = strings.Replace(argi, "${mac}", localIfc.GuestMAC.String(), -1)
		expanded[i] = argi
	}
	cmd := exec.Command(urcmd[0], expanded...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	startTime := time.Now()
	err := cmd.Run()
	stopTime := time.Now()
	cr := netv1a1.ExecReport{
		StartTime: k8smetav1.Time{startTime},
		StopTime:  k8smetav1.Time{stopTime},
		StdOut:    stdout.String(),
		StdErr:    stderr.String(),
	}
	if err == nil {
		cr.ExitStatus = 0
	} else {
		switch et := err.(type) {
		case *exec.ExitError:
			esys := et.Sys()
			switch esyst := esys.(type) {
			case syscall.WaitStatus:
				cr.ExitStatus = int32(esyst.ExitStatus())
			default:
				glog.Warningf("et.Sys has unexpected type: vni=%06x, att=%s, what=%s, type=%T, esys=%#+v\n", localIfc.VNI, attNSN, what, esys, esys)
				cr.ExitStatus = FailSysUnexpectedType
			}
		default:
			glog.Warningf("err is not a *exec.ExitError: vni=%06x, att=%s, what=%s, type=%T, err=%#+v\n", localIfc.VNI, attNSN, what, err, err)
			cr.ExitStatus = FailErrNotExit
		}
	}
	c.attachmentExecDurationHistograms.With(prometheus.Labels{"what": what}).Observe(stopTime.Sub(startTime).Seconds())
	c.attachmentExecStatusCounts.With(prometheus.Labels{"what": what, "exitStatus": strconv.FormatInt(int64(cr.ExitStatus), 10)}).Add(1)
	glog.V(4).Infof("Exec report: vni=%06x, att=%s, what=%s, report=%#+v\n", localIfc.VNI, attNSN, what, cr)
	if !saveReport {
		return
	}
	c.setExecReport(attNSN, &cr)
	c.queue.Add(attNSN)
	if true {
		return
	}
	patchObj := ExecReportPatchObj{
		netv1a1.NetworkAttachmentStatus{PostCreateExecReport: &cr}}
	patchBytes, err := json.Marshal(patchObj)
	if err != nil {
		glog.Errorf("Failed to marshal %#+v: %s\n", cr, err.Error())
		return
	}
	glog.V(5).Infof("Patch for %#+v is %q\n", cr, string(patchBytes))
	ne2, err := c.kcs.NetworkV1alpha1().NetworkAttachments(attNSN.Namespace).Patch(attNSN.Name, k8stypes.StrategicMergePatchType, patchBytes)
	if err != nil {
		glog.Warningf("patch attachment command result failed: vni=%06x, att=%s, what=%s, exitStatus=%d, err=%s\n", localIfc.VNI, attNSN, what, cr.ExitStatus, err.Error())
	} else {
		glog.V(4).Infof("patch attachment command result succeeded: vni=%06x, att=%s, what=%s, exitStatus=%d, newResourceVersion=%s\n", localIfc.VNI, attNSN, what, cr.ExitStatus, ne2.ResourceVersion)
	}
}

type ExecReportPatchObj struct {
	Status netv1a1.NetworkAttachmentStatus `json:"status"`
}
