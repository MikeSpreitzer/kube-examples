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

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkAttachmentList is a list of NetworkAttachment objects.
type NetworkAttachmentList struct {
	metav1.TypeMeta

	// +optional
	metav1.ListMeta

	Items []NetworkAttachment
}

type NetworkAttachmentSpec struct {
	// Node is the name of the node where the attachment should appear
	Node string

	// Subnet is the object name of the subnet of this attachment
	Subnet string

	// PostCreateExec is a command to exec inside the attachment
	// host's connection agent container after the Linux network
	// interface is created.  Precisely: if a local NetworkAttachment
	// is in the network fabric, has a non-empty PostCreateExec, and
	// that command has not yet been launched then the command is
	// launched and, upon completion, the results reported through the
	// NetworkAttachmentStatus PostCreateExecReport field.  The
	// connection agent is configured with a set of allowed programs
	// to invoke.  If a non-allowed program is requested then the
	// result will report an error.  Each argument is subjected to a
	// very restricted form of variable expansion.  The only allowed
	// syntax is `${variableName}` and the only variables are
	// `ifname`, `ipv4`, and `mac`.
	// +optional
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
	PostDeleteExec []string
}

type NetworkAttachmentStatus struct {
	// +optional
	Errors NetworkAttachmentErrors

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
	// PostCreateExec.
	// +optional
	PostCreateExecReport *ExecReport
}

type NetworkAttachmentErrors struct {
	// IPAM holds errors about the IP Address Management for this attachment.
	// +optional
	IPAM []string

	// Host holds errors from the node where this attachment is placed.
	// +optional
	Host []string
}

// ExecReport reports on what happened when a command was execd.
type ExecReport struct {
	// ExitStatus is the Linux exit status from the command, or a
	// negative number to signal a prior problem (detailed in StdErr).
	ExitStatus int32

	StartTime metav1.Time

	StopTime metav1.Time

	StdOut string
	StdErr string
}

// Equal tests whether the two referenced ExecReports say the same thing
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

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type NetworkAttachment struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

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
	IPv4 string

	// VNI identifies the virtual network.
	// Valid values are in the range [1,2097151].
	VNI uint32
}

type SubnetStatus struct {
	// Errors are the complaints, if any, from the IPAM controller.
	// +optional
	Errors []string
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Subnet struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

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

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type IPLock struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

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
