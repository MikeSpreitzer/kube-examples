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

// Package ovs defines a network fabric that implements k8s.io/examples/staging/kos/pkg/networkfabric.InterfaceManager .
//
// The fabric is thread-safe.
//
// The fabric is based on an Open vSwitch (OvS) bridge, which gets configured
// through OpenFlow.
//
// The fabric constructor's creates the OvS bridge and a Linux network Interface
// configured to act as a VXLAN Tunnel End Point (VTEP), and connects them
// together. It also installs some default OpenFlow flows in the bridge.
// All these operations are idempotent so that it is safe for the process that
// uses the fabric to restart after a failure and re-instantiate the fabric.
// This is important because the bridge and the VTEP outlive the process that
// created them.
//
// If multiple processes using an OvS fabric ran at the same time on the same
// node, each fabric would configure the same OvS bridge without realizing
// another fabric is doing the same. This can lead to severe errors and
// misconfigurations. It is up to users of the fabric to ensure that they are
// the only process on their node that is using an OvS fabric.
//
// A local Network Interface is implemented as a Linux network interface connected
// to the bridge and three OpenFlow flows that allow the network interface to send
// and receive traffic. One flow encapsulates all traffic coming from the Linux
// network interface in a VXLAN packet, one flow forwards ARP requests for the
// Network Interface IP and VNI to the Linux network interface, one flow forwards
// normal Layer 2 frames for the Network Interface MAC address and VNI to the
// Linux network interface.
// The flows are added/removed atomically to/from the bridge, but the creation
// of the Linux network interface and the addition of the flows are not executed
// atomically. If an error occurs after the creation of the Linux network
// interface but before (or during) the addition of the flows, a one-shot attempt
// to delete the Linux network interface is done; if it fails the fabric gives up
// and an incomplete implementation of the local Network Interface is left on the node.
// Another case where an incomplete implementation of a local Network Interface
// is left on the node is when the process that uses this fabric crashes between
// creation of the Linux network interface and addition of the flows.
// `ListLocalIfcs()`, the method that returns all the existing local network
// Interfaces in the bridge, queries the bridge OpenFlow flows and Linux network
// interfaces and tries to pair them, each pairing is then parsed into the
// appropriate `k8s.io/examples/staging/kos/pkg/networkfabric.LocalNetIfc`.
// `ListLocalIfcs()` might find incomplete implementations, that is, Linux
// network interfaces connected to the bridge for whom OpenFlow flows are not in
// the bridge; the reasons that can lead to such incomplete implementations were
// given earlier in this paragraph. In such cases, an attempt to clean up the
// incomplete implementation is made by deleting the Linux network interface; only
// one try is made, if it fails, the incomplete implementation is left on the
// node and no error is returned.
//
//
// A remote Network Interface is implemented as two OpenFlow flows. One flow
// sends ARP requests for the remote Network Interface IP and VNI to the
// remote Network Interface host through the VTEP of the fabric's bridge. The
// other flow does the same thing, except for Layer 2 frames for the Network
// Interface MAC address and VNI instead of ARP requests. The two flows are
// added/removed atomically to/from the fabric's bridge.
// `ListRemoteIfcs()`, the method that returns all the existing remote Network
// Interfaces in the bridge, simply queries the bridge for all the OpenFlow
// flows and parses the flows that were added for a remote Network Interface
// into the appropriate `k8s.io/examples/staging/kos/pkg/networkfabric.RemoteNetIfc`.
//
//
// Currently, all operations on the bridge are done using the OvS CLI (through
// Golang's os.Exec).
//
// To be able to create an ovs network fabric in an application, you need to
// import package ovs in the main package of the application. This ensures that
// the factory which creates ovs network fabrics is registered in the network
// fabric factory registry, and can therefore be used to instantiate network
// fabrics.
package ovs // import "k8s.io/examples/staging/kos/pkg/networkfabric/ovs"
