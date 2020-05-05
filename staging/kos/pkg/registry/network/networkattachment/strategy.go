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

package networkattachment

import (
	"context"
	"fmt"
	"strconv"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"

	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/examples/staging/kos/pkg/apis/network"
)

// NewStrategies creates and returns strategy objects for the main
// resource and its status subresource
func NewStrategies(typer runtime.ObjectTyper) (networkattachmentStrategy, networkattachmentStatusStrategy) {
	s := networkattachmentStrategy{typer, names.SimpleNameGenerator}
	return s, networkattachmentStatusStrategy{s}
}

// GetAttrs returns labels.Set, fields.Set,
// and error in case the given runtime.Object is not a NetworkAttachment.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	networkattachment, ok := obj.(*network.NetworkAttachment)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a NetworkAttachment")
	}
	return labels.Set(networkattachment.ObjectMeta.Labels), SelectableFields(networkattachment), nil
}

// MatchNetworkAttachment is the filter used by the generic etcd backend to
// watch events from etcd to clients of the apiserver only interested in
// specific labels/fields.
func MatchNetworkAttachment(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *network.NetworkAttachment) fields.Set {
	return generic.AddObjectMetaFieldsSet(
		fields.Set{
			"spec.node":         obj.Spec.Node,
			"spec.subnet":       obj.Spec.Subnet,
			"status.ipv4":       obj.Status.IPv4,
			"status.hostIP":     obj.Status.HostIP,
			"status.addressVNI": strconv.FormatUint(uint64(obj.Status.AddressVNI), 10),
		},
		&obj.ObjectMeta, true)
}

type networkattachmentStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

var _ rest.RESTCreateStrategy = networkattachmentStrategy{}
var _ rest.RESTUpdateStrategy = networkattachmentStrategy{}
var _ rest.RESTDeleteStrategy = networkattachmentStrategy{}

func (networkattachmentStrategy) NamespaceScoped() bool {
	return true
}

func (networkattachmentStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	na := obj.(*network.NetworkAttachment)
	na.ExtendedObjectMeta = network.ExtendedObjectMeta{}
	na.Writes = na.Writes.SetWrite(network.NASectionSpec, network.Now())
	na.Status = network.NetworkAttachmentStatus{}
}

func (networkattachmentStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	oldNA := old.(*network.NetworkAttachment)
	newNA := obj.(*network.NetworkAttachment)
	newNA.Status = oldNA.Status
	newNA.ExtendedObjectMeta = oldNA.ExtendedObjectMeta
	// ValidateUpdate insists that the only Spec field that can change is PostDeleteExec
	if !SliceOfStringEqual(oldNA.Spec.PostDeleteExec, newNA.Spec.PostDeleteExec) {
		now := network.Now()
		newNA.Writes = newNA.Writes.SetWrite(network.NASectionSpec, now)
		newNA.Generation = oldNA.Generation + 1
	}
}

func SliceOfStringEqual(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	for i, xi := range x {
		if xi != y[i] {
			return false
		}
	}
	return true
}

func (networkattachmentStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

func (networkattachmentStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (networkattachmentStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (networkattachmentStrategy) Canonicalize(obj runtime.Object) {
}

func (networkattachmentStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	var errs field.ErrorList
	immutableFieldMsg := "attempt to update immutable field"
	newNa, oldNa := obj.(*network.NetworkAttachment), old.(*network.NetworkAttachment)
	if newNa.Spec.Node != oldNa.Spec.Node {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "node"), immutableFieldMsg))
	}
	if newNa.Spec.Subnet != oldNa.Spec.Subnet {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "subnet"), immutableFieldMsg))
	}
	if !SliceOfStringEqual(newNa.Spec.PostCreateExec, oldNa.Spec.PostCreateExec) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "postCreateExec"), immutableFieldMsg))
	}
	return errs
}

type networkattachmentStatusStrategy struct {
	networkattachmentStrategy
}

var _ rest.RESTUpdateStrategy = networkattachmentStatusStrategy{}

func (networkattachmentStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

func (networkattachmentStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNA := obj.(*network.NetworkAttachment)
	oldNA := old.(*network.NetworkAttachment)
	// update is not allowed to set spec
	newNA.Spec = oldNA.Spec
	newNA.ExtendedObjectMeta = oldNA.ExtendedObjectMeta
	now := network.Now()
	if oldNA.Status.LockUID != newNA.Status.LockUID ||
		oldNA.Status.AddressVNI != newNA.Status.AddressVNI ||
		oldNA.Status.IPv4 != newNA.Status.IPv4 ||
		!oldNA.Status.WaitedForIPAM.Equal(newNA.Status.WaitedForIPAM) ||
		!SliceOfStringEqual(oldNA.Status.Errors.IPAM, newNA.Status.Errors.IPAM) {
		newNA.Writes = newNA.Writes.SetWrite(network.NASectionAddr, now)
	}
	if oldNA.Status.MACAddress != newNA.Status.MACAddress ||
		oldNA.Status.IfcName != newNA.Status.IfcName ||
		oldNA.Status.HostIP != newNA.Status.HostIP ||
		!oldNA.Status.WaitedForCA.Equal(newNA.Status.WaitedForCA) ||
		!SliceOfStringEqual(oldNA.Status.Errors.Host, newNA.Status.Errors.Host) {
		newNA.Writes = newNA.Writes.SetWrite(network.NASectionImpl, now)
	}
	if !oldNA.Status.PostCreateExecReport.Equiv(newNA.Status.PostCreateExecReport) {
		newNA.Writes = newNA.Writes.SetWrite(network.NASectionExecReport, now)
	}
}

func (networkattachmentStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	na := obj.(*network.NetworkAttachment)
	allErrs := apimachineryvalidation.ValidateObjectMeta(&na.ObjectMeta, true, func(name string, prefix bool) []string { return nil }, field.NewPath("metadata"))
	return allErrs
}
