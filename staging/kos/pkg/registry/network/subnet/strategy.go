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

package subnet

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"

	"k8s.io/examples/staging/kos/pkg/apis/network"
	"k8s.io/examples/staging/kos/pkg/apis/network/helper"
	"k8s.io/examples/staging/kos/pkg/util/parse/network/subnet"
)

const subnetVNIIndex = "subnetVNI"

// NewStrategies creates and returns strategy objects for the main
// resource and its status subresource
func NewStrategies(typer runtime.ObjectTyper, checkConflicts bool, subnetInformer cache.SharedIndexInformer) (*subnetStrategy, subnetStatusStrategy) {
	subnetInformer.AddIndexers(map[string]cache.IndexFunc{subnetVNIIndex: SubnetVNI})
	subnetIndexer := subnetInformer.GetIndexer()
	s := &subnetStrategy{typer,
		names.SimpleNameGenerator,
		checkConflicts,
		subnetIndexer}
	return s, subnetStatusStrategy{s}
}

// GetAttrs returns labels.Set, fields.Set,
// and error in case the given runtime.Object is not a Subnet.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	subnet, ok := obj.(*network.Subnet)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Subnet")
	}
	return labels.Set(subnet.ObjectMeta.Labels), SelectableFields(subnet), nil
}

// MatchSubnet is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific
// labels/fields.
func MatchSubnet(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
func SelectableFields(obj *network.Subnet) fields.Set {
	return generic.AddObjectMetaFieldsSet(
		fields.Set{
			"spec.vni": strconv.FormatUint(uint64(obj.Spec.VNI), 10),
		},
		&obj.ObjectMeta, true)
}

type subnetStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
	checkConflicts bool
	subnetIndexer  cache.Indexer
}

var _ rest.RESTCreateStrategy = &subnetStrategy{}
var _ rest.RESTUpdateStrategy = &subnetStrategy{}
var _ rest.RESTDeleteStrategy = &subnetStrategy{}

func (*subnetStrategy) NamespaceScoped() bool {
	return true
}

func (*subnetStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	subnet := obj.(*network.Subnet)
	subnet.ExtendedObjectMeta = network.ExtendedObjectMeta{}
	subnet.Writes = subnet.Writes.SetWrite(network.SubnetSectionSpec, network.Now())
	subnet.Status = network.SubnetStatus{}
}

func (*subnetStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	oldSubnet := old.(*network.Subnet)
	newSubnet := obj.(*network.Subnet)
	newSubnet.Status = oldSubnet.Status
	newSubnet.ExtendedObjectMeta.Writes = oldSubnet.ExtendedObjectMeta.Writes
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

func (ss *subnetStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	s := obj.(*network.Subnet)

	errs := apimachineryvalidation.ValidateObjectMeta(&s.ObjectMeta, true, func(name string, prefix bool) []string { return nil }, field.NewPath("metadata"))

	subnetSummary, parsingErrs := subnet.NewSummary(s)
	for _, e := range parsingErrs {
		if e.Reason == subnet.VNIOutOfRange {
			errs = append(errs, field.Invalid(field.NewPath("spec", "vni"), strconv.FormatUint(uint64(s.Spec.VNI), 10), e.Error()))
		}
		if e.Reason == subnet.MalformedCIDR {
			errs = append(errs, field.Invalid(field.NewPath("spec", "ipv4"), s.Spec.IPv4, e.Error()))
		}
	}

	if len(errs) > 0 || !ss.checkConflicts {
		// The only checks left are those for conflicts with other subnets. If
		// we're here either such checks are disabled or basic validation (such
		// as CIDR syntax checks) failed, hence we return.
		return errs
	}

	return append(errs, ss.checkNSAndCIDRConflicts(subnetSummary)...)
}

func (ss *subnetStrategy) checkNSAndCIDRConflicts(candidate *subnet.Summary) (errs field.ErrorList) {
	potentialRivals, err := ss.subnetIndexer.ByIndex(subnetVNIIndex, strconv.FormatUint(uint64(candidate.VNI), 10))
	if err != nil {
		klog.Errorf("subnetIndexer.ByIndex failed for index %s and VNI %06x: %s", subnetVNIIndex, candidate.VNI, err.Error())
		errs = field.ErrorList{field.InternalError(field.NewPath("spec", "vni"), errors.New("failed to retrieve other subnets with same vni"))}
		return
	}
	klog.V(5).Infof("Found %d subnets with VNI %06x", len(potentialRivals), candidate.VNI)
	// Check whether there are Namespace and CIDR conflicts with other subnets.
	for _, potentialRival := range potentialRivals {
		pr, err := subnet.NewSummary(potentialRival)
		// Make sure potentialRival is well formed. The code in this file makes
		// it impossible to create a malformed subnet, but the check is done in
		// case this version of strategy is rolled out after a more lenient one
		// which let malformed subnets through.
		if err != nil {
			prMeta := potentialRival.(k8smetav1.Object)
			klog.V(6).Infof("Skipping %s/%s while validating %s because parsing failed: %s.", prMeta.GetNamespace(), prMeta.GetName(), candidate.NamespacedName, err.Error())
			continue
		}
		klog.V(2).Infof("Validating %s against %s", candidate.NamespacedName, pr.NamespacedName)
		if candidate.NSConflict(pr) {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "vni"), fmt.Sprintf("subnets with same VNI must be within same namespace, but %s has the same VNI and a different namespace", pr.NamespacedName)))
		}
		if candidate.CIDRConflict(pr) {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "ipv4"), fmt.Sprintf("subnets with same VNI must have disjoint CIDRs, but CIDR overlaps with %s's", pr.NamespacedName)))
		}
	}
	return errs
}

func (*subnetStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (*subnetStrategy) AllowUnconditionalUpdate() bool {
	return false
}

func (*subnetStrategy) Canonicalize(obj runtime.Object) {
}

func (ss *subnetStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newS, oldS := obj.(*network.Subnet), old.(*network.Subnet)

	errs := apimachineryvalidation.ValidateObjectMeta(&newS.ObjectMeta, true, func(name string, prefix bool) []string { return nil }, field.NewPath("metadata"))
	errs = append(errs, helper.ValidateExtendedObjectMeta(newS.ExtendedObjectMeta)...)

	immutableFieldMsg := "attempt to update immutable field"
	if newS.Spec.VNI != oldS.Spec.VNI {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "vni"), immutableFieldMsg))
	}
	if newS.Spec.IPv4 != oldS.Spec.IPv4 {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "ipv4"), immutableFieldMsg))
	}
	return errs
}

func SubnetVNI(obj interface{}) ([]string, error) {
	s, isInternalVersionSubnet := obj.(*network.Subnet)
	if isInternalVersionSubnet {
		return []string{strconv.FormatUint(uint64(s.Spec.VNI), 10)}, nil
	}
	return nil, errors.New("received object which is not an internal version subnet")
}

type subnetStatusStrategy struct {
	*subnetStrategy
}

var _ rest.RESTUpdateStrategy = subnetStatusStrategy{}

func (subnetStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

func (subnetStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newSubnet := obj.(*network.Subnet)
	oldSubnet := old.(*network.Subnet)
	// update is not allowed to set spec
	newSubnet.Spec = oldSubnet.Spec
	newSubnet.ExtendedObjectMeta.Writes = oldSubnet.ExtendedObjectMeta.Writes
	now := network.Now()
	if newSubnet.Status.Validated != oldSubnet.Status.Validated || !SliceOfStringEqual(newSubnet.Status.Errors, oldSubnet.Status.Errors) {
		newSubnet.Writes = newSubnet.Writes.SetWrite(network.SubnetSectionStatus, now)
	}
}

func (subnetStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	subnet := obj.(*network.Subnet)
	errs := apimachineryvalidation.ValidateObjectMeta(&subnet.ObjectMeta, true, func(name string, prefix bool) []string { return nil }, field.NewPath("metadata"))
	return append(errs, helper.ValidateExtendedObjectMeta(subnet.ExtendedObjectMeta)...)
}
