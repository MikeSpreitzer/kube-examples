/*
Copyright 2020 The Kubernetes Authors.

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

// Package helper contains helper functions for the internal version of the
// network API types.
package helper

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"k8s.io/examples/staging/kos/pkg/apis/network"
)

// ValidateExtendedObjectMeta returns a list of errors carrying information on
// why `eom` is invalid, or an empty list if `eom` is valid.
func ValidateExtendedObjectMeta(eom network.ExtendedObjectMeta) field.ErrorList {
	errs := field.ErrorList{}
	if err := ValidateClientWrite(eom.LastClientWrite); err != nil {
		path := field.NewPath("extendedObjectMeta").Child("lastClientWrite")
		errs = append(errs, field.Invalid(path, eom.LastClientWrite, err.Error()))
	}
	if err := ValidateControllerStart(eom.LastControllerStart); err != nil {
		path := field.NewPath("extendedObjectMeta").Child("lastControllerStart")
		errs = append(errs, field.Invalid(path, eom.LastControllerStart, err.Error()))
	}
	return errs
}

// ValidateClientWrite checks whether `cw` fields are consistent.
func ValidateClientWrite(cw network.ClientWrite) error {
	return validateTimedField(cw.Time, cw.Name, "Name")
}

// ValidateControllerStart checks whether `cs` fields are consistent.
func ValidateControllerStart(cs network.ControllerStart) error {
	return validateTimedField(cs.ControllerTime, cs.Controller, "Controller")
}

func validateTimedField(time metav1.MicroTime, fieldVal, fieldName string) error {
	if (time == metav1.MicroTime{} && fieldVal != "") {
		return fmt.Errorf("both %s and time must be set, but only %s is set", fieldName, fieldName)
	}
	if (time != metav1.MicroTime{} && fieldVal == "") {
		return fmt.Errorf("both %s and time must be set, but only time is set", fieldName)
	}
	return nil
}
