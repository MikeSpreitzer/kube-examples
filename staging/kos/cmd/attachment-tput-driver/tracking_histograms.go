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

package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

type TrackingHistogram interface {
	prometheus.Histogram
	ObserveAt(x float64, ns, name string)
	DumpToLog()
	DumpToLogWLabels(labels string)
}

type trackingHistogram struct {
	prometheus.Histogram
	name string

	statMu  sync.RWMutex
	maxX    float64
	maxNS   string
	maxName string
}

var _ TrackingHistogram = &trackingHistogram{}

func (th *trackingHistogram) ObserveAt(x float64, ns, name string) {
	th.Histogram.Observe(x)
	th.statMu.Lock()
	defer th.statMu.Unlock()
	if x > th.maxX {
		th.maxX = x
		th.maxNS = ns
		th.maxName = name
		// fmt.Printf("%s: maxX=%g, maxAt=%s/%s\n", th.name, th.maxX, th.maxNS, th.maxName)
	}
}

func (th *trackingHistogram) DumpToLog() {
	th.statMu.RLock()
	defer th.statMu.RUnlock()
	glog.Warningf("TrackingHistogram stats: histogram=%s, maxX=%g, maxAt=%s/%s",
		th.name,
		th.maxX,
		th.maxNS,
		th.maxName)
}

func (th *trackingHistogram) DumpToLogWLabels(labels string) {
	th.statMu.RLock()
	defer th.statMu.RUnlock()
	glog.Warningf("TrackingHistogram stats: histogram=%s, labels=%s, maxX=%g, maxAt=%s/%s",
		th.name,
		labels,
		th.maxX,
		th.maxNS,
		th.maxName)
}

func NewTrackingHistogram(opts prometheus.HistogramOpts) TrackingHistogram {
	return &trackingHistogram{
		name:      opts.Name,
		Histogram: prometheus.NewHistogram(opts),
	}
}

type TrackingHistogramVec interface {
	With(prometheus.Labels) TrackingHistogram
	DumpToLog()
	List() []prometheus.Collector
}

type trackingHistogramVec struct {
	sync.RWMutex
	labels2UsedTrackingHistogram   map[string]TrackingHistogram
	labels2UnusedTrackingHistogram map[string]TrackingHistogram
}

var _ TrackingHistogramVec = &trackingHistogramVec{}

func (thv *trackingHistogramVec) List() []prometheus.Collector {
	thv.RLock()
	defer thv.RUnlock()

	res := make([]prometheus.Collector, 0, len(thv.labels2UnusedTrackingHistogram)+len(thv.labels2UsedTrackingHistogram))
	for _, th := range thv.labels2UnusedTrackingHistogram {
		res = append(res, th)
	}
	for _, th := range thv.labels2UsedTrackingHistogram {
		res = append(res, th)
	}
	return res
}

func (thv *trackingHistogramVec) DumpToLog() {
	thv.RLock()
	defer thv.RUnlock()

	for labels, th := range thv.labels2UsedTrackingHistogram {
		th.DumpToLogWLabels(labels)
	}
}

func (thv *trackingHistogramVec) With(l prometheus.Labels) TrackingHistogram {
	labels := joinLabels(l)

	thv.Lock()
	defer thv.Unlock()

	if th, found := thv.labels2UnusedTrackingHistogram[labels]; found {
		delete(thv.labels2UnusedTrackingHistogram, labels)
		thv.labels2UsedTrackingHistogram[labels] = th
		return th
	}
	if th, found := thv.labels2UsedTrackingHistogram[labels]; found {
		return th
	}
	panic(fmt.Sprintf("no histogram with labels %s was found", labels))
}

// NewTrackingHistogramVec returns a TrackingHistogramVec that mimicks a
// Prometheus HistogramVec, in the sense that invoking `.With(labels)` on the
// returned TrackingHistogramVec returns the appropriate Histogram. However,
// unlike for a real Prometheus HistogramVec, the set of Histograms in the
// returned TrackingHistogramVec is fixed and determined by `potentialLabels` at
// creation time. This is done because maintaining a dynamic set of Histograms
// would require TrackingHistogramVec implementers to implement interfaces
// defined in the Prometheus package, which is far from trivial. Notice that the
// chosen approach works well only if the number of managed histograms is small.
//
// For each item `pl` in `potentialLabels` a Prometheus Histogram whose const
// labels are obtained by merging the labels in `opts` and `pl` is created. `pl`
// is turned into const labels because all the Histograms have the same name and
// the same label names, and registering multiple metrics with the same name and
// the same non-const label names is disallowed; OTOH, registering multiple
// metrics with the same name and same const label names (but different values)
// is allowed.
func NewTrackingHistogramVec(opts prometheus.HistogramOpts, potentialLabels []prometheus.Labels) TrackingHistogramVec {
	labels2TrackingHistogram := map[string]TrackingHistogram{}
	for _, pl := range potentialLabels {
		labels := joinLabels(pl)
		if _, found := labels2TrackingHistogram[labels]; found {
			panic("labels can occurr at most once but " + labels + " appear more than once")
		}
		optsC := opts
		optsC.ConstLabels = make(prometheus.Labels, len(opts.ConstLabels))
		for lName, lVal := range opts.ConstLabels {
			optsC.ConstLabels[lName] = lVal
		}
		for lName, lVal := range pl {
			if _, found := optsC.ConstLabels[lName]; found {
				panic("label names can appear in at most one of const and potential labels, \"" +
					lName +
					"\" was found in both")
			}
			optsC.ConstLabels[lName] = lVal
		}
		labels2TrackingHistogram[labels] = NewTrackingHistogram(optsC)
	}
	return &trackingHistogramVec{
		labels2UnusedTrackingHistogram: labels2TrackingHistogram,
		labels2UsedTrackingHistogram:   map[string]TrackingHistogram{},
	}
}

// joinLabels returns the content of `labels` in the form
// "l1Name=l1Val, ... , lnName=lnVal", where liName is
// the ith label name in alphabetical order.
func joinLabels(labels prometheus.Labels) string {
	labelsBuf := make([]string, 0, len(labels))
	for lName, lVal := range labels {
		labelsBuf = append(labelsBuf, lName+"="+lVal)
	}
	sort.Strings(labelsBuf)
	return strings.Join(labelsBuf, ", ")
}
