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
	"flag"
	"os"
	"runtime"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/logs"
	"k8s.io/klog"

	"k8s.io/examples/staging/kos/pkg/cmd/server"
	"k8s.io/examples/staging/kos/pkg/util/version"
)

func main() {
	flag.Set("alsologtostderr", "true")
	logs.InitLogs()
	defer logs.FlushLogs()

	if len(os.Getenv("GOMAXPROCS")) == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	stopCh := genericapiserver.SetupSignalHandler()
	options := server.NewNetworkAPIServerOptions(os.Stdout, os.Stderr)
	cmd := server.NewCommandStartNetworkAPIServer(options, stopCh)
	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	klog.Infof("Starting network-apiserver.  GitCommit=%q", version.GitCommit)
	if err := cmd.Execute(); err != nil {
		klog.Fatal(err)
	}
}
