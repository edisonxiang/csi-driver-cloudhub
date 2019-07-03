/*
Copyright 2019 The KubeEdge Authors.

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

package cloudhub

import (
	"fmt"

	"github.com/golang/glog"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100
)

type CloudHub struct {
	name      string
	nodeID    string
	version   string
	endpoint  string
	ephemeral bool

	ids *identityServer
	cs  *controllerServer
}

var (
	vendorVersion = "dev"
)

// NewCloudHubDriver creates a new cloudhub driver
func NewCloudHubDriver(driverName, nodeID, endpoint, version string, ephemeral bool) (*CloudHub, error) {
	if driverName == "" {
		return nil, fmt.Errorf("No driver name provided")
	}

	if nodeID == "" {
		return nil, fmt.Errorf("No node id provided")
	}

	if endpoint == "" {
		return nil, fmt.Errorf("No driver endpoint provided")
	}
	if version != "" {
		vendorVersion = version
	}

	glog.Infof("Driver: %v ", driverName)
	glog.Infof("Version: %s", vendorVersion)

	return &CloudHub{
		name:      driverName,
		version:   vendorVersion,
		nodeID:    nodeID,
		endpoint:  endpoint,
		ephemeral: ephemeral,
	}, nil
}

func (hp *CloudHub) Run() {
	// Create GRPC servers
	hp.ids = NewIdentityServer(hp.name, hp.version)
	hp.cs = NewControllerServer(hp.ephemeral)

	s := NewNonBlockingGRPCServer()
	s.Start(hp.endpoint, hp.ids, hp.cs, nil)
	s.Wait()
}
