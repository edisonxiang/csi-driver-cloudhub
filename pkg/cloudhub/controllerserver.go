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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/golang/glog"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/kubeedge/beehive/pkg/core/model"
)

const (
	deviceID           = "deviceID"
	provisionRoot      = "/csi-data-dir"
	maxStorageCapacity = tib
)

type accessType int

const (
	mountAccess accessType = iota
	blockAccess
)

type controllerServer struct {
	caps []*csi.ControllerServiceCapability
}

// NewControllerServer creates controller server
func NewControllerServer(ephemeral bool) *controllerServer {
	if ephemeral {
		return &controllerServer{caps: getControllerServiceCapabilities(nil)}
	}
	return &controllerServer{
		caps: getControllerServiceCapabilities(
			[]csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			}),
	}
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid create volume req: %v", req)
		return nil, err
	}

	// Check arguments
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	// Keep a record of the requested access types.
	var accessTypeMount, accessTypeBlock bool

	for _, cap := range caps {
		if cap.GetBlock() != nil {
			accessTypeBlock = true
		}
		if cap.GetMount() != nil {
			accessTypeMount = true
		}
	}
	// A real driver would also need to check that the other
	// fields in VolumeCapabilities are sane. The check above is
	// just enough to pass the "[Testpattern: Dynamic PV (block
	// volmode)] volumeMode should fail in binding dynamic
	// provisioned PV to PVC" storage E2E test.

	if accessTypeBlock && accessTypeMount {
		return nil, status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
	}

	// Check for maximum available capacity
	capacity := int64(req.GetCapacityRange().GetRequiredBytes())
	if capacity >= maxStorageCapacity {
		return nil, status.Errorf(codes.OutOfRange, "Requested capacity %d exceeds maximum allowed %d", capacity, maxStorageCapacity)
	}

	volumeID := uuid.NewUUID().String()

	// Build message struct
	msg := model.NewMessage("")
	resource, err := buildResource("fb4ebb70-2783-42b8-b3ef-63e2fd6d242e", CSINamespaceDefault, CSIResourceTypeVolume, volumeID)
	if err != nil {
		glog.Errorf("Build message resource failed with error: %s", err)
		return nil, err
	}

	m := jsonpb.Marshaler{}
	js, err := m.MarshalToString(req)
	if err != nil {
		glog.Errorf("MarshalToString failed with error: %s", err)
		return nil, err
	}
	glog.Infof("CreateVolume MarshalToString: %s", js)
	msg.Content = js
	msg.BuildRouter("cloudhub", CSIGroupResource, resource, CSIOperationTypeCreateVolume)

	// Marshal message
	reqData, err := json.Marshal(msg)
	if err != nil {
		glog.Errorf("Marshal request failed with error: %v", err)
		return nil, err
	}

	// Send message to CloudHub
	resdata := send2CloudHub(string(reqData))

	// Unmarshal message
	result, err := extractMessage(resdata)
	if err != nil {
		glog.Errorf("Unmarshal response failed with error: %v", err)
		return nil, err
	}

	// Get message content
	var data []byte
	switch result.Content.(type) {
	case []byte:
		data = result.GetContent().([]byte)
	default:
		var err error
		data, err = json.Marshal(result.GetContent())
		if err != nil {
			glog.Errorf("Marshal result content with error: %s", err)
			return nil, err
		}
	}

	if string(data) != "OK" {
		glog.Errorf("CreateVolume with error: %s", string(data))
		return nil, errors.New(string(data))
	}

	createVolumeResponse := &csi.CreateVolumeResponse{}
	if req.GetVolumeContentSource() != nil {
		createVolumeResponse = &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: req.GetParameters(),
				ContentSource: req.GetVolumeContentSource(),
			},
		}
	} else {
		createVolumeResponse = &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: req.GetParameters(),
			},
		}
	}
	return createVolumeResponse, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid delete volume req: %v", req)
		return nil, err
	}

	// Build message struct
	msg := model.NewMessage("")
	resource, err := buildResource("fb4ebb70-2783-42b8-b3ef-63e2fd6d242e", CSINamespaceDefault, CSIResourceTypeVolume, req.GetVolumeId())
	if err != nil {
		glog.Errorf("Build message resource failed with error: %s", err)
		return nil, err
	}

	m := jsonpb.Marshaler{}
	js, err := m.MarshalToString(req)
	if err != nil {
		glog.Errorf("MarshalToString failed with error: %s", err)
		return nil, err
	}
	glog.Infof("DeleteVolume MarshalToString: %s", js)
	msg.Content = js
	msg.BuildRouter("cloudhub", CSIGroupResource, resource, CSIOperationTypeDeleteVolume)

	// Marshal message
	reqData, err := json.Marshal(msg)
	if err != nil {
		glog.Errorf("Marshal request failed with error: %v", err)
		return nil, err
	}

	// Send message to CloudHub
	resdata := send2CloudHub(string(reqData))

	// Unmarshal message
	result, err := extractMessage(resdata)
	if err != nil {
		glog.Errorf("Unmarshal response failed with error: %v", err)
		return nil, err
	}

	// Get message content
	var data []byte
	switch result.Content.(type) {
	case []byte:
		data = result.GetContent().([]byte)
	default:
		var err error
		data, err = json.Marshal(result.GetContent())
		if err != nil {
			glog.Errorf("Marshal result content with error: %s", err)
			return nil, err
		}
	}

	if string(data) != "OK" {
		glog.Errorf("DeleteVolume with error: %s", string(data))
		return nil, errors.New(string(data))
	}

	deleteVolumeResponse := &csi.DeleteVolumeResponse{}
	return deleteVolumeResponse, nil
}

func (cs *controllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.caps,
	}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {

	// Volume Attach
	instanceID := req.GetNodeId()
	volumeID := req.GetVolumeId()

	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume ID must be provided")
	}

	if len(instanceID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Instance ID must be provided")
	}

	// Build message struct
	msg := model.NewMessage("")
	resource, err := buildResource("fb4ebb70-2783-42b8-b3ef-63e2fd6d242e", CSINamespaceDefault, CSIResourceTypeVolume, volumeID)
	if err != nil {
		glog.Errorf("Build message resource failed with error: %s", err)
		return nil, err
	}

	m := jsonpb.Marshaler{}
	js, err := m.MarshalToString(req)
	if err != nil {
		glog.Errorf("MarshalToString failed with error: %s", err)
		return nil, err
	}
	glog.Infof("ControllerPublishVolume MarshalToString: %s", js)
	msg.Content = js
	msg.BuildRouter("cloudhub", CSIGroupResource, resource, CSIOperationTypeControllerPublishVolume)

	// Marshal message
	reqData, err := json.Marshal(msg)
	if err != nil {
		glog.Errorf("Marshal request failed with error: %v", err)
		return nil, err
	}

	// Send message to CloudHub
	resdata := send2CloudHub(string(reqData))

	// Unmarshal message
	result, err := extractMessage(resdata)
	if err != nil {
		glog.Errorf("Unmarshal response failed with error: %v", err)
		return nil, err
	}

	// Get message content
	var data []byte
	switch result.Content.(type) {
	case []byte:
		data = result.GetContent().([]byte)
	default:
		var err error
		data, err = json.Marshal(result.GetContent())
		if err != nil {
			glog.Errorf("Marshal result content with error: %s", err)
			return nil, err
		}
	}

	if string(data) != "OK" {
		glog.Errorf("ControllerPublishVolume with error: %s", string(data))
		return nil, errors.New(string(data))
	}

	controllerPublishVolumeResponse := &csi.ControllerPublishVolumeResponse{}
	return controllerPublishVolumeResponse, nil

	/* Publish Volume Info
	pvInfo := map[string]string{}
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: pvInfo,
	}, nil*/
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {

	// Volume Detach
	instanceID := req.GetNodeId()
	volumeID := req.GetVolumeId()

	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerUnpublishVolume Volume ID must be provided")
	}

	if len(instanceID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerUnpublishVolume Instance ID must be provided")
	}

	// Build message struct
	msg := model.NewMessage("")
	resource, err := buildResource("fb4ebb70-2783-42b8-b3ef-63e2fd6d242e", CSINamespaceDefault, CSIResourceTypeVolume, volumeID)
	if err != nil {
		glog.Errorf("Build message resource failed with error: %s", err)
		return nil, err
	}

	m := jsonpb.Marshaler{}
	js, err := m.MarshalToString(req)
	if err != nil {
		glog.Errorf("MarshalToString failed with error: %s", err)
		return nil, err
	}
	glog.Infof("ControllerUnpublishVolume MarshalToString: %s", js)
	msg.Content = js
	msg.BuildRouter("cloudhub", CSIGroupResource, resource, CSIOperationTypeControllerUnpublishVolume)

	// Marshal message
	reqData, err := json.Marshal(msg)
	if err != nil {
		glog.Errorf("Marshal request failed with error: %v", err)
		return nil, err
	}

	// Send message to CloudHub
	resdata := send2CloudHub(string(reqData))

	// Unmarshal message
	result, err := extractMessage(resdata)
	if err != nil {
		glog.Errorf("Unmarshal response failed with error: %v", err)
		return nil, err
	}

	// Get message content
	var data []byte
	switch result.Content.(type) {
	case []byte:
		data = result.GetContent().([]byte)
	default:
		var err error
		data, err = json.Marshal(result.GetContent())
		if err != nil {
			glog.Errorf("Marshal result content with error: %s", err)
			return nil, err
		}
	}

	if string(data) != "OK" {
		glog.Errorf("ControllerUnpublishVolume with error: %s", string(data))
		return nil, errors.New(string(data))
	}

	controllerUnpublishVolumeResponse := &csi.ControllerUnpublishVolumeResponse{}
	return controllerUnpublishVolumeResponse, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, req.VolumeId)
	}

	for _, cap := range req.GetVolumeCapabilities() {
		if cap.GetMount() == nil && cap.GetBlock() == nil {
			return nil, status.Error(codes.InvalidArgument, "cannot have both mount and block access type be undefined")
		}

		// A real driver would check the capabilities of the given volume with
		// the set of requested capabilities.
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (cs *controllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *controllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *controllerServer) validateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, cap := range cs.caps {
		if c == cap.GetRpc().GetType() {
			return nil
		}
	}
	return status.Error(codes.InvalidArgument, fmt.Sprintf("%s", c))
}

func getControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) []*csi.ControllerServiceCapability {
	var csc []*csi.ControllerServiceCapability

	for _, cap := range cl {
		glog.Infof("Enabling controller service capability: %v", cap.String())
		csc = append(csc, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return csc
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("ControllerExpandVolume is not yet implemented"))
}

func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("CreateSnapshot is not yet implemented"))
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("DeleteSnapshot is not yet implemented"))
}

func (cs *controllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("ListSnapshots is not yet implemented"))
}
