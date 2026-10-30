/*
Copyright 2026 The Kubernetes Authors.

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

package tests

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	gcedriver "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-pd-csi-driver"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/k8sclient"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GCE PD CSI Driver ControllerModifyVolume", func() {
	It("should modify IOPS and throughput on the same disk type without triggering conversion", func() {
		if !runCMVTests() {
			Skip("Not running ControllerModifyVolume tests, as RUN_CONTROLLER_MODIFY_VOLUME_TESTS is falsy")
		}

		testContext := getRandomMwTestContext()
		client := testContext.Client
		p, z, _ := testContext.Instance.GetIdentity()
		volName := testNamePrefix + string(uuid.NewUUID())

		initialIOPS := provisionedIOPSOnCreateHdbInt
		initialThroughput := provisionedThroughputOnCreateHdbInt
		updatedIOPS := int64(3013)
		updatedThroughput := int64(181)

		params := map[string]string{
			parameters.ParameterKeyType:                          hdbDiskType,
			parameters.ParameterKeyProvisionedIOPSOnCreate:       provisionedIOPSOnCreateHdb,
			parameters.ParameterKeyProvisionedThroughputOnCreate: provisionedThroughputOnCreateHdb,
		}
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: z}},
			},
		}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed: %v", err)

		defer func() {
			err := client.DeleteVolume(volume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume failed: %v", err)
			project, key, err := common.VolumeIDToKey(volume.VolumeId)
			Expect(err).To(BeNil(), "Failed to parse volume ID: %v", err)
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		disk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk before modification: %v", err)
		Expect(disk.Status).To(Equal(readyState))
		Expect(disk.Type).To(ContainSubstring(hdbDiskType))
		Expect(disk.ProvisionedIops).To(Equal(initialIOPS))
		Expect(disk.ProvisionedThroughput).To(Equal(initialThroughput))
		initialDiskType := disk.Type

		err = client.ControllerModifyVolume(volume.VolumeId, map[string]string{
			"type":       hdbDiskType,
			"iops":       "3013",
			"throughput": "181Mi",
		})
		Expect(err).To(BeNil(), "ControllerModifyVolume should update performance without starting a conversion: %v", err)

		err = wait.Poll(10*time.Second, 5*time.Minute, func() (bool, error) {
			disk, err := computeService.Disks.Get(p, z, volName).Do()
			if err != nil {
				return false, err
			}
			return disk.ProvisionedIops == updatedIOPS && disk.ProvisionedThroughput == updatedThroughput, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for IOPS and throughput to update: %v", err)

		disk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk after modification: %v", err)
		Expect(disk.Type).To(Equal(initialDiskType), "Disk type changed during performance modification")
		Expect(disk.ProvisionedIops).To(Equal(updatedIOPS))
		Expect(disk.ProvisionedThroughput).To(Equal(updatedThroughput))

		klog.Infof("Updated IOPS and throughput on %s without changing disk type %s", volName, disk.Type)
	})
})

const (
	conversionTestProject = "pd-to-hd-conversion-test"
	conversionTestZone    = "us-central1-a"
	conversionTestDisk    = "pd-to-hd-conversion-test-disk"
	conversionTestNode    = "pd-to-hd-conversion-test-node"
	conversionTestVAC     = "pd-to-hd-conversion-test-vac"
	conversionTestPVC     = "pd-to-hd-conversion-test-pvc"
	conversionTestPod     = "pd-to-hd-conversion-test-pod"
)

type pdToHDConversionFixture struct {
	controller *gcedriver.GCEControllerServer
	cloud      *gce.FakeCloudProvider
	kube       *k8sfake.Clientset
	disk       *compute.Disk
	volumeID   string
	nodeID     string
}

func newPdToHDConversionFixture(t *testing.T, diskType string, users []string, annotations map[string]string, withVAC bool, conversionErr error) *pdToHDConversionFixture {
	t.Helper()

	volumeID := fmt.Sprintf("projects/%s/zones/%s/disks/%s", conversionTestProject, conversionTestZone, conversionTestDisk)
	nodeID := fmt.Sprintf("projects/%s/zones/%s/instances/%s", conversionTestProject, conversionTestZone, conversionTestNode)
	disk := &compute.Disk{
		Name:     conversionTestDisk,
		Zone:     conversionTestZone,
		SelfLink: fmt.Sprintf("https://www.googleapis.com/compute/v1/%s", volumeID),
		Type:     diskType,
		SizeGb:   200,
		Status:   readyState,
		Users:    users,
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: conversionTestDisk, Annotations: annotations},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Name: conversionTestPVC, Namespace: metav1.NamespaceDefault},
		},
	}
	objects := []runtime.Object{pv}
	className := conversionTestVAC
	if withVAC {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: conversionTestPVC, Namespace: metav1.NamespaceDefault},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeAttributesClassName: &className},
		}
		vac := &storagev1beta1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			DriverName: constants.DriverName,
			Parameters: map[string]string{"type": hdbDiskType},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: conversionTestPod, Namespace: metav1.NamespaceDefault},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: conversionTestPVC},
					},
				}},
			},
		}
		objects = append(objects, pvc, vac, pod)
	}

	kube := k8sfake.NewSimpleClientset(objects...)
	previousGetClient := k8sclient.GetClient
	k8sclient.GetClient = func() (kubernetes.Interface, error) { return kube, nil }

	cloud, err := gce.CreateFakeCloudProvider(conversionTestProject, conversionTestZone, []*gce.CloudDisk{
		gce.CloudDiskFromV1(disk),
	})
	if err != nil {
		k8sclient.GetClient = previousGetClient
		t.Fatalf("failed to create fake Compute provider: %v", err)
	}
	cloud.ConversionTestParams.TypeConversionErr = conversionErr

	instanceDisks := []*compute.AttachedDisk(nil)
	if len(users) > 0 {
		keyProject, key, keyErr := common.VolumeIDToKey(volumeID)
		if keyErr != nil {
			k8sclient.GetClient = previousGetClient
			t.Fatalf("failed to parse test volume ID: %v", keyErr)
		}
		instanceDisks = append(instanceDisks, &compute.AttachedDisk{
			DeviceName: conversionTestDisk,
			Mode:       "READ_WRITE",
			Source:     cloud.GetDiskSourceURI(keyProject, key),
			Type:       "PERSISTENT",
		})
	}
	cloud.InsertInstance(&compute.Instance{
		Name:        conversionTestNode,
		Zone:        conversionTestZone,
		MachineType: "projects/" + conversionTestProject + "/zones/" + conversionTestZone + "/machineTypes/c3-standard-4",
		Disks:       instanceDisks,
	}, conversionTestZone, conversionTestNode)

	driver := gcedriver.GetGCEDriver()
	controller := gcedriver.NewControllerServer(
		driver,
		cloud,
		200*time.Millisecond,
		time.Minute,
		nil,
		false,
		false,
		gcedriver.MultiZoneVolumeHandleConfig{},
		gcedriver.ListVolumesConfig{},
		gcedriver.ProvisionableDisksConfig{
			SupportsIopsChange:       []string{hdbDiskType, hdxDiskType},
			SupportsThroughputChange: []string{hdbDiskType, hdtDiskType, hdmlDiskType},
		},
		false,
		&gcedriver.GCEControllerServerArgs{EnablePdConversion: true},
	)
	if err := driver.SetupGCEDriver(constants.DriverName, "test", nil, nil, nil, controller, nil); err != nil {
		k8sclient.GetClient = previousGetClient
		t.Fatalf("failed to set up test controller: %v", err)
	}

	t.Cleanup(func() {
		controller.WaitForConversionWorkers()
		k8sclient.GetClient = previousGetClient
	})

	return &pdToHDConversionFixture{
		controller: controller,
		cloud:      cloud,
		kube:       kube,
		disk:       disk,
		volumeID:   volumeID,
		nodeID:     nodeID,
	}
}

func conversionAPIErrorForE2E(code int, reason, message string) error {
	return &googleapi.Error{
		Code:   code,
		Errors: []googleapi.ErrorItem{{Reason: reason, Message: message}},
	}
}

func TestPDToHDConversionStartsForDetachedDisk(t *testing.T) {
	fixture := newPdToHDConversionFixture(t, "pd-balanced", nil, nil, false, nil)

	_, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ControllerModifyVolume status = %v (err %v), want Unavailable while conversion runs", status.Code(err), err)
	}
	if !fixture.cloud.ConversionCalled() {
		t.Fatal("disk conversion was not started for the detached disk")
	}

	fixture.controller.WaitForConversionWorkers()
	if got := gce.CloudDiskFromV1(fixture.disk).GetPDType(); got != hdbDiskType {
		t.Errorf("disk type = %q, want %q after conversion", got, hdbDiskType)
	}
}

func TestPDToHDConversionWaitsForWorkloadDetach(t *testing.T) {
	users := []string{fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s", conversionTestProject, conversionTestZone, conversionTestNode)}
	fixture := newPdToHDConversionFixture(t, "pd-balanced", users, nil, true, nil)

	_, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ControllerModifyVolume status = %v (err %v), want FailedPrecondition for attached disk", status.Code(err), err)
	}
	if fixture.cloud.ConversionCalled() {
		t.Fatal("conversion started while the disk was attached to the workload")
	}

	if err := fixture.kube.CoreV1().Pods(metav1.NamespaceDefault).Delete(context.Background(), conversionTestPod, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to terminate workload Pod: %v", err)
	}
	if fixture.cloud.ConversionCalled() {
		t.Fatal("conversion started when the Pod terminated but before disk detach")
	}

	fixture.disk.Users = nil
	if _, err := fixture.controller.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: fixture.volumeID,
		NodeId:   fixture.nodeID,
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume failed after Pod termination: %v", err)
	}
	fixture.controller.WaitForConversionWorkers()
	if !fixture.cloud.ConversionCalled() {
		t.Fatal("queued conversion did not start after the disk detached")
	}
	if got := gce.CloudDiskFromV1(fixture.disk).GetPDType(); got != hdbDiskType {
		t.Errorf("disk type = %q after detach-triggered conversion, want %q", got, hdbDiskType)
	}
}

func TestPDToHDConversionBlocksAttachmentWhileInProgress(t *testing.T) {
	const operation = "https://www.googleapis.com/compute/alpha/projects/conversion-test/zones/us-central1-a/operations/convert-1"
	annotations := map[string]string{
		constants.DiskTypeConversionOperationKey: operation,
		constants.DiskTypeConvertedFromKey:       "pd-balanced",
		constants.DiskTypeConvertedToKey:         hdbDiskType,
	}
	fixture := newPdToHDConversionFixture(t, hdbDiskType, nil, annotations, false, nil)

	_, err := fixture.controller.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: fixture.volumeID,
		NodeId:   fixture.nodeID,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ControllerPublishVolume status = %v (err %v), want Unavailable during conversion", status.Code(err), err)
	}
	if got := fixture.cloud.ConversionTestParams.AttachCallCount; got != 0 {
		t.Fatalf("disk attach was attempted %d times while conversion was active, want 0", got)
	}
}

func TestPDToHDConversionResumesAfterControllerRestart(t *testing.T) {
	const operation = "https://www.googleapis.com/compute/alpha/projects/conversion-test/zones/us-central1-a/operations/convert-restart"
	annotations := map[string]string{
		constants.DiskTypeConversionOperationKey: operation,
		constants.DiskTypeConvertedFromKey:       "pd-balanced",
		constants.DiskTypeConvertedToKey:         hdbDiskType,
	}
	fixture := newPdToHDConversionFixture(t, hdbDiskType, nil, annotations, false, nil)

	_, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ControllerModifyVolume status after restart = %v (err %v), want Unavailable while operation is reconciled", status.Code(err), err)
	}
	if fixture.cloud.ConversionCalled() {
		t.Fatal("controller restarted the Compute conversion instead of resuming the recorded operation")
	}

	fixture.controller.WaitForConversionWorkers()
	pv, err := fixture.kube.CoreV1().PersistentVolumes().Get(context.Background(), conversionTestDisk, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get PersistentVolume after restart recovery: %v", err)
	}
	if operation := pv.Annotations[constants.DiskTypeConversionOperationKey]; operation != "" {
		t.Fatalf("conversion operation annotation remains %q after completed recovery", operation)
	}

	if _, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	}); err != nil {
		t.Fatalf("ControllerModifyVolume did not succeed after recovered conversion completed: %v", err)
	}
	if fixture.cloud.ConversionCalled() {
		t.Fatal("completed conversion was submitted to Compute again")
	}
}

func TestPDToHDConversionRetriesAfterInstantSnapshot(t *testing.T) {
	assertConversionRetryResumes(t, "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE", http.StatusBadRequest, "active instant snapshot is still using the disk")
}

func TestPDToHDConversionRetriesRateLimitAndQuotaFailures(t *testing.T) {
	testCases := []struct {
		name    string
		reason  string
		code    int
		message string
	}{
		{
			name:    "regional API rate limit",
			reason:  "rateLimitExceeded",
			code:    http.StatusTooManyRequests,
			message: "too many disk conversion requests",
		},
		{
			name:    "conversion quota",
			reason:  "quotaExceeded",
			code:    http.StatusForbidden,
			message: "conversion quota exceeded",
		},
		{
			name:    "target disk stockout",
			reason:  "ZONE_RESOURCE_POOL_EXHAUSTED",
			code:    http.StatusServiceUnavailable,
			message: "target disk capacity is temporarily unavailable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertConversionRetryResumes(t, tc.reason, tc.code, tc.message)
		})
	}
}

func assertConversionRetryResumes(t *testing.T, reason string, code int, message string) {
	t.Helper()
	fixture := newPdToHDConversionFixture(t, "pd-balanced", nil, nil, false, conversionAPIErrorForE2E(code, reason, message))
	request := &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	}

	_, err := fixture.controller.ControllerModifyVolume(context.Background(), request)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("first ControllerModifyVolume status = %v (err %v), want Unavailable for retriable %s", status.Code(err), err, reason)
	}
	pv, err := fixture.kube.CoreV1().PersistentVolumes().Get(context.Background(), conversionTestDisk, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get PersistentVolume after retriable failure: %v", err)
	}
	if got := pv.Annotations[constants.DiskTypeConversionOperationKey]; got != constants.ConversionStatePending {
		t.Fatalf("conversion operation after retriable failure = %q, want pending", got)
	}
	if len(fixture.cloud.ConversionTestParams.TypeConversionRequestIDs) != 1 {
		t.Fatalf("conversion request IDs after first attempt = %v, want one request", fixture.cloud.ConversionTestParams.TypeConversionRequestIDs)
	}
	requestID := fixture.cloud.ConversionTestParams.TypeConversionRequestIDs[0]

	fixture.cloud.ConversionTestParams.TypeConversionErr = nil
	_, err = fixture.controller.ControllerModifyVolume(context.Background(), request)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("retry ControllerModifyVolume status = %v (err %v), want Unavailable while accepted operation runs", status.Code(err), err)
	}
	fixture.controller.WaitForConversionWorkers()

	requestIDs := fixture.cloud.ConversionTestParams.TypeConversionRequestIDs
	if len(requestIDs) != 2 {
		t.Fatalf("conversion request IDs after retry = %v, want two attempts", requestIDs)
	}
	if requestIDs[1] != requestID {
		t.Fatalf("retry request ID = %q, want stable original ID %q", requestIDs[1], requestID)
	}
	if got := gce.CloudDiskFromV1(fixture.disk).GetPDType(); got != hdbDiskType {
		t.Errorf("disk type = %q after retry, want %q", got, hdbDiskType)
	}
}

func TestPDToHDConversionRejectsUnsupportedTarget(t *testing.T) {
	fixture := newPdToHDConversionFixture(t, "pd-balanced", nil, nil, true,
		conversionAPIErrorForE2E(http.StatusBadRequest, "DISK_TYPE_CONVERSION_UNSUPPORTED", "conversion path is unsupported"))

	_, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ControllerModifyVolume status = %v (err %v), want InvalidArgument for unsupported conversion", status.Code(err), err)
	}

	pv, err := fixture.kube.CoreV1().PersistentVolumes().Get(context.Background(), conversionTestDisk, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get PersistentVolume after terminal failure: %v", err)
	}
	if got := pv.Annotations[constants.DiskTypeConversionInfeasibleTargetKey]; got != hdbDiskType {
		t.Fatalf("infeasible target = %q, want %q", got, hdbDiskType)
	}
	if got := pv.Annotations[constants.DiskTypeConversionInfeasibleKey]; got == "" {
		t.Fatal("terminal conversion failure was not recorded on the PersistentVolume")
	}
	if _, exists := pv.Annotations[constants.DiskTypeConversionOperationKey]; exists {
		t.Fatalf("terminal failure left conversion state %q", pv.Annotations[constants.DiskTypeConversionOperationKey])
	}
	if fixture.cloud.ConversionTestParams.TypeConversionCallCount != 1 {
		t.Fatalf("conversion API calls = %d, want 1", fixture.cloud.ConversionTestParams.TypeConversionCallCount)
	}
}

func TestPDToHDConversionRejectsOutOfSpecVACPerformance(t *testing.T) {
	testCases := []struct {
		name       string
		parameters map[string]string
		message    string
	}{
		{
			name:       "IOPS above target limit",
			parameters: map[string]string{"type": hdbDiskType, "iops": "100001"},
			message:    "provisioned IOPS exceeds the maximum",
		},
		{
			name:       "throughput above target limit",
			parameters: map[string]string{"type": hdbDiskType, "throughput": "100001Mi"},
			message:    "provisioned throughput exceeds the maximum",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPdToHDConversionFixture(t, "pd-balanced", nil, nil, true,
				conversionAPIErrorForE2E(http.StatusBadRequest, "badRequest", tc.message))
			_, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
				VolumeId:          fixture.volumeID,
				MutableParameters: tc.parameters,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("ControllerModifyVolume status = %v (err %v), want InvalidArgument for out-of-spec performance", status.Code(err), err)
			}

			pv, err := fixture.kube.CoreV1().PersistentVolumes().Get(context.Background(), conversionTestDisk, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get PersistentVolume after rejected performance request: %v", err)
			}
			if got := pv.Annotations[constants.DiskTypeConversionInfeasibleTargetKey]; got != hdbDiskType {
				t.Fatalf("infeasible target = %q, want %q", got, hdbDiskType)
			}
			if got := pv.Annotations[constants.DiskTypeConversionInfeasibleKey]; got == "" {
				t.Fatal("out-of-spec performance was not recorded as an infeasible conversion")
			}
			if fixture.cloud.ConversionTestParams.TypeConversionCallCount != 1 {
				t.Fatalf("conversion API calls = %d, want 1", fixture.cloud.ConversionTestParams.TypeConversionCallCount)
			}
		})
	}
}

func TestPDToHDConversionCancelsPendingRetryWhenVACIsCleared(t *testing.T) {
	users := []string{fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s", conversionTestProject, conversionTestZone, conversionTestNode)}
	fixture := newPdToHDConversionFixture(t, "pd-balanced", users, nil, true, nil)

	_, err := fixture.controller.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          fixture.volumeID,
		MutableParameters: map[string]string{"type": hdbDiskType},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ControllerModifyVolume status = %v (err %v), want FailedPrecondition while disk is attached", status.Code(err), err)
	}

	pvc, err := fixture.kube.CoreV1().PersistentVolumeClaims(metav1.NamespaceDefault).Get(context.Background(), conversionTestPVC, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get PVC: %v", err)
	}
	pvc.Spec.VolumeAttributesClassName = nil
	if _, err := fixture.kube.CoreV1().PersistentVolumeClaims(metav1.NamespaceDefault).Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to clear PVC VolumeAttributesClass: %v", err)
	}

	fixture.disk.Users = nil
	if _, err := fixture.controller.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: fixture.volumeID,
		NodeId:   fixture.nodeID,
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume failed after clearing VAC: %v", err)
	}
	fixture.controller.WaitForConversionWorkers()

	if fixture.cloud.ConversionCalled() {
		t.Fatal("conversion was started after the PVC no longer requested the Hyperdisk type")
	}
	pv, err := fixture.kube.CoreV1().PersistentVolumes().Get(context.Background(), conversionTestDisk, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get PersistentVolume after queued conversion cancellation: %v", err)
	}
	if _, exists := pv.Annotations[constants.DiskTypeConversionOperationKey]; exists {
		t.Fatalf("queued conversion annotation remains %q after VAC was cleared", pv.Annotations[constants.DiskTypeConversionOperationKey])
	}
}
