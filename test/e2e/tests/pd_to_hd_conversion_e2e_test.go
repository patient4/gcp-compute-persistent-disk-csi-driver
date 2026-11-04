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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GCE PD CSI Driver ControllerModifyVolume", func() {
	It("should modify IOPS and throughput on the same disk type without triggering conversion", func() {
		if !runCMVTests() {
			Skip("Not running ControllerModifyVolume tests, as RUN_CONTROLLER_MODIFY_VOLUME_TESTS is falsy")
		}
		if len(mwTestContexts) == 0 {
			Skip("Not running ControllerModifyVolume test, as no multiwriter test contexts are configured")
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

	It("should reject out-of-range IOPS and throughput on Hyperdisk Balanced", func() {
		if !runCMVTests() {
			Skip("Not running ControllerModifyVolume tests, as RUN_CONTROLLER_MODIFY_VOLUME_TESTS is falsy")
		}
		if len(mwTestContexts) == 0 {
			Skip("Not running ControllerModifyVolume test, as no multiwriter test contexts are configured")
		}

		testContext := getRandomMwTestContext()
		project, zone, _ := testContext.Instance.GetIdentity()
		testCases := []struct {
			name       string
			parameters map[string]string
			errorText  string
		}{
			{
				name:       "IOPS above the supported maximum",
				parameters: map[string]string{"type": hdbDiskType, "iops": "100001"},
				errorText:  "provisioned IOPS exceeds the maximum",
			},
			{
				name:       "throughput above the supported maximum",
				parameters: map[string]string{"type": hdbDiskType, "throughput": "100001Mi"},
				errorText:  "provisioned throughput exceeds the maximum",
			},
		}

		for _, testCase := range testCases {
			By(testCase.name)
			volName := testNamePrefix + string(uuid.NewUUID())
			volume, err := testContext.Client.CreateVolume(volName, map[string]string{
				parameters.ParameterKeyType:                          hdbDiskType,
				parameters.ParameterKeyProvisionedIOPSOnCreate:       provisionedIOPSOnCreateHdb,
				parameters.ParameterKeyProvisionedThroughputOnCreate: provisionedThroughputOnCreateHdb,
			}, defaultHdBSizeGb, &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{constants.TopologyKeyZone: zone}},
				},
			}, nil)
			Expect(err).To(BeNil(), "CreateVolume failed: %v", err)
			if err != nil {
				continue
			}

			func() {
				defer func() {
					Expect(testContext.Client.DeleteVolume(volume.VolumeId)).To(Succeed())
					_, key, err := common.VolumeIDToKey(volume.VolumeId)
					Expect(err).To(BeNil(), "Failed to parse volume ID: %v", err)
					_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
					Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
				}()

				disk, err := computeService.Disks.Get(project, zone, volName).Do()
				Expect(err).To(BeNil(), "Could not get disk before modification: %v", err)
				if err != nil {
					return
				}
				Expect(disk.Type).To(ContainSubstring(hdbDiskType))
				initialIOPS := disk.ProvisionedIops
				initialThroughput := disk.ProvisionedThroughput
				initialType := disk.Type

				err = testContext.Client.ControllerModifyVolume(volume.VolumeId, testCase.parameters)
				Expect(err).NotTo(BeNil(), "Expected GCE to reject out-of-range performance parameters")
				if err == nil {
					return
				}
				Expect(strings.ToLower(err.Error())).To(ContainSubstring(testCase.errorText))

				disk, err = computeService.Disks.Get(project, zone, volName).Do()
				Expect(err).To(BeNil(), "Could not get disk after rejected modification: %v", err)
				if err != nil {
					return
				}
				Expect(disk.Type).To(Equal(initialType), "Disk type changed after rejected modification")
				Expect(disk.ProvisionedIops).To(Equal(initialIOPS), "IOPS changed after rejected modification")
				Expect(disk.ProvisionedThroughput).To(Equal(initialThroughput), "Throughput changed after rejected modification")
			}()
		}
	})
})

func TestDiskConversionE2E(t *testing.T) {
	ctx, env, restoreDriver := testutils.SetupDiskConversionE2E(t)
	t.Cleanup(func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		defer cancel()
		if err := restoreDriver(restoreCtx); err != nil {
			t.Errorf("restore controller deployment: %v", err)
		}
	})

	t.Run("convert detached disk", func(t *testing.T) {
		fixture := testutils.NewDiskConversionFixture(t, env)
		fixture.RequestConversion(t, ctx, 3000, 150)
		if err := fixture.WaitForDiskType(ctx, "hyperdisk-balanced", 2*time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := fixture.WaitForConversionComplete(ctx, "hyperdisk-balanced", 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("queue conversion until workload detaches", func(t *testing.T) {
		fixture := testutils.NewDiskConversionFixture(t, env)
		fixture.StartPod(t, ctx)
		fixture.RequestConversion(t, ctx, 3000, 150)
		if err := fixture.WaitForPVAnnotation(ctx, "pdcsi.gke.io/disk-type-conversion-operation", "Pending", 10*time.Minute); err != nil {
			t.Fatalf("conversion was not queued while the disk was attached: %v", err)
		}
		fixture.StopPod(t, ctx)
		if err := fixture.WaitForDiskType(ctx, "hyperdisk-balanced", 2*time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := fixture.WaitForConversionComplete(ctx, "hyperdisk-balanced", 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("block attachment during conversion", func(t *testing.T) {
		fixture := testutils.NewDiskConversionFixture(t, env)
		fixture.RequestConversion(t, ctx, 3000, 150)
		operation, err := fixture.WaitForActiveOperation(ctx, 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		fixture.CreatePod(t, ctx)
		if err := fixture.WaitForPodBlocked(ctx, operation, 2*time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := fixture.WaitForPodAndDisk(ctx, "hyperdisk-balanced", 2*time.Hour); err != nil {
			t.Fatalf("disk did not become attachable after conversion operation %s: %v", operation, err)
		}
		if err := fixture.WaitForConversionComplete(ctx, "hyperdisk-balanced", 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("resume conversion after controller restart", func(t *testing.T) {
		fixture := testutils.NewDiskConversionFixture(t, env)
		fixture.RequestConversion(t, ctx, 3000, 150)
		operation, err := fixture.WaitForActiveOperation(ctx, 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := testutils.RestartDiskConversionController(ctx, env); err != nil {
			t.Fatalf("restart controller while conversion %s is active: %v", operation, err)
		}
		if err := fixture.VerifyConversionIntent(ctx, operation); err != nil {
			t.Fatal(err)
		}
		if err := fixture.WaitForDiskType(ctx, "hyperdisk-balanced", 2*time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := fixture.WaitForConversionComplete(ctx, "hyperdisk-balanced", 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("retry conversion after instant snapshot is removed", func(t *testing.T) {
		fixture := testutils.NewDiskConversionFixture(t, env)
		if err := fixture.CreateInstantSnapshot(ctx); err != nil {
			t.Fatalf("create instant snapshot: %v", err)
		}
		fixture.RequestConversion(t, ctx, 3000, 150)
		if err := fixture.WaitForSnapshotRetry(ctx, 15*time.Minute); err != nil {
			t.Fatalf("conversion did not report a retriable snapshot conflict: %v", err)
		}
		fixture.DeleteInstantSnapshot(t, ctx)
		if err := fixture.WaitForDiskType(ctx, "hyperdisk-balanced", 2*time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := fixture.WaitForConversionComplete(ctx, "hyperdisk-balanced", 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cancel queued conversion when VAC is cleared", func(t *testing.T) {
		fixture := testutils.NewDiskConversionFixture(t, env)
		fixture.StartPod(t, ctx)
		fixture.RequestConversion(t, ctx, 3000, 150)
		if err := fixture.WaitForPVAnnotation(ctx, "pdcsi.gke.io/disk-type-conversion-operation", "Pending", 10*time.Minute); err != nil {
			t.Fatalf("conversion was not queued while the disk was attached: %v", err)
		}
		fixture.ClearVolumeAttributesClass(t, ctx)
		if err := fixture.WaitForConversionCancelled(ctx, 5*time.Minute); err != nil {
			t.Fatalf("pending conversion was not cancelled after clearing the PVC VAC: %v", err)
		}
		fixture.StopPod(t, ctx)
		if err := fixture.WaitForDiskType(ctx, "pd-balanced", 2*time.Minute); err != nil {
			t.Fatalf("disk type changed after the queued conversion was cancelled: %v", err)
		}
	})
}
