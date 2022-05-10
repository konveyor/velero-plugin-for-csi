/*
Copyright 2019, 2020 the Velero contributors.

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

package restore

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volumesnapmoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotv1beta1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

const (
	AnnBindCompleted      = "pv.kubernetes.io/bind-completed"
	AnnBoundByController  = "pv.kubernetes.io/bound-by-controller"
	AnnStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	AnnSelectedNode       = "volume.kubernetes.io/selected-node"
)

// PVCRestoreItemAction is a restore item action plugin for Velero
type PVCRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the PVCRestoreItemAction should be run while restoring PVCs.
func (p *PVCRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
		//TODO: add label selector volumeSnapshotLabel
	}, nil
}

func removePVCAnnotations(pvc *corev1api.PersistentVolumeClaim, remove []string) {
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
		return
	}
	for k := range pvc.Annotations {
		if util.Contains(remove, k) {
			delete(pvc.Annotations, k)
		}
	}
}

func resetPVCSpec(pvc *corev1api.PersistentVolumeClaim, vsName string) error {

	// Restore operation for the PVC will use the volumesnapshot as the data source.
	// So clear out the volume name, which is a ref to the PV
	pvc.Spec.VolumeName = ""
	pvc.Spec.DataSource = &corev1api.TypedLocalObjectReference{
		APIGroup: &snapshotv1beta1api.SchemeGroupVersion.Group,
		Kind:     "VolumeSnapshot",
		// use VolSync VS
		Name: vsName,
	}
	return nil
}

func setPVCStorageResourceRequest(pvc *corev1api.PersistentVolumeClaim, restoreSize resource.Quantity, log logrus.FieldLogger) {
	{
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1api.ResourceList{}
		}

		storageReq, exists := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
		if !exists || storageReq.Cmp(restoreSize) < 0 {
			pvc.Spec.Resources.Requests[corev1api.ResourceStorage] = restoreSize
			rs := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
			log.Infof("Resetting storage requests for PVC %s/%s to %s", pvc.Namespace, pvc.Name, rs.String())
		}
	}
}

// Execute modifies the PVC's spec to use the volumesnapshot object as the data source ensuring that the newly provisioned volume
// can be pre-populated with data from the volumesnapshot.
func (p *PVCRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	var pvc corev1api.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &pvc); err != nil {
		return nil, errors.WithStack(err)
	}
	p.Log.Infof("Starting PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)

	removePVCAnnotations(&pvc,
		[]string{AnnBindCompleted, AnnBoundByController, AnnStorageProvisioner, AnnSelectedNode})

	// If cross-namespace restore is configured, change the namespace
	// for PVC object to be restored
	if val, ok := input.Restore.Spec.NamespaceMapping[pvc.GetNamespace()]; ok {
		pvc.SetNamespace(val)
	}

	dmr := volumesnapmoverv1alpha1.DataMoverRestore{}
	datamoverClient, err := util.GetDatamoverClient()
	if err != nil {
		return nil, err
	}

	dataMoverRestoreName := fmt.Sprintf("dmr-" + pvc.Name)
	err = datamoverClient.Get(context.TODO(), client.ObjectKey{Namespace: pvc.Namespace, Name: dataMoverRestoreName}, &dmr)
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("failed to get datamoverrestore %s", dataMoverRestoreName))
	}
	p.Log.Infof("Got DMR for PVC restore %s", dmr.Name)

	repDestination := volsyncv1alpha1.ReplicationDestination{}
	repDestinationClient, err := util.GetReplicationDestinationClient()
	if err != nil {
		return nil, err
	}

	// TODO: change this name
	err = repDestinationClient.Get(context.TODO(), client.ObjectKey{Namespace: dmr.Spec.ProtectedNamespace, Name: "dmr-mssql-pvc-rep-dest"}, &repDestination)
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("failed to get repDest for PVC %s", "dmr-mssql-pvc-rep-dest"))
	}
	p.Log.Infof("Got RepDest for PVC restore %s", repDestination.Name)

	// use VolSync VS
	volumeSnapshotName := repDestination.Status.LatestImage.Name

	stringRestoreSize := dmr.Spec.DataMoverBackupref.BackedUpPVCData.Size
	restoreSize := resource.MustParse(stringRestoreSize)

	// It is possible that the volume provider allocated a larger capacity volume than what was requested in the backed up PVC.
	// In this scenario the volumesnapshot of the PVC will endup being larger than its requested storage size.
	// Such a PVC, on restore as-is, will be stuck attempting to use a Volumesnapshot as a data source for a PVC that
	// is not large enough.
	// To counter that, here we set the storage request on the PVC to the larger of the PVC's storage request and the size of the
	// VolumeSnapshot
	setPVCStorageResourceRequest(&pvc, restoreSize, p.Log)

	resetPVCSpec(&pvc, volumeSnapshotName)

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	p.Log.Infof("Returning from PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)

	return &velero.RestoreItemActionExecuteOutput{
		UpdatedItem: &unstructured.Unstructured{Object: pvcMap},
	}, nil
}
