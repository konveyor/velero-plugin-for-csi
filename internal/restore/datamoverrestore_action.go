package restore

import (
	"context"
	"fmt"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// VolumeSnapshotRestoreRestoreItemAction is a restore item action plugin to retrieve
// VolumeSnapshotBackup from backup and create VolumeSnapshotRestore
type VolumeSnapshotRestoreRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the VolumeSnapshotRestoreRestoreItemAction should be invoked
func (p *VolumeSnapshotRestoreRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Info("VolumeSnapshotRestoreRestoreItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotbackups.datamover.oadp.openshift.io"},
	}, nil
}

// Execute backs up a DataMoverBackup object with a completely filled status
func (p *VolumeSnapshotRestoreRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	p.Log.Infof("Executing VolumeSnapshotRestoreRestoreItemAction")
	p.Log.Infof("Executing on item: %v", input.Item)
	vsb := datamoverv1alpha1.VolumeSnapshotBackup{}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &vsb); err != nil {
		return &velero.RestoreItemActionExecuteOutput{}, errors.Wrapf(err, "failed to convert VSB input.Item from unstructured")
	}

	datamoverClient, err := util.GetDatamoverClient()
	if err != nil {
		return nil, err
	}

	// create DMR using VSB fields
	vsr := datamoverv1alpha1.VolumeSnapshotRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprint("vsr-" + vsb.Annotations[util.DatamoverSourcePVCName]),
			Namespace: vsb.Namespace,
		},
		Spec: datamoverv1alpha1.VolumeSnapshotRestoreSpec{
			ResticSecretRef: corev1.LocalObjectReference{
				Name: "restic-secret",
			},
			DataMoverBackupref: datamoverv1alpha1.DMBRef{
				BackedUpPVCData: datamoverv1alpha1.PVCData{
					Name: vsb.Annotations[util.DatamoverSourcePVCName],
					Size: vsb.Annotations[util.DatamoverSourcePVCSize],
				},
				ResticRepository: vsb.Annotations[util.DatamoverResticRepository],
			},
			ProtectedNamespace: vsb.Spec.ProtectedNamespace,
		},
	}

	err = datamoverClient.Create(context.Background(), &vsr)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating volumesnapshotrestore CR")
	}
	p.Log.Infof("[vsb-restore] dmr created: %s", vsr.Name)

	// block until DMR is completed for VolSync VSC handle
	volSnapshotRestoreCompleted, err := util.IsVolumeSnapshotRestoreCompleted(vsr.Namespace, vsr.Name, vsr.Spec.ProtectedNamespace, p.Log)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !volSnapshotRestoreCompleted {
		return nil, errors.New("volumeSnapshotRestore never completed")
	}

	p.Log.Infof("[vsb-restore] VSR completed completed: %s", vsr.Name)

	// returning empty output so we do not restore VSB
	return &velero.RestoreItemActionExecuteOutput{}, nil
}
