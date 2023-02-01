package delete

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"k8s.io/apimachinery/pkg/runtime"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// VolumeSnapshotBackupDeleteItemAction is a delete item action plugin for Velero.
type VolumeSnapshotBackupDeleteItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the VolumeSnapshotBackupDeleteItemAction should be invoked to delete volumesnapshotbackups.
func (p *VolumeSnapshotBackupDeleteItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("VolumeSnapshotBackupDeleteItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotbackups.datamover.oadp.openshift.io"},
	}, nil
}

func (p *VolumeSnapshotBackupDeleteItemAction) Execute(input *velero.DeleteItemActionExecuteInput) error {
	p.Log.Info("Starting VolumeSnapshotBackupDeleteItemAction for volumeSnapshotbackup")

	vsb := datamoverv1alpha1.VolumeSnapshotBackup{}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &vsb); err != nil {
		return errors.Wrapf(err, "failed to convert input.Item from unstructured")
	}

	// We don't want this DeleteItemAction plugin to delete Volumesnapshotbackup taken outside of Velero.
	// So skip deleting Volumesnapshotbackup objects that were not created in the process of creating
	// the Velero backup being deleted.
	if !util.HasBackupLabel(&vsb.ObjectMeta, input.Backup.Name) {
		p.Log.Info("VolumeSnapshotBackup %s/%s was not taken by backup %s, skipping deletion", vsb.Namespace, vsb.Name, input.Backup.Name)
		return nil
	}

	p.Log.Infof("Deleting Volumesnapshotbackup %s/%s", vsb.Namespace, vsb.Name)
	snapMoverClient, err := util.GetVolumeSnapshotMoverClient()
	if err != nil {
		return err
	}

	err = snapMoverClient.Delete(context.TODO(), &vsb)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
