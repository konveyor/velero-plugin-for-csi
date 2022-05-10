package restore

import (
	"context"
	"fmt"

	volumesnapmoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DataMoverRestoreItemAction is a restore item action plugin to retrieve
// DataMoverBackup from backup and create DataMoverRestore
type DataMoverRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the DataMoverRestoreItemAction should be invoked
func (p *DataMoverRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Info("DataMoverRestoreItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"datamoverbackups.pvc.oadp.openshift.io"},
	}, nil
}

// Execute backs up a DataMoverBackup object with a completely filled status
func (p *DataMoverRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	p.Log.Infof("Executing DataMoverRestoreItemAction")
	p.Log.Infof("Executing on item: %v", input.Item)
	dmb := volumesnapmoverv1alpha1.DataMoverBackup{}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &dmb); err != nil {
		return &velero.RestoreItemActionExecuteOutput{}, errors.Wrapf(err, "failed to convert DMB input.Item from unstructured")
	}

	datamoverClient, err := util.GetDatamoverClient()
	if err != nil {
		return nil, err
	}

	// create DMR using DMB fields
	dmr := volumesnapmoverv1alpha1.DataMoverRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprint("dmr-" + dmb.Annotations[util.DatamoverSourcePVCName]),
			Namespace: dmb.Namespace,
		},
		Spec: volumesnapmoverv1alpha1.DataMoverRestoreSpec{
			ResticSecretRef: corev1.LocalObjectReference{
				Name: "restic-secret",
			},
			DataMoverBackupref: volumesnapmoverv1alpha1.DMBRef{
				BackedUpPVCData: volumesnapmoverv1alpha1.PVCData{
					Name: dmb.Annotations[util.DatamoverSourcePVCName],
					Size: dmb.Annotations[util.DatamoverSourcePVCSize],
				},
				ResticRepository: dmb.Annotations[util.DatamoverResticRepository],
			},
			ProtectedNamespace: dmb.Spec.ProtectedNamespace,
		},
	}

	err = datamoverClient.Create(context.Background(), &dmr)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating datamoverrestore CR")
	}
	p.Log.Infof("[dmb-restore] dmr created: %s", dmr.Name)

	// block until DMR is completed for VolSync VSC handle
	dataMoverRestoreCompleted, err := util.IsDataMoverRestoreCompleted(dmr.Namespace, dmr.Name, dmr.Spec.ProtectedNamespace, p.Log)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !dataMoverRestoreCompleted {
		return nil, errors.New("datamoverRestore never completed")
	}

	p.Log.Infof("[dmb-restore] DMR completed completed: %s", dmr.Name)

	// returning empty output so we do not restore DMB
	return &velero.RestoreItemActionExecuteOutput{}, nil
}
