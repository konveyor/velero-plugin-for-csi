/*
Copyright 2020 the Velero contributors.

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

package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapshotv1beta1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/typed/volumesnapshot/v1beta1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	volumesnapmoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/restic"
)

const (
	//TODO: use annotation from velero https://github.com/vmware-tanzu/velero/pull/2283
	resticPodAnnotation = "backup.velero.io/backup-volumes"
)

func GetPVForPVC(pvc *corev1api.PersistentVolumeClaim, corev1 corev1client.PersistentVolumesGetter) (*corev1api.PersistentVolume, error) {
	if pvc.Spec.VolumeName == "" {
		return nil, errors.Errorf("PVC %s/%s has no volume backing this claim", pvc.Namespace, pvc.Name)
	}
	if pvc.Status.Phase != corev1api.ClaimBound {
		// TODO: confirm if this PVC should be snapshotted if it has no PV bound
		return nil, errors.Errorf("PVC %s/%s is in phase %v and is not bound to a volume", pvc.Namespace, pvc.Name, pvc.Status.Phase)
	}
	pvName := pvc.Spec.VolumeName
	pv, err := corev1.PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get PV %s for PVC %s/%s", pvName, pvc.Namespace, pvc.Name)
	}
	return pv, nil
}

func GetPodsUsingPVC(pvcNamespace, pvcName string, corev1 corev1client.PodsGetter) ([]corev1api.Pod, error) {
	podsUsingPVC := []corev1api.Pod{}
	podList, err := corev1.Pods(pvcNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, p := range podList.Items {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				podsUsingPVC = append(podsUsingPVC, p)
			}
		}
	}

	return podsUsingPVC, nil
}

func GetPodVolumeNameForPVC(pod corev1api.Pod, pvcName string) (string, error) {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
			return v.Name, nil
		}
	}
	return "", errors.Errorf("Pod %s/%s does not use PVC %s/%s", pod.Namespace, pod.Name, pod.Namespace, pvcName)
}

func Contains(slice []string, key string) bool {
	for _, i := range slice {
		if i == key {
			return true
		}
	}
	return false
}

func IsPVCBackedUpByRestic(pvcNamespace, pvcName string, podClient corev1client.PodsGetter, defaultVolumesToRestic bool) (bool, error) {
	pods, err := GetPodsUsingPVC(pvcNamespace, pvcName, podClient)
	if err != nil {
		return false, errors.WithStack(err)
	}

	for _, p := range pods {
		resticVols := restic.GetPodVolumesUsingRestic(&p, defaultVolumesToRestic)
		if len(resticVols) > 0 {
			volName, err := GetPodVolumeNameForPVC(p, pvcName)
			if err != nil {
				return false, err
			}
			if Contains(resticVols, volName) {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetVolumeSnapshotClassForStorageClass returns a VolumeSnapshotClass for the supplied volume provisioner/ driver name.
func GetVolumeSnapshotClassForStorageClass(provisioner string, snapshotClient snapshotter.SnapshotV1beta1Interface) (*snapshotv1beta1api.VolumeSnapshotClass, error) {
	snapshotClasses, err := snapshotClient.VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error listing volumesnapshot classes")
	}
	// We pick the volumesnapshotclass that matches the CSI driver name and has a 'velero.io/csi-volumesnapshot-class'
	// label. This allows multiple VolumesnapshotClasses for the same driver with different values for the
	// other fields in the spec.
	// https://pkg.go.dev/github.com/kubernetes-csi/external-snapshotter/v2@v2.0.1/pkg/apis/volumesnapshot/v1beta1?tab=doc#VolumeSnapshotClass
	for _, sc := range snapshotClasses.Items {
		_, hasLabelSelector := sc.Labels[VolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner && hasLabelSelector {
			return &sc, nil
		}
	}
	return nil, errors.Errorf("failed to get volumesnapshotclass for provisioner %s, ensure that the desired volumesnapshot class has the %s label", provisioner, VolumeSnapshotClassSelectorLabel)
}

// Get DataMoverBackup CR with complete status fields
func GetDataMoverbackupWithCompletedStatus(datamoverbackupNS string, datamoverbackupName string, log logrus.FieldLogger) (volumesnapmoverv1alpha1.DataMoverBackup, error) {

	timeout := 5 * time.Minute
	interval := 5 * time.Second
	dmb := volumesnapmoverv1alpha1.DataMoverBackup{}

	datamoverClient, err := GetDatamoverClient()
	if err != nil {
		return dmb, err
	}

	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		err := datamoverClient.Get(context.TODO(), client.ObjectKey{Namespace: datamoverbackupNS, Name: datamoverbackupName}, &dmb)
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get datamoverbackup %s/%s", datamoverbackupNS, datamoverbackupName))
		}

		if len(dmb.Status.Phase) == 0 || dmb.Status.Phase != volumesnapmoverv1alpha1.DatamoverBackupPhaseCompleted {
			log.Infof("Waiting for datamoverbackup %s/%s to complete. Retrying in %ds", datamoverbackupNS, datamoverbackupName, interval/time.Second)
			return false, nil
		}

		return true, nil

	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of datamoverbackup %s/%s", datamoverbackupNS, datamoverbackupName)
		}
		return dmb, err
	}
	log.Infof("Return DMB from GetDataMoverbackupWithCompletedStatus: %v", dmb)
	return dmb, nil
}

// Check if datamoverbackup CR exists for a given volumesnapshotcontent
func DoesDataMoverBackupExistForVSC(volSnap *snapshotv1beta1api.VolumeSnapshotContent, log logrus.FieldLogger) (bool, error) {
	datamoverClient, err := GetDatamoverClient()
	if err != nil {
		return false, err
	}
	dmb := volumesnapmoverv1alpha1.DataMoverBackup{}

	err = datamoverClient.Get(context.TODO(), client.ObjectKey{Namespace: volSnap.Namespace, Name: fmt.Sprint("dmb-" + volSnap.Spec.VolumeSnapshotRef.Name)}, &dmb)
	if err != nil {
		return false, err
	}

	if len(dmb.Spec.VolumeSnapshotContent.Name) > 0 && dmb.Spec.VolumeSnapshotContent.Name == volSnap.Name {
		return true, nil
	}

	return false, err
}

// block until replicationDestination is completed to get VSC from object store
func IsDataMoverRestoreCompleted(datamoverNS string, dataMoverRestoreName string, protectedNS string, log logrus.FieldLogger) (bool, error) {

	timeout := 5 * time.Minute
	interval := 5 * time.Second

	dmr := volumesnapmoverv1alpha1.DataMoverRestore{}
	repDestination := volsyncv1alpha1.ReplicationDestination{}

	datamoverClient, err := GetDatamoverClient()
	if err != nil {
		return false, err
	}

	repDestinationClient, err := GetReplicationDestinationClient()
	if err != nil {
		return false, err
	}

	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		err := datamoverClient.Get(context.TODO(), client.ObjectKey{Namespace: datamoverNS, Name: dataMoverRestoreName}, &dmr)
		log.Infof("Inside IsDataMoverRestoreCompleted, Fetched DMR: %v ", dmr)
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get datamoverrestore %s", dataMoverRestoreName))
		}

		repDestinationName := fmt.Sprintf("%s-rep-dest", dmr.Name)
		err = repDestinationClient.Get(context.TODO(), client.ObjectKey{Namespace: protectedNS, Name: repDestinationName}, &repDestination)
		log.Infof("Fetched ReplicationDestination: %v ", repDestination)
		if err != nil {
			// TODO: check for err not finding RD, and then other errs
			log.Infof("Waiting for replicationdestination %s to complete. Retrying in %ds", dataMoverRestoreName, interval/time.Second)
			return false, nil
		}

		if repDestination.Status == nil || repDestination.Status.LastSyncTime == nil || repDestination.Spec.Trigger.Manual != repDestination.Status.LastManualSync {
			log.Infof("Waiting for datamoverrestore %s to complete. Retrying in %ds", dataMoverRestoreName, interval/time.Second)
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of datamoverrestore %s", dataMoverRestoreName)
		}
		return false, err
	}
	log.Infof("Return DMR from IsDataMoverRestoreCompleted as true: %v", dmr)
	return true, nil
}

func GetVolSyncSnapContent(repDest *volsyncv1alpha1.ReplicationDestination, protectedNS string, log logrus.FieldLogger) (*snapshotv1beta1api.VolumeSnapshotContent, error) {

	_, snapClient, err := GetClients()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	snapShotClient := snapClient.SnapshotV1beta1()

	volSyncVolSnapshotName := repDest.Status.LatestImage.Name

	// get volumeSnapshot created by VolSync ReplicationDestination
	vs, err := snapShotClient.VolumeSnapshots(protectedNS).Get(context.TODO(), volSyncVolSnapshotName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s", volSyncVolSnapshotName))
	}

	// use this VS to get the name of the VSC created by Volsync
	volSyncSnapContentName := vs.Status.BoundVolumeSnapshotContentName

	vsc, err := snapShotClient.VolumeSnapshotContents().Get(context.TODO(), *volSyncSnapContentName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s for volumesnapshot %s/%s", *vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name))
	}

	log.Infof("VSC from VolSync has been found")
	return vsc, nil
}

// GetVolumeSnapshotContentForVolumeSnapshot returns the volumesnapshotcontent object associated with the volumesnapshot
func GetVolumeSnapshotContentForVolumeSnapshot(volSnap *snapshotv1beta1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1beta1Interface, log logrus.FieldLogger, shouldWait bool) (*snapshotv1beta1api.VolumeSnapshotContent, error) {
	if !shouldWait {
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			// volumesnapshot hasn't been reconciled and we're not waiting for it.
			return nil, nil
		}
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *volSnap.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "error getting volume snapshot content from API")
		}
		return vsc, nil
	}

	// We'll wait 10m for the VSC to be reconciled polling every 5s
	// TODO: make this timeout configurable.
	timeout := 10 * time.Minute
	interval := 5 * time.Second
	var snapshotContent *snapshotv1beta1api.VolumeSnapshotContent

	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name))
		}

		if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
			log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volSnap.Namespace, volSnap.Name, interval/time.Second)
			return false, nil
		}

		snapshotContent, err = snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s for volumesnapshot %s/%s", *vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name))
		}

		// we need to wait for the VolumeSnaphotContent to have a snapshot handle because during restore,
		// we'll use that snapshot handle as the source for the VolumeSnapshotContent so it's statically
		// bound to the existing snapshot.
		if snapshotContent.Status == nil || snapshotContent.Status.SnapshotHandle == nil {
			log.Infof("Waiting for volumesnapshotcontents %s to have snapshot handle. Retrying in %ds", snapshotContent.Name, interval/time.Second)
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
		}
		return nil, err
	}

	return snapshotContent, nil
}

func GetDatamoverClient() (client.Client, error) {
	client2, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, err
	}
	volumesnapmoverv1alpha1.AddToScheme(client2.Scheme())

	return client2, err
}

func GetReplicationDestinationClient() (client.Client, error) {
	client3, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, err
	}
	volsyncv1alpha1.AddToScheme(client3.Scheme())

	return client3, err
}

func GetClients() (*kubernetes.Clientset, *snapshotterClientSet.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	snapshotterClient, err := snapshotterClientSet.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return client, snapshotterClient, nil
}

// IsVolumeSnapshotClassHasListerSecret returns whether a volumesnapshotclass has a snapshotlister secret
func IsVolumeSnapshotClassHasListerSecret(vc *snapshotv1beta1api.VolumeSnapshotClass) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L59-L60
	// There is no release w/ these constants exported. Using the strings for now.
	_, nameExists := vc.Annotations[PrefixedSnapshotterListSecretNameKey]
	_, nsExists := vc.Annotations[PrefixedSnapshotterListSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotContentHasDeleteSecret returns whether a volumesnapshotcontent has a deletesnapshot secret
func IsVolumeSnapshotContentHasDeleteSecret(vsc *snapshotv1beta1api.VolumeSnapshotContent) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L56-L57
	// use exported constants in the next release
	_, nameExists := vsc.Annotations[PrefixedSnapshotterSecretNameKey]
	_, nsExists := vsc.Annotations[PrefixedSnapshotterSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotHasVSCDeleteSecret returns whether a volumesnapshot should set the deletesnapshot secret
// for the static volumesnapshotcontent that is created on restore
func IsVolumeSnapshotHasVSCDeleteSecret(vs *snapshotv1beta1api.VolumeSnapshot) bool {
	_, nameExists := vs.Annotations[CSIDeleteSnapshotSecretName]
	_, nsExists := vs.Annotations[CSIDeleteSnapshotSecretNamespace]
	return nameExists && nsExists
}

// AddAnnotations adds the supplied key-values to the annotations on the object
func AddAnnotations(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Annotations == nil {
		o.Annotations = make(map[string]string)
	}
	for k, v := range vals {
		o.Annotations[k] = v
	}
}

// AddLabels adds the supplied key-values to the labels on the object
func AddLabels(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Labels == nil {
		o.Labels = make(map[string]string)
	}
	for k, v := range vals {
		o.Labels[k] = label.GetValidName(v)
	}
}

// IsVolumeSnapshotExists returns whether a specific volumesnapshot object exists.
func IsVolumeSnapshotExists(volSnap *snapshotv1beta1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1beta1Interface) bool {
	exists := false
	if volSnap != nil {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err == nil && vs != nil {
			exists = true
		}
	}
	return exists
}

func SetVolumeSnapshotContentDeletionPolicy(vscName string, csiClient snapshotter.SnapshotV1beta1Interface) error {
	pb := []byte(`{"spec":{"deletionPolicy":"Delete"}}`)
	_, err := csiClient.VolumeSnapshotContents().Patch(context.TODO(), vscName, types.MergePatchType, pb, metav1.PatchOptions{})

	return err
}

func HasBackupLabel(o *metav1.ObjectMeta, backupName string) bool {
	if o.Labels == nil || len(strings.TrimSpace(backupName)) == 0 {
		return false
	}
	return o.Labels[velerov1api.BackupNameLabel] == label.GetValidName(backupName)
}
