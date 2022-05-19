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
	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// VolumeSnapshotContentRestoreItemAction is a restore item action plugin for Velero
type VolumeSnapshotContentRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating VolumeSnapshotContentRestoreItemAction action should be invoked while restoring
// volumesnapshotcontent.snapshot.storage.k8s.io resources
func (p *VolumeSnapshotContentRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotcontent.snapshot.storage.k8s.io"},
	}, nil
}

// skip restoring VSC as we will create one using VolSync
func (p *VolumeSnapshotContentRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	p.Log.Info("Skipping VolumeSnapshotContentRestoreItemAction")

	return &velero.RestoreItemActionExecuteOutput{
		SkipRestore: true,
	}, nil
}
