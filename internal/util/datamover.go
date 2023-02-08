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
	"os"
	"strconv"
)

const VolumeSnapshotMoverEnv = "VOLUME_SNAPSHOT_MOVER"

// We expect VolumeSnapshotMoverEnv to be set once when container is started.
// When true, we will use the csi data-mover code path.
var dataMoverCase, _ = strconv.ParseBool(os.Getenv(VolumeSnapshotMoverEnv))

// DataMoverCase use getter to avoid changing bool in other packages
func DataMoverCase() bool {
	return dataMoverCase
}
