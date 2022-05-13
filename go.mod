module github.com/vmware-tanzu/velero-plugin-for-csi

go 1.13

require (
	github.com/hashicorp/go-hclog v0.12.0 // indirect
	github.com/hashicorp/go-plugin v1.0.1-0.20190610192547-a1bc61569a26 // indirect
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/konveyor/volume-snapshot-mover v0.0.0-20220512143335-5df9c2ff91eb
	github.com/kubernetes-csi/external-snapshotter/client/v4 v4.2.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/vmware-tanzu/velero v1.7.0
	k8s.io/api v0.23.0
	k8s.io/apimachinery v0.23.0
	k8s.io/client-go v0.23.0
	sigs.k8s.io/controller-runtime v0.11.0
)

replace github.com/gogo/protobuf => github.com/gogo/protobuf v1.3.2
