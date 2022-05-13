module github.com/vmware-tanzu/velero-plugin-for-csi

go 1.13

require (
	github.com/backube/volsync v0.3.0
	github.com/caddyserver/caddy v1.0.3 // indirect
	github.com/docker/spdystream v0.0.0-20170912183627-bc6354cbbc29 // indirect
	github.com/drone/envsubst v1.0.3-0.20200709223903-efdb65b94e5a // indirect
	github.com/go-openapi/validate v0.19.5 // indirect
	github.com/google/go-github v17.0.0+incompatible // indirect
	github.com/hashicorp/go-hclog v1.2.0 // indirect
	github.com/hashicorp/go-plugin v1.0.1-0.20190610192547-a1bc61569a26 // indirect
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/konveyor/volume-snapshot-mover v0.0.0-20220512143335-5df9c2ff91eb
	github.com/kubernetes-csi/external-snapshotter/client/v4 v4.2.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/vladimirvivien/echo v0.0.1-alpha.6 // indirect
	github.com/vmware-tanzu/velero v1.8.1
	github.com/xlab/handysort v0.0.0-20150421192137-fb3537ed64a1 // indirect
	k8s.io/api v0.24.0
	k8s.io/apimachinery v0.24.0
	k8s.io/client-go v0.24.0
	sigs.k8s.io/controller-runtime v0.11.0
	sigs.k8s.io/kind v0.9.0 // indirect
	vbom.ml/util v0.0.0-20160121211510-db5cfe13f5cc // indirect
)

replace github.com/gogo/protobuf => github.com/gogo/protobuf v1.3.2
