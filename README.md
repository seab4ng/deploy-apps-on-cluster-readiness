# deploy-apps-on-cluster-readiness

Kopf operator for Rancher management clusters. When a downstream cluster
(`provisioning.cattle.io/v1` Cluster) reaches `status.ready: true`, the operator:

1. Deploys **Longhorn** (into `longhorn-system`) and the **FileBrowser + PVC
   controllers** (into `shawarma-controllers-system`) from manifests stored in a
   ConfigMap.
2. Optionally — if the Cluster object is labeled `humus-argocd: "true"` — installs
   **ArgoCD** from a Helm chart, registers the cluster's GitLab (git) and
   Artifactory (helm) repositories in ArgoCD, and applies an **ApplicationSet**
   whose git generator points at `argocd/<cluster-name>/*.yaml`.

Everything is idempotent: re-runs (retries, operator restarts) only create what is
missing; `helm upgrade --install` converges the ArgoCD release.

## Labels on the Cluster object

| Label | Effect |
|---|---|
| `humus-ignore: apps` | Skip this cluster entirely (nothing installed) |
| `humus-argocd: "true"` | Also deploy the ArgoCD stack |

## Configuration

**ConfigMap** `humus-cluster-controller/humus-cluster-controller` (shared, see
[deploy/configmap-example.yaml](deploy/configmap-example.yaml)):

| Key | Content |
|---|---|
| `shawarma-humus-longhorn.yaml` | Longhorn manifests |
| `shawarma-humus-filebrowser.yaml` | FileBrowser controller manifests |
| `shawarma-humus-pvc-controller.yaml` | PVC controller manifests |
| `argocd-chart-repo` | Helm repo with the argo-cd chart (`https://...` or `oci://...`) |
| `argocd-chart-version` | Chart version to install |
| `argocd-new-values.yaml` | Values passed to helm with `-f` |
| `argocd-applicationset.yaml` | ApplicationSet template (`${GITLAB_REPO_URL}`, `${REVISION}`, `${CLUSTER_NAME}`) |

**Per-cluster Secret** `argocd-repos-<cluster-name>` in the same namespace,
created by the cluster-provisioning job (see
[deploy/secret-example.yaml](deploy/secret-example.yaml)): `gitlab-repo-url`,
`artifactory-helm-url`, `username`, `password`, `git-revision`. The operator
retries until it exists, so creation order does not matter.

## Deploying the operator

```bash
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/configmap-example.yaml   # after filling it in
kubectl apply -f deploy/deployment.yaml
```

## Air-gapped environments

The image is fully self-contained (python deps + helm binary baked in at build
time); at runtime the operator only talks to the management API, the downstream
API, and the internal Artifactory. Make sure:

- `argocd-chart-repo` points at the **internal** Artifactory (reachable from the
  management cluster, where the chart is pulled).
- `argocd-new-values.yaml` **overrides the ArgoCD image registries** to the
  internal mirror — the chart defaults pull from quay.io, which downstream nodes
  cannot reach.
- The GitLab/Artifactory URLs in the per-cluster secret are reachable **from the
  downstream cluster** (ArgoCD is what uses them).

## CI / releases

Creating a tag (or a GitHub release) builds the image and pushes it to Docker Hub
as `sokushinbutsu/dappsoclusterr:<tag>` (tag `1.0.0` → image `1.0.0`, plus
`latest`). Requires two repository secrets: `DOCKERHUB_USERNAME` and
`DOCKERHUB_TOKEN`.
