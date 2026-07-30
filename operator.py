"""
Kopf operator: deploy humus apps onto Rancher downstream clusters once they are ready.

Watches provisioning.cattle.io/v1 Cluster objects. A brand-new Cluster object with
no machines attached is not ready for deployments, so we wait for status.ready == True,
then deploy Longhorn and the other humus controllers (FileBrowser, PVC controller)
onto the downstream cluster.

Optionally (label humus-argocd: "true" on the Cluster object) it also installs ArgoCD
from its Helm chart, registers the cluster's GitLab and Artifactory repositories in
ArgoCD, and applies an ApplicationSet whose git generator points at
argocd/<cluster-name>/*.yaml in the GitLab repo.

Manifests and shared ArgoCD settings come from a ConfigMap on the management cluster.
Per-cluster repository URLs and credentials come from a Secret named
argocd-repos-<cluster-name>, created by the cluster-provisioning job (Jenkins).
The downstream cluster is reached via the kubeconfig Secret that Rancher creates
in fleet-default.
"""

import base64
import subprocess
import tempfile
from string import Template

import kopf
import kubernetes
import yaml
from kubernetes.client import ApiClient, ApiException, Configuration
from kubernetes.config.kube_config import KubeConfigLoader
from kubernetes.dynamic import DynamicClient

# Rancher's own management cluster - never deploy apps there.
LOCAL_CLUSTER_NAME = "local"

# Labels set through the cluster-creation job in Jenkins (or manually).
IGNORE_LABEL_KEY = "humus-ignore"    # humus-ignore: apps  -> skip this cluster entirely
IGNORE_LABEL_VALUE = "apps"
ARGOCD_LABEL_KEY = "humus-argocd"    # humus-argocd: "true" -> also deploy the ArgoCD stack
ARGOCD_LABEL_VALUE = "true"

# ConfigMap on the management cluster holding manifests and shared ArgoCD settings.
CONFIGMAP_NAMESPACE = "humus-cluster-controller"
CONFIGMAP_NAME = "humus-cluster-controller"

# Longhorn must be deployed in longhorn-system according to the docs.
LONGHORN_NAMESPACE = "longhorn-system"
# Namespace for all other controllers.
CONTROLLERS_NAMESPACE = "shawarma-controllers-system"

# Base-apps keys inside the ConfigMap.
LONGHORN_MANIFEST = "shawarma-humus-longhorn.yaml"
FILEBROWSER_MANIFEST = "shawarma-humus-filebrowser.yaml"
PVC_CONTROLLER_MANIFEST = "shawarma-humus-pvc-controller.yaml"
MANIFEST_KEYS = (LONGHORN_MANIFEST, FILEBROWSER_MANIFEST, PVC_CONTROLLER_MANIFEST)

# ArgoCD keys inside the ConfigMap.
ARGOCD_CHART_REPO_KEY = "argocd-chart-repo"        # https://... helm repo, or oci://... registry
ARGOCD_CHART_VERSION_KEY = "argocd-chart-version"
ARGOCD_VALUES_KEY = "argocd-new-values.yaml"
ARGOCD_APPSET_KEY = "argocd-applicationset.yaml"   # template with ${GITLAB_REPO_URL} ${REVISION} ${CLUSTER_NAME}
ARGOCD_CONFIG_KEYS = (
    ARGOCD_CHART_REPO_KEY,
    ARGOCD_CHART_VERSION_KEY,
    ARGOCD_VALUES_KEY,
    ARGOCD_APPSET_KEY,
)

# Per-cluster Secret (created by the provisioning job) with repo URLs + credentials.
ARGOCD_REPOS_SECRET_PREFIX = "argocd-repos-"       # argocd-repos-<cluster-name>
ARGOCD_REPOS_SECRET_NAMESPACE = CONFIGMAP_NAMESPACE
ARGOCD_REPOS_SECRET_KEYS = (
    "gitlab-repo-url",
    "artifactory-helm-url",
    "username",
    "password",
    "git-revision",
)

ARGOCD_NAMESPACE = "argocd"
ARGOCD_RELEASE_NAME = "argocd"
ARGOCD_CHART_NAME = "argo-cd"
HELM_TIMEOUT = "10m"
HELM_SUBPROCESS_TIMEOUT_SECONDS = 660  # a bit above helm's own --timeout

# Rancher stores each downstream cluster kubeconfig here as <cluster>-kubeconfig.
KUBECONFIG_SECRET_NAMESPACE = "fleet-default"

RETRY_DELAY_SECONDS = 30


def cluster_is_ready(body, **_) -> bool:
    return body.get("status", {}).get("ready") is True


@kopf.on.startup()
def startup(logger, **_):
    # Load the management-cluster config once, instead of in every handler call.
    kubernetes.config.load_incluster_config()
    logger.info("humus cluster controller started; watching provisioning.cattle.io Clusters.")


@kopf.on.resume("provisioning.cattle.io", "v1", "Cluster", when=cluster_is_ready)
@kopf.on.field("provisioning.cattle.io", "v1", "Cluster",
               field="status.ready", when=cluster_is_ready)
def on_cluster_ready(name, meta, logger, **_):
    # Skip the main local cluster of Rancher.
    if name == LOCAL_CLUSTER_NAME:
        logger.debug("Skipping Rancher local cluster.")
        return

    labels = meta.get("labels", {})
    if labels.get(IGNORE_LABEL_KEY) == IGNORE_LABEL_VALUE:
        logger.info(
            f"humus apps such as Longhorn and FileBrowser will NOT be installed on cluster {name}. "
            f"This behavior is defined by the label {IGNORE_LABEL_KEY}: {IGNORE_LABEL_VALUE} "
            "attached to this cluster object."
        )
        return

    config = get_configmap_data()
    require_keys(f"ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}", config, MANIFEST_KEYS)

    kubeconfig = get_downstream_kubeconfig(name)
    downstream = build_downstream_client(kubeconfig)

    ensure_namespace(downstream, LONGHORN_NAMESPACE, logger)
    ensure_namespace(downstream, CONTROLLERS_NAMESPACE, logger)

    # Apply everything on every run; objects that already exist are skipped (409),
    # so a partially failed previous run gets completed here.
    longhorn_created = apply_manifests(
        downstream, config[LONGHORN_MANIFEST].strip(), LONGHORN_NAMESPACE, logger
    )
    controllers_created = apply_manifests(
        downstream, config[FILEBROWSER_MANIFEST].strip(), CONTROLLERS_NAMESPACE, logger
    ) + apply_manifests(
        downstream, config[PVC_CONTROLLER_MANIFEST].strip(), CONTROLLERS_NAMESPACE, logger
    )

    if longhorn_created:
        logger.info(f"humus Longhorn has been deployed on cluster: {name}")
    if controllers_created:
        logger.info(f"humus FileBrowser and PVC controllers have been deployed on cluster: {name}")
    if not longhorn_created and not controllers_created:
        logger.debug(f"All humus apps already present on cluster {name}; nothing to do.")

    # Optional ArgoCD stack, opted in per cluster via label.
    if labels.get(ARGOCD_LABEL_KEY) == ARGOCD_LABEL_VALUE:
        deploy_argocd_stack(name, kubeconfig, downstream, config, logger)
    else:
        logger.debug(f"Cluster {name} has no {ARGOCD_LABEL_KEY}: {ARGOCD_LABEL_VALUE} label; skipping ArgoCD.")


def deploy_argocd_stack(cluster_name: str, kubeconfig: str, downstream: ApiClient, config: dict, logger):
    """Install ArgoCD via Helm, register the cluster's repositories, apply the ApplicationSet."""
    require_keys(f"ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}", config, ARGOCD_CONFIG_KEYS)

    # Read the per-cluster repos secret first, so we fail fast (and retry)
    # before running the expensive helm install.
    repos = get_argocd_repos_secret(cluster_name)

    helm_install_argocd(
        kubeconfig,
        chart_repo=config[ARGOCD_CHART_REPO_KEY].strip(),
        chart_version=config[ARGOCD_CHART_VERSION_KEY].strip(),
        values_yaml=config[ARGOCD_VALUES_KEY],
        logger=logger,
    )

    create_argocd_repo_secrets(downstream, repos, logger)

    appset_yaml = Template(config[ARGOCD_APPSET_KEY]).safe_substitute(
        GITLAB_REPO_URL=repos["gitlab-repo-url"],
        REVISION=repos["git-revision"],
        CLUSTER_NAME=cluster_name,
    )
    appset_created = apply_manifests(downstream, appset_yaml, ARGOCD_NAMESPACE, logger)
    if appset_created:
        logger.info(f"ArgoCD ApplicationSet has been deployed on cluster: {cluster_name}")

    logger.info(f"ArgoCD stack is in place on cluster: {cluster_name}")


def helm_install_argocd(kubeconfig: str, chart_repo: str, chart_version: str, values_yaml: str, logger):
    """Install/upgrade the ArgoCD Helm release on the downstream cluster.

    'helm upgrade --install' is idempotent: re-runs are no-ops unless the chart
    version or values changed, in which case the release converges to them.
    Helm needs the kubeconfig and values as files, so they are written to
    temporary files that only live for the duration of the call.
    """
    if chart_repo.startswith("oci://"):
        chart_args = [f"{chart_repo.rstrip('/')}/{ARGOCD_CHART_NAME}"]
    else:
        chart_args = [ARGOCD_CHART_NAME, "--repo", chart_repo]

    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as kubeconfig_file, \
         tempfile.NamedTemporaryFile("w", suffix=".yaml") as values_file:
        kubeconfig_file.write(kubeconfig)
        kubeconfig_file.flush()
        values_file.write(values_yaml)
        values_file.flush()

        cmd = [
            "helm", "upgrade", "--install", ARGOCD_RELEASE_NAME, *chart_args,
            "--version", chart_version,
            "--namespace", ARGOCD_NAMESPACE, "--create-namespace",
            "-f", values_file.name,
            "--kubeconfig", kubeconfig_file.name,
            "--wait", "--timeout", HELM_TIMEOUT,
        ]
        logger.info(f"Installing/upgrading ArgoCD chart {chart_version} from {chart_repo} ...")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=HELM_SUBPROCESS_TIMEOUT_SECONDS
            )
        except subprocess.TimeoutExpired:
            raise kopf.TemporaryError(
                f"helm did not finish within {HELM_SUBPROCESS_TIMEOUT_SECONDS}s.",
                delay=RETRY_DELAY_SECONDS,
            )

    if result.returncode != 0:
        raise kopf.TemporaryError(
            f"helm install of ArgoCD failed: {result.stderr.strip()[-500:]}",
            delay=RETRY_DELAY_SECONDS,
        )
    logger.info("ArgoCD helm release installed/upgraded.")


def get_argocd_repos_secret(cluster_name: str) -> dict:
    """Read the per-cluster ArgoCD repositories Secret created by the provisioning job.

    Retries until it exists, so ordering between the Jenkins job and cluster
    readiness does not matter.
    """
    core = kubernetes.client.CoreV1Api()
    secret_name = f"{ARGOCD_REPOS_SECRET_PREFIX}{cluster_name}"
    try:
        secret = core.read_namespaced_secret(name=secret_name, namespace=ARGOCD_REPOS_SECRET_NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            raise kopf.TemporaryError(
                f"Secret {ARGOCD_REPOS_SECRET_NAMESPACE}/{secret_name} not found yet "
                "(it is created by the cluster-provisioning job).",
                delay=RETRY_DELAY_SECONDS,
            )
        raise kopf.TemporaryError(
            f"Failed to read secret {ARGOCD_REPOS_SECRET_NAMESPACE}/{secret_name}: {e.status} {e.reason}",
            delay=RETRY_DELAY_SECONDS,
        )

    data = {
        key: base64.b64decode(value).decode("utf-8").strip()
        for key, value in (secret.data or {}).items()
    }
    require_keys(f"Secret {ARGOCD_REPOS_SECRET_NAMESPACE}/{secret_name}", data, ARGOCD_REPOS_SECRET_KEYS)
    return data


def create_argocd_repo_secrets(api_client: ApiClient, repos: dict, logger):
    """Register the GitLab (git) and Artifactory (helm) repositories in ArgoCD.

    ArgoCD picks up any Secret in its namespace labeled
    argocd.argoproj.io/secret-type: repository.
    """
    core = kubernetes.client.CoreV1Api(api_client)
    repo_secrets = {
        "humus-gitlab-repo": {
            "type": "git",
            "url": repos["gitlab-repo-url"],
            "username": repos["username"],
            "password": repos["password"],
        },
        "humus-artifactory-helm-repo": {
            "type": "helm",
            "name": "artifactory",
            "url": repos["artifactory-helm-url"],
            "username": repos["username"],
            "password": repos["password"],
        },
    }

    for secret_name, string_data in repo_secrets.items():
        body = kubernetes.client.V1Secret(
            metadata=kubernetes.client.V1ObjectMeta(
                name=secret_name,
                namespace=ARGOCD_NAMESPACE,
                labels={"argocd.argoproj.io/secret-type": "repository"},
            ),
            string_data=string_data,
        )
        try:
            core.create_namespaced_secret(namespace=ARGOCD_NAMESPACE, body=body)
            logger.info(f"Created ArgoCD repository secret {secret_name}.")
        except ApiException as e:
            if e.status == 409:
                logger.debug(f"ArgoCD repository secret {secret_name} already exists; skipping.")
                continue
            raise kopf.TemporaryError(
                f"Failed to create ArgoCD repository secret {secret_name}: {e.status} {e.reason}",
                delay=RETRY_DELAY_SECONDS,
            )


def get_configmap_data() -> dict:
    """Read the operator's ConfigMap from the management cluster."""
    core = kubernetes.client.CoreV1Api()
    try:
        configmap = core.read_namespaced_config_map(name=CONFIGMAP_NAME, namespace=CONFIGMAP_NAMESPACE)
    except ApiException as e:
        raise kopf.TemporaryError(
            f"Failed to read ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}: {e.status} {e.reason}",
            delay=RETRY_DELAY_SECONDS,
        )
    return configmap.data or {}


def require_keys(source: str, data: dict, keys):
    """Raise a retryable error if any of the given keys is missing or empty."""
    missing = [key for key in keys if not data.get(key)]
    if missing:
        raise kopf.TemporaryError(f"{source} is missing keys: {missing}", delay=RETRY_DELAY_SECONDS)


def get_downstream_kubeconfig(cluster_name: str) -> str:
    """Fetch and decode the downstream cluster kubeconfig from its Rancher-managed secret.

    Retries (via kopf) if the secret does not exist yet.
    """
    core = kubernetes.client.CoreV1Api()
    secret_name = f"{cluster_name}-kubeconfig"
    try:
        secret = core.read_namespaced_secret(name=secret_name, namespace=KUBECONFIG_SECRET_NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            raise kopf.TemporaryError(
                f"Kubeconfig secret {KUBECONFIG_SECRET_NAMESPACE}/{secret_name} not found yet.",
                delay=RETRY_DELAY_SECONDS,
            )
        raise kopf.TemporaryError(
            f"Failed to read secret {KUBECONFIG_SECRET_NAMESPACE}/{secret_name}: {e.status} {e.reason}",
            delay=RETRY_DELAY_SECONDS,
        )

    kubeconfig_b64 = (secret.data or {}).get("value")
    if not kubeconfig_b64:
        raise kopf.TemporaryError(
            f"Secret {KUBECONFIG_SECRET_NAMESPACE}/{secret_name} has no 'value' key.",
            delay=RETRY_DELAY_SECONDS,
        )
    return base64.b64decode(kubeconfig_b64).decode("utf-8")


def build_downstream_client(kubeconfig: str) -> ApiClient:
    """Build a dedicated API client for the downstream cluster.

    Deliberately does NOT touch the process-wide default configuration,
    which belongs to the management cluster.
    """
    loader = KubeConfigLoader(config_dict=yaml.safe_load(kubeconfig))
    config = Configuration()
    loader.load_and_set(client_configuration=config)
    return ApiClient(configuration=config)


def ensure_namespace(api_client: ApiClient, namespace: str, logger):
    """Create a namespace on the downstream cluster if it does not exist yet."""
    core = kubernetes.client.CoreV1Api(api_client)
    body = kubernetes.client.V1Namespace(
        metadata=kubernetes.client.V1ObjectMeta(name=namespace)
    )
    try:
        core.create_namespace(body)
        logger.info(f"Created namespace {namespace}.")
    except ApiException as e:
        if e.status == 409:
            logger.debug(f"Namespace {namespace} already exists.")
            return
        raise kopf.TemporaryError(
            f"Failed to create namespace {namespace}: {e.status} {e.reason}",
            delay=RETRY_DELAY_SECONDS,
        )


def apply_manifests(api_client: ApiClient, manifests_yaml: str, namespace: str, logger) -> int:
    """Create all objects from a multi-document YAML string on the downstream cluster.

    Objects that already exist are skipped, so this is safe to re-run.
    Returns the number of objects actually created.
    """
    dyn_client = DynamicClient(api_client)
    created = 0
    for obj in yaml.safe_load_all(manifests_yaml):
        if not obj:  # Skip empty documents.
            continue

        kind = obj["kind"]
        obj_name = obj.get("metadata", {}).get("name", "<unnamed>")
        resource = dyn_client.resources.get(api_version=obj["apiVersion"], kind=kind)
        try:
            resource.create(body=obj, namespace=namespace)
            created += 1
            logger.debug(f"Created {kind}/{obj_name} in namespace {namespace}.")
        except ApiException as e:
            if e.status == 409:
                logger.debug(f"{kind}/{obj_name} already exists in {namespace}; skipping.")
                continue
            raise kopf.TemporaryError(
                f"Failed to create {kind}/{obj_name} in {namespace}: {e.status} {e.reason}",
                delay=RETRY_DELAY_SECONDS,
            )
    return created
