"""
Kopf operator: deploy c4isr apps onto Rancher downstream clusters once they are ready.

Watches provisioning.cattle.io/v1 Cluster objects. A brand-new Cluster object with
no machines attached is not ready for deployments, so we wait for status.ready == True,
then deploy Longhorn and the other c4isr controllers (FileBrowser, PVC controller)
onto the downstream cluster.

Optionally (label c4isr-argocd: "true" on the Cluster object) it also installs ArgoCD
from its Helm chart, registers the cluster's GitLab and Artifactory repositories in
ArgoCD, and applies an ApplicationSet whose git generator points at
argocd/<cluster-name>/*.yaml in the GitLab repo.

Manifests and shared ArgoCD settings come from a ConfigMap on the management cluster.
Per-cluster repository URLs and credentials come from a Secret named
argocd-repos-<cluster-name>, created by the cluster-provisioning job (Jenkins).
The downstream cluster is reached via the kubeconfig Secret that Rancher creates
in fleet-default.
"""

import asyncio
import base64
import contextlib
import itertools
import os
import signal
import subprocess
import tempfile
import threading
import time
from string import Template

import kopf
import kubernetes
import yaml
from kubernetes.client import ApiClient, ApiException, Configuration
from kubernetes.config.config_exception import ConfigException
from kubernetes.config.kube_config import KubeConfigLoader
from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.exceptions import ResourceNotFoundError, ResourceNotUniqueError
from urllib3.exceptions import HTTPError

# Rancher's own management cluster - never deploy apps there.
LOCAL_CLUSTER_NAME = "local"

# Labels set through the cluster-creation job in Jenkins (or manually).
IGNORE_LABEL_KEY = "c4isr-ignore"    # c4isr-ignore: apps  -> skip this cluster entirely
IGNORE_LABEL_VALUE = "apps"
ARGOCD_LABEL_KEY = "c4isr-argocd"    # c4isr-argocd: "true" -> also deploy the ArgoCD stack
ARGOCD_LABEL_VALUE = "true"

# ConfigMap on the management cluster holding manifests and shared ArgoCD settings.
CONFIGMAP_NAMESPACE = "c4isr-cluster-controller"
CONFIGMAP_NAME = "c4isr-cluster-controller"

# Longhorn must be deployed in longhorn-system according to the docs.
LONGHORN_NAMESPACE = "longhorn-system"
# Namespace for all other controllers.
CONTROLLERS_NAMESPACE = "elta-controllers-system"

# Base-apps keys inside the ConfigMap.
LONGHORN_MANIFEST = "elta-c4isr-longhorn.yaml"
FILEBROWSER_MANIFEST = "elta-c4isr-filebrowser.yaml"
PVC_CONTROLLER_MANIFEST = "elta-c4isr-pvc-controller.yaml"
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
HELM_TIMEOUT = os.environ.get("HELM_TIMEOUT", "10m")
HELM_SUBPROCESS_TIMEOUT_SECONDS = float(os.environ.get("HELM_SUBPROCESS_TIMEOUT_SECONDS", "660"))

# Rancher stores each downstream cluster kubeconfig here as <cluster>-kubeconfig.
KUBECONFIG_SECRET_NAMESPACE = "fleet-default"

# Watch, HTTP client, and retry settings (overridable via env vars for testing/tuning).
# Note: server_timeout MUST be an int because Kopf passes it as ?timeoutSeconds=<val>
# to kube-apiserver, which parses it with Go strconv.ParseInt (rejecting "180.0").
WATCH_SERVER_TIMEOUT = int(float(os.environ.get("WATCH_SERVER_TIMEOUT", "180")))
WATCH_CLIENT_TIMEOUT = int(float(os.environ.get("WATCH_CLIENT_TIMEOUT", "210")))
WATCH_CONNECT_TIMEOUT = int(float(os.environ.get("WATCH_CONNECT_TIMEOUT", "30")))
WATCH_INACTIVITY_TIMEOUT = float(os.environ.get("WATCH_INACTIVITY_TIMEOUT", "70"))
NETWORK_REQUEST_TIMEOUT = float(os.environ.get("NETWORK_REQUEST_TIMEOUT", "60"))
NETWORK_ERROR_BACKOFFS = tuple(
    float(x)
    for x in os.environ.get("NETWORK_ERROR_BACKOFFS", "1,2,4,8,15,30,30,30,30").split(",")
    if x.strip()
)

K8S_CONNECT_TIMEOUT = float(os.environ.get("K8S_CONNECT_TIMEOUT", "10"))
K8S_READ_TIMEOUT = float(os.environ.get("K8S_READ_TIMEOUT", "30"))
DEFAULT_API_TIMEOUT = (K8S_CONNECT_TIMEOUT, K8S_READ_TIMEOUT)

RETRY_DELAY_SECONDS = float(os.environ.get("RETRY_DELAY_SECONDS", "30"))
EXECUTOR_MAX_WORKERS = int(os.environ.get("EXECUTOR_MAX_WORKERS", "16"))
STARTUP_GRACE_SECONDS = float(os.environ.get("STARTUP_GRACE_SECONDS", "30"))
HANDLER_STUCK_SECONDS = float(
    os.environ.get("HANDLER_STUCK_SECONDS", str(int(HELM_SUBPROCESS_TIMEOUT_SECONDS + 240)))
)

# Runtime state tracked for liveness probes and in-process watchdog.
_startup_monotonic: float = time.monotonic()
_watchdog_task: asyncio.Task | None = None
_invocation_counter = itertools.count(1)
_active_handlers_lock = threading.Lock()
_active_handlers: dict[int, tuple[str, float]] = {}
WATCHDOG_INTERVAL_SECONDS = float(os.environ.get("WATCHDOG_INTERVAL_SECONDS", "30"))

# Transport/socket exceptions that can escape urllib3 when connections drop or time out.
TRANSPORT_EXCEPTIONS = (HTTPError, TimeoutError, OSError)


class TimeoutApiClient(ApiClient):
    """Kubernetes ApiClient that enforces socket timeouts on every call and cleans up pools.

    Without an explicit _request_timeout, urllib3 defaults to timeout=None (infinite wait),
    which permanently freezes handler threads on half-open/blackholed connections (including
    internal DynamicClient /version and /apis discovery requests).
    """

    def __init__(self, configuration: Configuration | None = None, timeout=DEFAULT_API_TIMEOUT, **kwargs):
        if configuration is None:
            configuration = Configuration.get_default_copy()
        # Disable hidden urllib3 retries so dead endpoints fail fast within K8S_CONNECT_TIMEOUT
        # and let Kopf manage retries explicitly via TemporaryError.
        configuration.retries = False
        super().__init__(configuration=configuration, **kwargs)
        self._default_timeout = timeout

    def call_api(self, *args, **kwargs):
        if kwargs.get("_request_timeout") is None:
            kwargs["_request_timeout"] = self._default_timeout
        return super().call_api(*args, **kwargs)

    def close(self):
        super().close()
        if hasattr(self, "rest_client") and hasattr(self.rest_client, "pool_manager"):
            self.rest_client.pool_manager.clear()


def cluster_is_ready(body, **_) -> bool:
    return body.get("status", {}).get("ready") is True


def _check_health_state() -> dict:
    """Validate that the Cluster watch stream is alive and no handler thread is wedged."""
    now = time.monotonic()
    uptime = now - _startup_monotonic

    if uptime >= STARTUP_GRACE_SECONDS:
        watcher_alive = any(
            not task.done() and task.get_name().startswith("watcher for clusters.")
            for task in asyncio.all_tasks()
        )
        if not watcher_alive:
            raise RuntimeError("Cluster watcher task is not running.")

    with _active_handlers_lock:
        active_snapshot = list(_active_handlers.values())

    for cluster_name, started_at in active_snapshot:
        elapsed = now - started_at
        if elapsed > HANDLER_STUCK_SECONDS:
            raise RuntimeError(
                f"Handler for cluster {cluster_name} has been stuck for {elapsed:.0f}s."
            )

    return {
        "uptime_seconds": round(uptime, 1),
        "active_handlers": len(active_snapshot),
    }


async def _watchdog_loop(logger):
    """Self-terminate on unrecoverable state even if K8s livenessProbe is not configured."""
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_SECONDS)
        try:
            _check_health_state()
        except Exception as e:
            logger.critical(f"Watchdog detected unhealthy operator state ({e}); terminating for pod restart.")
            os.kill(os.getpid(), signal.SIGTERM)
            return


@kopf.on.startup()
async def startup(settings: kopf.OperatorSettings, logger, **_):
    global _startup_monotonic, _watchdog_task
    _startup_monotonic = time.monotonic()

    # Load management-cluster config once; fall back to kubeconfig for local development.
    try:
        kubernetes.config.load_incluster_config()
    except ConfigException:
        kubernetes.config.load_kube_config()

    # Persist handler progress and diffbase strictly in metadata.annotations.
    # Kopf's default SmartProgressStorage also patches status.kopf on clusters/status,
    # which fails with 403 Forbidden under our least-privilege RBAC and conflicts with Rancher.
    settings.persistence.progress_storage = kopf.AnnotationsProgressStorage()
    settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage()

    # Disable dynamic CRD/namespace watch scanning: the Cluster CRD already exists at
    # startup and the operator ServiceAccount intentionally does not watch CRDs.
    settings.scanning.disabled = True

    # Bound every watch stream and HTTP request well below typical 5-minute LB/firewall
    # conntrack idle timeouts. Kopf 1.45+ also requests watch bookmarks (every ~60s) and
    # reconnects automatically after inactivity_timeout (70s) if a socket silently dies.
    settings.watching.server_timeout = WATCH_SERVER_TIMEOUT
    settings.watching.client_timeout = WATCH_CLIENT_TIMEOUT
    settings.watching.connect_timeout = WATCH_CONNECT_TIMEOUT
    settings.watching.inactivity_timeout = WATCH_INACTIVITY_TIMEOUT

    settings.networking.connect_timeout = WATCH_CONNECT_TIMEOUT
    settings.networking.request_timeout = NETWORK_REQUEST_TIMEOUT
    settings.networking.error_backoffs = NETWORK_ERROR_BACKOFFS

    settings.execution.max_workers = EXECUTOR_MAX_WORKERS

    _watchdog_task = asyncio.create_task(_watchdog_loop(logger), name="c4isr-health-watchdog")

    logger.info("c4isr cluster controller started; watching provisioning.cattle.io Clusters.")


@kopf.on.cleanup()
async def cleanup(**_):
    global _watchdog_task
    if _watchdog_task is not None:
        _watchdog_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _watchdog_task
        _watchdog_task = None


@kopf.on.probe(id="watcher_alive")
async def probe_health(**_):
    """Verify the Cluster watch task is active and no handler thread is wedged."""
    return _check_health_state()


@kopf.on.create("provisioning.cattle.io", "v1", "Cluster")
def on_cluster_created(name, body, logger, **_):
    # Purely informational: make new clusters visible in the logs immediately,
    # even while they are not ready yet (deployment happens in on_cluster_ready).
    ready = body.get("status", {}).get("ready") is True
    logger.info(
        f"Cluster {name} detected (ready={ready}); "
        "c4isr apps will be deployed once status.ready is True."
    )


@kopf.on.resume("provisioning.cattle.io", "v1", "Cluster", when=cluster_is_ready)
@kopf.on.field("provisioning.cattle.io", "v1", "Cluster", field="status.ready", when=cluster_is_ready)
def on_cluster_ready(name, meta, logger, **_):
    # Skip the main local cluster of Rancher.
    if name == LOCAL_CLUSTER_NAME:
        logger.debug("Skipping Rancher local cluster.")
        return

    labels = meta.get("labels", {})
    if labels.get(IGNORE_LABEL_KEY) == IGNORE_LABEL_VALUE:
        logger.info(
            f"c4isr apps such as Longhorn and FileBrowser will NOT be installed on cluster {name}. "
            f"This behavior is defined by the label {IGNORE_LABEL_KEY}: {IGNORE_LABEL_VALUE} "
            "attached to this cluster object."
        )
        return

    invocation_id = next(_invocation_counter)
    with _active_handlers_lock:
        _active_handlers[invocation_id] = (name, time.monotonic())

    try:
        with build_management_client() as mgmt_client:
            config = get_configmap_data(mgmt_client)
            require_keys(f"ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}", config, MANIFEST_KEYS)
            kubeconfig = get_downstream_kubeconfig(mgmt_client, name)

        with build_downstream_client(kubeconfig) as downstream, \
             tempfile.TemporaryDirectory(prefix="k8s-discovery-") as cache_dir:
            dyn_client = build_dynamic_client(
                downstream, cache_file=os.path.join(cache_dir, "discovery.json")
            )

            ensure_namespace(downstream, LONGHORN_NAMESPACE, logger)
            ensure_namespace(downstream, CONTROLLERS_NAMESPACE, logger)

            # Apply everything on every run; objects that already exist are skipped (409),
            # so a partially failed previous run gets completed here.
            longhorn_created = apply_manifests(
                dyn_client, config[LONGHORN_MANIFEST].strip(), LONGHORN_NAMESPACE, logger
            )
            controllers_created = apply_manifests(
                dyn_client, config[FILEBROWSER_MANIFEST].strip(), CONTROLLERS_NAMESPACE, logger
            ) + apply_manifests(
                dyn_client, config[PVC_CONTROLLER_MANIFEST].strip(), CONTROLLERS_NAMESPACE, logger
            )

            if longhorn_created:
                logger.info(f"c4isr Longhorn has been deployed on cluster: {name}")
            if controllers_created:
                logger.info(f"c4isr FileBrowser and PVC controllers have been deployed on cluster: {name}")
            if not longhorn_created and not controllers_created:
                logger.debug(f"All c4isr apps already present on cluster {name}; nothing to do.")

            # Optional ArgoCD stack, opted in per cluster via label.
            # Checked after base apps so Longhorn/controllers are installed immediately
            # even if the Jenkins job has not created argocd-repos-<cluster> yet.
            if labels.get(ARGOCD_LABEL_KEY) == ARGOCD_LABEL_VALUE:
                with build_management_client() as mgmt_client:
                    repos = get_argocd_repos_secret(mgmt_client, name)
                deploy_argocd_stack(name, kubeconfig, downstream, dyn_client, config, repos, logger)
            else:
                logger.debug(
                    f"Cluster {name} has no {ARGOCD_LABEL_KEY}: {ARGOCD_LABEL_VALUE} label; skipping ArgoCD."
                )
    finally:
        with _active_handlers_lock:
            _active_handlers.pop(invocation_id, None)


def deploy_argocd_stack(
    cluster_name: str,
    kubeconfig: str,
    downstream: ApiClient,
    dyn_client: DynamicClient,
    config: dict,
    repos: dict,
    logger,
):
    """Install ArgoCD via Helm, register the cluster's repositories, apply the ApplicationSet."""
    require_keys(f"ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}", config, ARGOCD_CONFIG_KEYS)

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
    appset_created = apply_manifests(dyn_client, appset_yaml, ARGOCD_NAMESPACE, logger)
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


def get_argocd_repos_secret(mgmt_client: ApiClient, cluster_name: str) -> dict:
    """Read the per-cluster ArgoCD repositories Secret created by the provisioning job.

    Retries until it exists, so ordering between the Jenkins job and cluster
    readiness does not matter.
    """
    core = kubernetes.client.CoreV1Api(mgmt_client)
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
    except TRANSPORT_EXCEPTIONS as e:
        raise kopf.TemporaryError(
            f"Transport error reading secret {ARGOCD_REPOS_SECRET_NAMESPACE}/{secret_name}: {e}",
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
        "c4isr-gitlab-repo": {
            "type": "git",
            "url": repos["gitlab-repo-url"],
            "username": repos["username"],
            "password": repos["password"],
        },
        "c4isr-artifactory-helm-repo": {
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
        except TRANSPORT_EXCEPTIONS as e:
            raise kopf.TemporaryError(
                f"Transport error creating ArgoCD repository secret {secret_name}: {e}",
                delay=RETRY_DELAY_SECONDS,
            )


def get_configmap_data(mgmt_client: ApiClient) -> dict:
    """Read the operator's ConfigMap from the management cluster."""
    core = kubernetes.client.CoreV1Api(mgmt_client)
    try:
        configmap = core.read_namespaced_config_map(name=CONFIGMAP_NAME, namespace=CONFIGMAP_NAMESPACE)
    except ApiException as e:
        raise kopf.TemporaryError(
            f"Failed to read ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}: {e.status} {e.reason}",
            delay=RETRY_DELAY_SECONDS,
        )
    except TRANSPORT_EXCEPTIONS as e:
        raise kopf.TemporaryError(
            f"Transport error reading ConfigMap {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME}: {e}",
            delay=RETRY_DELAY_SECONDS,
        )
    return configmap.data or {}


def require_keys(source: str, data: dict, keys):
    """Raise a retryable error if any of the given keys is missing or empty."""
    missing = [key for key in keys if not data.get(key)]
    if missing:
        raise kopf.TemporaryError(f"{source} is missing keys: {missing}", delay=RETRY_DELAY_SECONDS)


def get_downstream_kubeconfig(mgmt_client: ApiClient, cluster_name: str) -> str:
    """Fetch and decode the downstream cluster kubeconfig from its Rancher-managed secret.

    Retries (via kopf) if the secret does not exist yet.
    """
    core = kubernetes.client.CoreV1Api(mgmt_client)
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
    except TRANSPORT_EXCEPTIONS as e:
        raise kopf.TemporaryError(
            f"Transport error reading secret {KUBECONFIG_SECRET_NAMESPACE}/{secret_name}: {e}",
            delay=RETRY_DELAY_SECONDS,
        )

    kubeconfig_b64 = (secret.data or {}).get("value")
    if not kubeconfig_b64:
        raise kopf.TemporaryError(
            f"Secret {KUBECONFIG_SECRET_NAMESPACE}/{secret_name} has no 'value' key.",
            delay=RETRY_DELAY_SECONDS,
        )
    return base64.b64decode(kubeconfig_b64).decode("utf-8")


def build_management_client() -> TimeoutApiClient:
    """Build a timeout-bounded API client for the management cluster."""
    return TimeoutApiClient()


def build_downstream_client(kubeconfig: str) -> TimeoutApiClient:
    """Build a dedicated, timeout-bounded API client for the downstream cluster.

    Deliberately does NOT touch the process-wide default configuration,
    which belongs to the management cluster.
    """
    try:
        parsed = yaml.safe_load(kubeconfig)
        if not isinstance(parsed, dict):
            raise ConfigException("Downstream kubeconfig is empty or not a YAML mapping.")
        loader = KubeConfigLoader(config_dict=parsed)
        config = Configuration()
        loader.load_and_set(client_configuration=config)
        return TimeoutApiClient(configuration=config)
    except (ConfigException, yaml.YAMLError, KeyError, TypeError) as e:
        raise kopf.TemporaryError(f"Invalid downstream kubeconfig: {e}", delay=RETRY_DELAY_SECONDS)


def build_dynamic_client(api_client: ApiClient, cache_file: str) -> DynamicClient:
    """Build a DynamicClient for the downstream cluster with retryable discovery errors."""
    try:
        return DynamicClient(api_client, cache_file=cache_file)
    except (ApiException, *TRANSPORT_EXCEPTIONS) as e:
        raise kopf.TemporaryError(
            f"Failed to discover API resources on downstream cluster: {e}",
            delay=RETRY_DELAY_SECONDS,
        )


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
    except TRANSPORT_EXCEPTIONS as e:
        raise kopf.TemporaryError(
            f"Transport error creating namespace {namespace}: {e}",
            delay=RETRY_DELAY_SECONDS,
        )


def apply_manifests(dyn_client: DynamicClient, manifests_yaml: str, namespace: str, logger) -> int:
    """Create all objects from a multi-document YAML string on the downstream cluster.

    Objects that already exist are skipped, so this is safe to re-run.
    Returns the number of objects actually created.
    """
    created = 0
    for obj in yaml.safe_load_all(manifests_yaml):
        if not obj:  # Skip empty documents.
            continue

        kind = obj["kind"]
        obj_name = obj.get("metadata", {}).get("name", "<unnamed>")
        target_namespace = obj.get("metadata", {}).get("namespace") or namespace

        try:
            resource = dyn_client.resources.get(api_version=obj["apiVersion"], kind=kind)
            resource.create(body=obj, namespace=target_namespace)
            created += 1
            logger.debug(f"Created {kind}/{obj_name} in namespace {target_namespace}.")
        except ApiException as e:
            if e.status == 409:
                logger.debug(f"{kind}/{obj_name} already exists in {target_namespace}; skipping.")
                continue
            raise kopf.TemporaryError(
                f"Failed to create {kind}/{obj_name} in {target_namespace}: {e.status} {e.reason}",
                delay=RETRY_DELAY_SECONDS,
            )
        except (ResourceNotFoundError, ResourceNotUniqueError, *TRANSPORT_EXCEPTIONS) as e:
            raise kopf.TemporaryError(
                f"Failed to resolve/create {kind}/{obj_name} in {target_namespace}: {e}",
                delay=RETRY_DELAY_SECONDS,
            )
    return created
