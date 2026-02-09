"""
Kubernetes sandbox pod manager.

Replaces Modal's SandboxManager (packages/modal-infra/src/sandbox/manager.py).
Creates and manages sandbox pods in Kubernetes instead of Modal sandboxes.

Key differences from Modal:
- Uses K8s API to create pods instead of modal.Sandbox.create()
- Snapshots use K8s VolumeSnapshots (CSI) instead of modal.snapshot_filesystem()
- Pod lookup via K8s API instead of modal.Sandbox.from_id()
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field

from kubernetes import client, config
from kubernetes.client.rest import ApiException

log = logging.getLogger("k8s_manager")

DEFAULT_SANDBOX_TIMEOUT_SECONDS = 7200  # 2 hours

# Configuration from environment
SANDBOX_NAMESPACE = os.environ.get("SANDBOX_NAMESPACE", "open-inspect-sandboxes")
SANDBOX_IMAGE = os.environ.get("SANDBOX_IMAGE", "open-inspect-sandbox:latest")
SANDBOX_CPU_REQUEST = os.environ.get("SANDBOX_CPU_REQUEST", "500m")
SANDBOX_CPU_LIMIT = os.environ.get("SANDBOX_CPU_LIMIT", "2000m")
SANDBOX_MEMORY_REQUEST = os.environ.get("SANDBOX_MEMORY_REQUEST", "512Mi")
SANDBOX_MEMORY_LIMIT = os.environ.get("SANDBOX_MEMORY_LIMIT", "4Gi")
SANDBOX_STORAGE_SIZE = os.environ.get("SANDBOX_STORAGE_SIZE", "10Gi")
SANDBOX_STORAGE_CLASS = os.environ.get("SANDBOX_STORAGE_CLASS", "")
IMAGE_PULL_SECRET = os.environ.get("IMAGE_PULL_SECRET", "")

# LLM API keys secret name in K8s
LLM_SECRETS_NAME = os.environ.get("LLM_SECRETS_NAME", "llm-api-keys")
GITHUB_APP_SECRETS_NAME = os.environ.get("GITHUB_APP_SECRETS_NAME", "github-app")


@dataclass
class SandboxConfig:
    """Configuration for creating a sandbox pod."""

    repo_owner: str
    repo_name: str
    sandbox_id: str | None = None
    session_config: dict | None = None
    control_plane_url: str = ""
    sandbox_auth_token: str = ""
    timeout_seconds: int = DEFAULT_SANDBOX_TIMEOUT_SECONDS
    github_app_token: str | None = None
    user_env_vars: dict[str, str] | None = None
    snapshot_pvc_name: str | None = None  # PVC to restore from


@dataclass
class SandboxHandle:
    """Handle to a running sandbox pod."""

    sandbox_id: str
    pod_name: str
    namespace: str
    status: str
    created_at: float
    snapshot_pvc_name: str | None = None
    pvc_name: str | None = None  # The workspace PVC for this sandbox

    def get_logs(self) -> str:
        """Get sandbox pod logs."""
        try:
            v1 = client.CoreV1Api()
            return v1.read_namespaced_pod_log(
                name=self.pod_name,
                namespace=self.namespace,
                tail_lines=200,
            )
        except ApiException:
            return ""


class K8sSandboxManager:
    """
    Manages sandbox lifecycle using Kubernetes pods.

    Responsibilities:
    - Create sandbox pods with workspace PVCs
    - Pre-warm sandboxes proactively
    - Take volume snapshots for session persistence
    - Restore sandboxes from volume snapshots
    - Terminate sandbox pods
    """

    def __init__(self):
        # Try in-cluster config first, fall back to kubeconfig
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self._v1 = client.CoreV1Api()
        self._batch_v1 = client.BatchV1Api()

        # Ensure namespace exists
        self._ensure_namespace()

    def _ensure_namespace(self) -> None:
        """Create the sandbox namespace if it doesn't exist."""
        try:
            self._v1.read_namespace(name=SANDBOX_NAMESPACE)
        except ApiException as e:
            if e.status == 404:
                ns = client.V1Namespace(
                    metadata=client.V1ObjectMeta(
                        name=SANDBOX_NAMESPACE,
                        labels={"app": "open-inspect", "component": "sandbox"},
                    )
                )
                self._v1.create_namespace(body=ns)
                log.info("Created namespace %s", SANDBOX_NAMESPACE)
            else:
                raise

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a string for use as a K8s resource name."""
        return name.lower().replace("_", "-").replace(".", "-")[:63]

    def create_sandbox(self, cfg: SandboxConfig) -> SandboxHandle:
        """
        Create a new sandbox pod.

        Args:
            cfg: Sandbox configuration

        Returns:
            SandboxHandle with the pod reference
        """
        start_time = time.time()

        sandbox_id = cfg.sandbox_id or f"sandbox-{cfg.repo_owner}-{cfg.repo_name}-{int(time.time() * 1000)}"
        pod_name = self._sanitize_name(f"sbx-{sandbox_id}")
        pvc_name = f"{pod_name}-workspace"

        # Create workspace PVC
        self._create_workspace_pvc(pvc_name, cfg.snapshot_pvc_name)

        # Build environment variables
        env_vars = self._build_env_vars(cfg, sandbox_id)

        # Build pod spec
        pod = self._build_pod_spec(
            pod_name=pod_name,
            sandbox_id=sandbox_id,
            pvc_name=pvc_name,
            env_vars=env_vars,
            timeout_seconds=cfg.timeout_seconds,
            repo_owner=cfg.repo_owner,
            repo_name=cfg.repo_name,
            is_restored=cfg.snapshot_pvc_name is not None,
        )

        # Create the pod
        self._v1.create_namespaced_pod(namespace=SANDBOX_NAMESPACE, body=pod)

        duration_ms = int((time.time() - start_time) * 1000)
        log.info(
            "sandbox.create sandbox_id=%s pod=%s duration_ms=%d",
            sandbox_id, pod_name, duration_ms,
        )

        return SandboxHandle(
            sandbox_id=sandbox_id,
            pod_name=pod_name,
            namespace=SANDBOX_NAMESPACE,
            status="warming",
            created_at=time.time(),
            pvc_name=pvc_name,
        )

    def _create_workspace_pvc(self, pvc_name: str, snapshot_source: str | None = None) -> None:
        """Create a PVC for the sandbox workspace, optionally from a snapshot."""
        spec = client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=client.V1VolumeResourceRequirements(
                requests={"storage": SANDBOX_STORAGE_SIZE}
            ),
        )

        if SANDBOX_STORAGE_CLASS:
            spec.storage_class_name = SANDBOX_STORAGE_CLASS

        if snapshot_source:
            # Restore from a VolumeSnapshot
            spec.data_source = client.V1TypedLocalObjectReference(
                api_group="snapshot.storage.k8s.io",
                kind="VolumeSnapshot",
                name=snapshot_source,
            )

        pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                name=pvc_name,
                namespace=SANDBOX_NAMESPACE,
                labels={
                    "app": "open-inspect",
                    "component": "sandbox-workspace",
                },
            ),
            spec=spec,
        )

        self._v1.create_namespaced_persistent_volume_claim(
            namespace=SANDBOX_NAMESPACE, body=pvc
        )

    def _build_env_vars(self, cfg: SandboxConfig, sandbox_id: str) -> list[client.V1EnvVar]:
        """Build the environment variable list for the sandbox pod."""
        # User-provided env vars first (system vars override below)
        env_list: list[client.V1EnvVar] = []
        if cfg.user_env_vars:
            for key, value in cfg.user_env_vars.items():
                env_list.append(client.V1EnvVar(name=key, value=value))

        # System env vars (override user vars)
        system_vars = {
            "PYTHONUNBUFFERED": "1",
            "SANDBOX_ID": sandbox_id,
            "CONTROL_PLANE_URL": cfg.control_plane_url,
            "SANDBOX_AUTH_TOKEN": cfg.sandbox_auth_token,
            "REPO_OWNER": cfg.repo_owner,
            "REPO_NAME": cfg.repo_name,
        }

        if cfg.github_app_token:
            system_vars["GITHUB_APP_TOKEN"] = cfg.github_app_token

        if cfg.session_config:
            system_vars["SESSION_CONFIG"] = json.dumps(cfg.session_config)

        if cfg.snapshot_pvc_name:
            system_vars["RESTORED_FROM_SNAPSHOT"] = "true"

        for key, value in system_vars.items():
            env_list.append(client.V1EnvVar(name=key, value=value))

        return env_list

    def _build_pod_spec(
        self,
        pod_name: str,
        sandbox_id: str,
        pvc_name: str,
        env_vars: list[client.V1EnvVar],
        timeout_seconds: int,
        repo_owner: str,
        repo_name: str,
        is_restored: bool = False,
    ) -> client.V1Pod:
        """Build the K8s Pod specification for a sandbox."""
        # LLM secrets from K8s Secret
        env_from = [
            client.V1EnvFromSource(
                secret_ref=client.V1SecretEnvSource(name=LLM_SECRETS_NAME, optional=True)
            )
        ]

        # Container spec
        container = client.V1Container(
            name="sandbox",
            image=SANDBOX_IMAGE,
            command=["python", "-m", "sandbox.entrypoint"],
            working_dir="/workspace",
            env=env_vars,
            env_from=env_from,
            resources=client.V1ResourceRequirements(
                requests={"cpu": SANDBOX_CPU_REQUEST, "memory": SANDBOX_MEMORY_REQUEST},
                limits={"cpu": SANDBOX_CPU_LIMIT, "memory": SANDBOX_MEMORY_LIMIT},
            ),
            volume_mounts=[
                client.V1VolumeMount(name="workspace", mount_path="/workspace"),
            ],
        )

        # Pod spec with active deadline for timeout
        labels = {
            "app": "open-inspect",
            "component": "sandbox",
            "sandbox-id": self._sanitize_name(sandbox_id),
            "repo-owner": self._sanitize_name(repo_owner),
            "repo-name": self._sanitize_name(repo_name),
        }

        image_pull_secrets = []
        if IMAGE_PULL_SECRET:
            image_pull_secrets = [client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)]

        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=pod_name,
                namespace=SANDBOX_NAMESPACE,
                labels=labels,
                annotations={
                    "open-inspect/sandbox-id": sandbox_id,
                    "open-inspect/timeout-seconds": str(timeout_seconds),
                    "open-inspect/restored": str(is_restored).lower(),
                },
            ),
            spec=client.V1PodSpec(
                containers=[container],
                volumes=[
                    client.V1Volume(
                        name="workspace",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                            claim_name=pvc_name,
                        ),
                    ),
                ],
                restart_policy="Never",
                active_deadline_seconds=timeout_seconds,
                image_pull_secrets=image_pull_secrets or None,
                # No service account token needed in sandbox
                automount_service_account_token=False,
            ),
        )

        return pod

    def get_sandbox(self, sandbox_id: str) -> SandboxHandle | None:
        """Look up a sandbox pod by sandbox ID."""
        label_selector = f"sandbox-id={self._sanitize_name(sandbox_id)}"
        try:
            pods = self._v1.list_namespaced_pod(
                namespace=SANDBOX_NAMESPACE,
                label_selector=label_selector,
            )
            if not pods.items:
                return None

            pod = pods.items[0]
            phase = pod.status.phase if pod.status else "Unknown"

            return SandboxHandle(
                sandbox_id=sandbox_id,
                pod_name=pod.metadata.name,
                namespace=SANDBOX_NAMESPACE,
                status=self._map_pod_phase(phase),
                created_at=pod.metadata.creation_timestamp.timestamp() if pod.metadata.creation_timestamp else time.time(),
            )
        except ApiException as e:
            log.warning("sandbox.lookup_error sandbox_id=%s error=%s", sandbox_id, e)
            return None

    def _map_pod_phase(self, phase: str) -> str:
        """Map K8s pod phase to sandbox status."""
        mapping = {
            "Pending": "spawning",
            "Running": "ready",
            "Succeeded": "stopped",
            "Failed": "failed",
            "Unknown": "stale",
        }
        return mapping.get(phase, "stale")

    def terminate_sandbox(self, sandbox_id: str) -> bool:
        """Terminate a sandbox pod and clean up its PVC."""
        handle = self.get_sandbox(sandbox_id)
        if not handle:
            return False

        try:
            self._v1.delete_namespaced_pod(
                name=handle.pod_name,
                namespace=SANDBOX_NAMESPACE,
                grace_period_seconds=15,
            )
            log.info("sandbox.terminated sandbox_id=%s pod=%s", sandbox_id, handle.pod_name)
            return True
        except ApiException as e:
            log.error("sandbox.terminate_error sandbox_id=%s error=%s", sandbox_id, e)
            return False

    def take_snapshot(self, sandbox_id: str, pvc_name: str) -> str | None:
        """
        Take a VolumeSnapshot of a sandbox's workspace PVC.

        Requires CSI driver with snapshot support (e.g., EBS CSI, GCE PD CSI).

        Args:
            sandbox_id: The sandbox ID for naming
            pvc_name: The PVC to snapshot

        Returns:
            VolumeSnapshot name if successful, None otherwise
        """
        try:
            from kubernetes.client import CustomObjectsApi

            custom_api = CustomObjectsApi()

            snapshot_name = f"snap-{self._sanitize_name(sandbox_id)}-{int(time.time())}"

            snapshot_body = {
                "apiVersion": "snapshot.storage.k8s.io/v1",
                "kind": "VolumeSnapshot",
                "metadata": {
                    "name": snapshot_name,
                    "namespace": SANDBOX_NAMESPACE,
                    "labels": {
                        "app": "open-inspect",
                        "component": "sandbox-snapshot",
                        "sandbox-id": self._sanitize_name(sandbox_id),
                    },
                },
                "spec": {
                    "source": {
                        "persistentVolumeClaimName": pvc_name,
                    },
                },
            }

            if SANDBOX_STORAGE_CLASS:
                snapshot_body["spec"]["volumeSnapshotClassName"] = SANDBOX_STORAGE_CLASS + "-snapshot"

            custom_api.create_namespaced_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                namespace=SANDBOX_NAMESPACE,
                plural="volumesnapshots",
                body=snapshot_body,
            )

            log.info(
                "sandbox.snapshot sandbox_id=%s snapshot=%s pvc=%s",
                sandbox_id, snapshot_name, pvc_name,
            )
            return snapshot_name

        except Exception as e:
            log.error("sandbox.snapshot_error sandbox_id=%s error=%s", sandbox_id, e)
            return None

    def restore_from_snapshot(
        self,
        snapshot_name: str,
        cfg: SandboxConfig,
    ) -> SandboxHandle:
        """
        Create a sandbox pod restored from a VolumeSnapshot.

        Args:
            snapshot_name: VolumeSnapshot to restore from
            cfg: Sandbox configuration

        Returns:
            SandboxHandle for the restored sandbox
        """
        cfg.snapshot_pvc_name = snapshot_name
        return self.create_sandbox(cfg)

    def cleanup_terminated_pods(self, max_age_seconds: float = 3600) -> int:
        """
        Clean up terminated sandbox pods older than max_age_seconds.

        Returns the number of pods cleaned up.
        """
        cleaned = 0
        now = time.time()

        try:
            pods = self._v1.list_namespaced_pod(
                namespace=SANDBOX_NAMESPACE,
                label_selector="component=sandbox",
                field_selector="status.phase!=Running,status.phase!=Pending",
            )

            for pod in pods.items:
                created = pod.metadata.creation_timestamp.timestamp() if pod.metadata.creation_timestamp else 0
                if now - created > max_age_seconds:
                    pod_name = pod.metadata.name
                    try:
                        self._v1.delete_namespaced_pod(
                            name=pod_name,
                            namespace=SANDBOX_NAMESPACE,
                            grace_period_seconds=0,
                        )
                        cleaned += 1
                    except ApiException:
                        pass

        except ApiException as e:
            log.error("sandbox.cleanup_error error=%s", e)

        if cleaned > 0:
            log.info("sandbox.cleanup cleaned=%d", cleaned)
        return cleaned
