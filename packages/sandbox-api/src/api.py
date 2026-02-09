"""
Sandbox API Service.

Manages Kubernetes sandbox pods. Replaces Modal's web_api.py endpoints.
Runs as a standalone FastAPI service deployed to Kubernetes.

Endpoints:
  POST /api/create-sandbox     - Create a new sandbox pod
  POST /api/warm-sandbox       - Pre-warm a sandbox
  GET  /api/health             - Health check
  POST /api/snapshot-sandbox   - Take a VolumeSnapshot of a sandbox
  POST /api/restore-sandbox    - Restore a sandbox from a VolumeSnapshot
  DELETE /api/sandbox/{id}     - Terminate a sandbox pod
"""

import logging
import os
import time

from fastapi import FastAPI, Header, HTTPException

from .auth.github_app import generate_installation_token
from .auth.internal import AuthConfigurationError, verify_internal_token
from .k8s_manager import K8sSandboxManager, SandboxConfig

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
log = logging.getLogger("sandbox_api")

app = FastAPI(title="Open-Inspect Sandbox API", version="1.0.0")

# Lazy-initialized K8s manager
_manager: K8sSandboxManager | None = None


def get_manager() -> K8sSandboxManager:
    global _manager
    if _manager is None:
        _manager = K8sSandboxManager()
    return _manager


def require_auth(authorization: str | None) -> None:
    """Verify authentication via HMAC token."""
    try:
        if not verify_internal_token(authorization):
            raise HTTPException(status_code=401, detail="Unauthorized")
    except AuthConfigurationError as e:
        raise HTTPException(status_code=503, detail=f"Auth not configured: {e}")


def _get_github_app_token() -> str | None:
    """Generate a GitHub App installation token if credentials are configured."""
    try:
        app_id = os.environ.get("GITHUB_APP_ID")
        private_key = os.environ.get("GITHUB_APP_PRIVATE_KEY")
        installation_id = os.environ.get("GITHUB_APP_INSTALLATION_ID")

        if app_id and private_key and installation_id:
            return generate_installation_token(
                app_id=app_id,
                private_key=private_key,
                installation_id=installation_id,
            )
    except Exception as e:
        log.warning("github.token_error: %s", e)

    return None


@app.get("/api/health")
def api_health() -> dict:
    """Health check endpoint. No authentication required."""
    return {"success": True, "data": {"status": "healthy", "service": "open-inspect-sandbox-api"}}


@app.post("/api/create-sandbox")
async def api_create_sandbox(
    request: dict,
    authorization: str | None = Header(None),
    x_trace_id: str | None = Header(None),
    x_request_id: str | None = Header(None),
    x_session_id: str | None = Header(None),
    x_sandbox_id: str | None = Header(None),
) -> dict:
    """
    Create a new sandbox pod in Kubernetes.

    POST body:
    {
        "session_id": "...",
        "sandbox_id": "...",
        "repo_owner": "...",
        "repo_name": "...",
        "control_plane_url": "...",
        "sandbox_auth_token": "...",
        "provider": "anthropic",
        "model": "claude-sonnet-4-5",
        "git_user_name": "...",
        "git_user_email": "...",
        "user_env_vars": {}
    }
    """
    start_time = time.time()
    outcome = "success"

    require_auth(authorization)

    try:
        manager = get_manager()
        github_app_token = _get_github_app_token()

        # Build session config dict
        session_config = {
            "session_id": request.get("session_id"),
            "repo_owner": request.get("repo_owner"),
            "repo_name": request.get("repo_name"),
            "opencode_session_id": request.get("opencode_session_id"),
            "provider": request.get("provider", "anthropic"),
            "model": request.get("model", "claude-sonnet-4-5"),
        }

        git_user_name = request.get("git_user_name")
        git_user_email = request.get("git_user_email")
        if git_user_name and git_user_email:
            session_config["git_user"] = {
                "name": git_user_name,
                "email": git_user_email,
            }

        config = SandboxConfig(
            repo_owner=request.get("repo_owner"),
            repo_name=request.get("repo_name"),
            sandbox_id=request.get("sandbox_id"),
            session_config=session_config,
            control_plane_url=request.get("control_plane_url", ""),
            sandbox_auth_token=request.get("sandbox_auth_token", ""),
            github_app_token=github_app_token,
            user_env_vars=request.get("user_env_vars") or None,
        )

        handle = manager.create_sandbox(config)

        return {
            "success": True,
            "data": {
                "sandbox_id": handle.sandbox_id,
                "pod_name": handle.pod_name,
                "status": handle.status,
                "created_at": handle.created_at,
            },
        }
    except Exception as e:
        outcome = "error"
        log.error("api.error endpoint=create_sandbox error=%s", e)
        return {"success": False, "error": str(e)}
    finally:
        duration_ms = int((time.time() - start_time) * 1000)
        log.info(
            "api.request method=POST path=/api/create-sandbox status=200 duration_ms=%d outcome=%s trace_id=%s",
            duration_ms, outcome, x_trace_id,
        )


@app.post("/api/warm-sandbox")
async def api_warm_sandbox(
    request: dict,
    authorization: str | None = Header(None),
    x_trace_id: str | None = Header(None),
) -> dict:
    """
    Pre-warm a sandbox pod for a repository.

    POST body:
    {
        "repo_owner": "...",
        "repo_name": "...",
        "control_plane_url": "..."
    }
    """
    start_time = time.time()
    outcome = "success"

    require_auth(authorization)

    try:
        manager = get_manager()
        github_app_token = _get_github_app_token()

        config = SandboxConfig(
            repo_owner=request.get("repo_owner"),
            repo_name=request.get("repo_name"),
            control_plane_url=request.get("control_plane_url", ""),
            github_app_token=github_app_token,
        )

        handle = manager.create_sandbox(config)

        return {
            "success": True,
            "data": {
                "sandbox_id": handle.sandbox_id,
                "status": handle.status,
            },
        }
    except Exception as e:
        outcome = "error"
        log.error("api.error endpoint=warm_sandbox error=%s", e)
        return {"success": False, "error": str(e)}
    finally:
        duration_ms = int((time.time() - start_time) * 1000)
        log.info(
            "api.request method=POST path=/api/warm-sandbox duration_ms=%d outcome=%s",
            duration_ms, outcome,
        )


@app.post("/api/snapshot-sandbox")
async def api_snapshot_sandbox(
    request: dict,
    authorization: str | None = Header(None),
    x_trace_id: str | None = Header(None),
) -> dict:
    """
    Take a VolumeSnapshot of a running sandbox's workspace.

    POST body:
    {
        "sandbox_id": "...",
        "session_id": "...",
        "reason": "execution_complete"
    }
    """
    require_auth(authorization)

    sandbox_id = request.get("sandbox_id")
    if not sandbox_id:
        raise HTTPException(status_code=400, detail="sandbox_id is required")

    try:
        manager = get_manager()
        handle = manager.get_sandbox(sandbox_id)
        if not handle:
            raise HTTPException(status_code=404, detail=f"Sandbox not found: {sandbox_id}")

        if not handle.pvc_name:
            raise HTTPException(status_code=400, detail="Sandbox has no workspace PVC")

        snapshot_name = manager.take_snapshot(sandbox_id, handle.pvc_name)
        if not snapshot_name:
            return {"success": False, "error": "Snapshot creation failed"}

        return {
            "success": True,
            "data": {
                "image_id": snapshot_name,  # Keep field name compatible with control plane
                "sandbox_id": sandbox_id,
                "session_id": request.get("session_id"),
                "reason": request.get("reason", "manual"),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("api.error endpoint=snapshot_sandbox error=%s", e)
        return {"success": False, "error": str(e)}


@app.post("/api/restore-sandbox")
async def api_restore_sandbox(
    request: dict,
    authorization: str | None = Header(None),
    x_trace_id: str | None = Header(None),
) -> dict:
    """
    Restore a sandbox from a VolumeSnapshot.

    POST body:
    {
        "snapshot_image_id": "...",     (VolumeSnapshot name)
        "session_config": {...},
        "sandbox_id": "...",
        "control_plane_url": "...",
        "sandbox_auth_token": "...",
        "user_env_vars": {},
        "timeout_seconds": 7200
    }
    """
    require_auth(authorization)

    snapshot_name = request.get("snapshot_image_id")
    if not snapshot_name:
        raise HTTPException(status_code=400, detail="snapshot_image_id is required")

    try:
        manager = get_manager()
        github_app_token = _get_github_app_token()

        session_config = request.get("session_config", {})
        config = SandboxConfig(
            repo_owner=session_config.get("repo_owner", ""),
            repo_name=session_config.get("repo_name", ""),
            sandbox_id=request.get("sandbox_id"),
            session_config=session_config,
            control_plane_url=request.get("control_plane_url", ""),
            sandbox_auth_token=request.get("sandbox_auth_token", ""),
            github_app_token=github_app_token,
            user_env_vars=request.get("user_env_vars") or None,
            timeout_seconds=int(request.get("timeout_seconds", 7200)),
            snapshot_pvc_name=snapshot_name,
        )

        handle = manager.restore_from_snapshot(snapshot_name, config)

        return {
            "success": True,
            "data": {
                "sandbox_id": handle.sandbox_id,
                "pod_name": handle.pod_name,
                "status": handle.status,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("api.error endpoint=restore_sandbox error=%s", e)
        return {"success": False, "error": str(e)}


@app.delete("/api/sandbox/{sandbox_id}")
async def api_terminate_sandbox(
    sandbox_id: str,
    authorization: str | None = Header(None),
) -> dict:
    """Terminate a sandbox pod."""
    require_auth(authorization)

    manager = get_manager()
    success = manager.terminate_sandbox(sandbox_id)

    if not success:
        raise HTTPException(status_code=404, detail=f"Sandbox not found: {sandbox_id}")

    return {"success": True, "data": {"sandbox_id": sandbox_id, "status": "terminated"}}
