from fastapi import Depends, Header, HTTPException

from app.core.config import settings

PERMISSION_READ = "read"
PERMISSION_WRITE = "write"
PERMISSION_OPS = "ops"


def require_permission(permission: str):
    async def dependency(x_auth_user: str | None = Header(default=None)):
        if not settings.auth_enabled:
            return {"sub": "anonymous", "permissions": [permission]}
        if getattr(settings, "auth_trusted_header_enabled", False) and x_auth_user:
            return {"sub": x_auth_user, "permissions": [permission], "mode": "trusted_header"}
        raise HTTPException(403, "auth enabled but no OIDC verifier is implemented; configure a trusted reverse-proxy header mode or disable AUTH_ENABLED")
    return Depends(dependency)
