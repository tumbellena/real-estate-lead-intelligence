# Admin-only endpoints. Currently just the kill switch, but its own router
# (rather than routes bolted directly onto `app` in main.py) so any future
# admin endpoint gets the same basic-auth protection automatically.
import os
import secrets

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.kill_switch import get_kill_switch_status, set_persisted_value

logger = structlog.get_logger()

router = APIRouter(prefix="/admin", tags=["admin"])
security = HTTPBasic()

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")


def require_admin(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    """
    FastAPI dependency enforcing HTTP Basic Auth. Uses secrets.compare_digest
    for both fields (not `==`) so a failed check takes the same amount of
    time regardless of how many characters matched - a plain string
    comparison leaks how close a guess was via response timing.
    """
    if not ADMIN_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin auth is not configured (ADMIN_PASSWORD unset)",
        )

    valid_username = secrets.compare_digest(credentials.username, ADMIN_USERNAME)
    valid_password = secrets.compare_digest(credentials.password, ADMIN_PASSWORD)
    if not (valid_username and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


@router.get("/kill-switch")
async def get_kill_switch(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_admin),
):
    return await get_kill_switch_status(db)


@router.post("/kill-switch")
async def toggle_kill_switch(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPBasicCredentials = Depends(security),
    _: None = Depends(require_admin),
):
    """Flips the persisted kill-switch value - enabled becomes disabled and vice versa."""
    status_before = await get_kill_switch_status(db)
    new_value = not status_before["persisted_value"]
    await set_persisted_value(db, new_value)
    status_after = await get_kill_switch_status(db)
    logger.info(
        "kill_switch_toggled",
        admin_username=credentials.username,
        persisted_value=new_value,
        automation_enabled=status_after["automation_enabled"],
    )
    return status_after
