# Shared kill-switch logic, used by both the FastAPI app (app/admin.py, the
# /admin/kill-switch endpoints) and the background worker (app/worker.py,
# checked at the top of every poll loop iteration). Both read/write the
# same system_config row, so flipping it from the API takes effect on the
# worker's next loop iteration - no restart, no redeploy.
#
# There are two independent layers here, and the effective state is "on"
# only if both agree:
#
# - AUTOMATION_ENABLED (env var): a coarse, infra-level override. Set at
#   deploy time, requires a restart to change. It's what you'd reach for if
#   the database or the admin API itself were the thing misbehaving - it
#   works even if nothing else in the app does.
# - system_config row (this module): a fine-grained, runtime-level toggle.
#   Flippable in milliseconds via POST /admin/kill-switch, no deploy
#   required - what you'd reach for during an actual incident.
import os

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import SystemConfig

AUTOMATION_ENABLED_KEY = "automation_enabled"


def env_var_enabled() -> bool:
    return os.getenv("AUTOMATION_ENABLED", "true").strip().lower() == "true"


async def _get_persisted_value(db: AsyncSession) -> bool | None:
    """The system_config-persisted value, or None if no row exists yet."""
    config = await db.get(SystemConfig, AUTOMATION_ENABLED_KEY)
    if config is None:
        return None
    return config.value == "true"


async def get_kill_switch_status(db: AsyncSession) -> dict:
    """
    The full picture: what's actually governing the worker right now
    (`automation_enabled`), what's stored in system_config
    (`persisted_value` - what POST actually flips), and whether the env var
    is the reason automation is off even if persisted_value says otherwise
    (`env_var_forces_disabled`) - so an operator who flips the switch and
    sees it stay disabled can immediately tell why.
    """
    env_enabled = env_var_enabled()
    persisted = await _get_persisted_value(db)
    persisted_value = persisted if persisted is not None else env_enabled
    return {
        "automation_enabled": env_enabled and persisted_value,
        "persisted_value": persisted_value,
        "env_var_forces_disabled": not env_enabled,
    }


async def is_automation_enabled(db: AsyncSession) -> bool:
    status = await get_kill_switch_status(db)
    return status["automation_enabled"]


async def set_persisted_value(db: AsyncSession, enabled: bool) -> None:
    config = await db.get(SystemConfig, AUTOMATION_ENABLED_KEY)
    if config is None:
        db.add(SystemConfig(key=AUTOMATION_ENABLED_KEY, value=str(enabled).lower()))
    else:
        config.value = str(enabled).lower()
    await db.commit()
