"""Persistent runtime state for the Yarbo scheduler.

Lives in a single Store keyed by config entry id. The schedule SPECS
(what the user configured) live in ``config_entry.options``; this file
owns RUNTIME state — last_run timestamps, skip flags, and the global
+ per-schedule pause flags. Splitting them keeps options reloadable
without losing run history.

Schema (Store v1):

    {
        "device_states": {
            "<sn>": {
                "global_enabled": true,
                "schedules": {
                    "<schedule_id>": {
                        "last_run_ts": 1730000000.0,  // unix seconds
                        "skip_next": false,
                        "enabled": true,
                    },
                    ...
                },
            },
            ...
        },
    }

Reads are cheap (in-memory). Writes go through ``async_save()``, which
HA's Store implements as an atomic write to ``.storage/yarbo.scheduler.<entry_id>``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

_STORE_VERSION = 1
_STORE_KEY_FMT = "yarbo.scheduler.{entry_id}"


class ScheduleStateStore:
    """Thin wrapper over ``homeassistant.helpers.storage.Store``.

    Every accessor takes ``sn`` (device serial) so multi-device
    accounts share one Store but state is partitioned cleanly.
    """

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._store: Store = Store(
            hass,
            _STORE_VERSION,
            _STORE_KEY_FMT.format(entry_id=entry_id),
        )
        self._data: dict[str, Any] = {}
        self._loaded = False

    async def async_load(self) -> None:
        """Load from disk. Must be called before any accessor."""
        loaded = await self._store.async_load()
        self._data = loaded or {}
        self._data.setdefault("device_states", {})
        self._loaded = True
        _LOGGER.debug(
            "scheduler state loaded: %d device(s)",
            len(self._data["device_states"]),
        )

    async def async_save(self) -> None:
        """Atomic write of the full state."""
        if not self._loaded:
            return
        await self._store.async_save(self._data)

    # ---- Per-device global pause ---------------------------------------

    def get_global_enabled(self, sn: str) -> bool:
        """Default True — schedules run unless the user paused them."""
        dev = self._data.get("device_states", {}).get(sn) or {}
        return bool(dev.get("global_enabled", True))

    def set_global_enabled(self, sn: str, enabled: bool) -> None:
        dev = self._data.setdefault("device_states", {}).setdefault(sn, {})
        dev["global_enabled"] = bool(enabled)

    # ---- Per-schedule state --------------------------------------------

    def get_schedule_state(self, sn: str, schedule_id: str) -> dict[str, Any]:
        """Return the per-schedule state dict (a fresh shallow copy).

        Always includes default values so callers don't need to
        ``.get(...)`` repeatedly.
        """
        sched = (
            self._data.get("device_states", {})
            .get(sn, {})
            .get("schedules", {})
            .get(schedule_id, {})
        )
        return {
            "last_run_ts": sched.get("last_run_ts"),
            "skip_next": bool(sched.get("skip_next", False)),
            "enabled": bool(sched.get("enabled", True)),
            # 0..100, where 0 = "fresh start" and N>0 = "resume from N%".
            # Set by the coordinator when a plan ends without success;
            # cleared on the next successful Completed transition.
            "resume_percent": int(sched.get("resume_percent", 0) or 0),
            # post-hold tracking. was_in_weather_hold is the previous
            # tick's weather-gate result; transitions True→False arm
            # post_hold (when post_hold_run is enabled in the spec).
            # post_hold_armed is the "permission slip" the evaluator
            # consumes to bypass cooldown + weather + snow gates.
            "was_in_weather_hold": bool(
                sched.get("was_in_weather_hold", False)
            ),
            "post_hold_armed": bool(sched.get("post_hold_armed", False)),
        }

    def get_last_run(
        self, sn: str, schedule_id: str
    ) -> datetime | None:
        ts = (
            self._data.get("device_states", {})
            .get(sn, {})
            .get("schedules", {})
            .get(schedule_id, {})
            .get("last_run_ts")
        )
        if ts is None:
            return None
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except (TypeError, ValueError):
            return None

    def record_run(
        self,
        sn: str,
        schedule_id: str,
        when: datetime,
    ) -> None:
        """Stamp last_run + clear pending skip + clear resume_percent.

        Called only on a SUCCESSFUL Completed transition, so clearing
        resume_percent here means the next run starts fresh from the
        beginning of the plan.
        """
        sched = self._sched(sn, schedule_id)
        sched["last_run_ts"] = when.timestamp()
        sched["skip_next"] = False
        sched["resume_percent"] = 0
        sched["post_hold_armed"] = False

    def set_resume_percent(
        self,
        sn: str,
        schedule_id: str,
        percent: int,
    ) -> None:
        """Save the partial-completion percent for resume on next start.

        Clamped to [0, 99] — 100 would imply success, which goes through
        ``record_run`` instead and clears this value.
        """
        clamped = max(0, min(99, int(percent)))
        self._sched(sn, schedule_id)["resume_percent"] = clamped

    def set_skip_next(
        self,
        sn: str,
        schedule_id: str,
        skip: bool,
    ) -> None:
        self._sched(sn, schedule_id)["skip_next"] = bool(skip)

    def set_schedule_enabled(
        self,
        sn: str,
        schedule_id: str,
        enabled: bool,
    ) -> None:
        self._sched(sn, schedule_id)["enabled"] = bool(enabled)

    def set_was_in_weather_hold(
        self,
        sn: str,
        schedule_id: str,
        held: bool,
    ) -> None:
        self._sched(sn, schedule_id)["was_in_weather_hold"] = bool(held)

    def set_post_hold_armed(
        self,
        sn: str,
        schedule_id: str,
        armed: bool,
    ) -> None:
        self._sched(sn, schedule_id)["post_hold_armed"] = bool(armed)

    # ---- Bookkeeping ----------------------------------------------------

    def prune_unknown_schedules(
        self,
        sn: str,
        known_ids: set[str],
    ) -> int:
        """Drop state for schedules the user has deleted.

        Returns the number of entries removed. Called after the options
        flow saves so the Store doesn't accumulate orphans across the
        lifetime of the integration.
        """
        scheds = (
            self._data.get("device_states", {})
            .get(sn, {})
            .get("schedules", {})
        )
        stale = [sid for sid in scheds if sid not in known_ids]
        for sid in stale:
            scheds.pop(sid, None)
        return len(stale)

    # ---- Internal -------------------------------------------------------

    def _sched(self, sn: str, schedule_id: str) -> dict[str, Any]:
        return (
            self._data.setdefault("device_states", {})
            .setdefault(sn, {})
            .setdefault("schedules", {})
            .setdefault(schedule_id, {})
        )
