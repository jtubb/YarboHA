"""Pure-logic zone-rule core.

A zone rule auto-enables a set of no-go zones when an external
precipitation-rate sensor accumulates above a threshold, then auto-
releases the zones after a rolling timer (extended on each subsequent
threshold crossing).

Use case: "Rain - Frontyard" zones get muddy after rain. Rule says
"after 0.5 inches of rain accumulated, lock these zones for 48 hours
past the last meaningful rain."

This module is intentionally HA-free at import level so the decision
logic can be unit-tested without HA installed. Sampling the rate
sensor and calling ``yarbo.set_nogozone_enabled`` happens in the
coordinator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal, TypedDict


TriggerType = Literal["precipitation", "presence"]


class ZoneRuleSpec(TypedDict, total=False):
    """User-configured zone rule, persisted in config_entry.options."""

    id: str  # uuid hex; never reused
    name: str  # display
    device_sn: str
    zone_ids: list[int]
    trigger_type: TriggerType   # how the rule decides to engage
    # Precipitation trigger fields:
    rate_entity: str            # numeric sensor returning current rate (in/h or mm/h)
    event_threshold: float      # accumulator value that triggers enable
    dry_reset_hours: float      # accumulator zeros after this much dry time when not engaged
    # Presence trigger fields:
    presence_entities: list[str]  # entities considered "home" engages the rule
    # Shared:
    duration_hours: float       # rolling release timer


# State as it lives in the runtime store. Carries the accumulator and
# bookkeeping the decider mutates each tick.
class ZoneRuleState(TypedDict, total=False):
    accumulator: float
    last_sample_at: float | None    # unix seconds; None on first sample
    last_precip_at: float | None    # last tick where rate > 0
    expires_at: float | None        # unix seconds; None when not engaged
    auto_owned_zones: list[int]     # zones we currently have enabled
    last_owned_state: dict[str, bool]  # what we set, for manual-override detection
    enabled: bool                   # per-rule pause switch (default True)


def state_with_defaults(state: ZoneRuleState | None) -> ZoneRuleState:
    s = state or {}
    return {
        "accumulator": float(s.get("accumulator", 0.0) or 0.0),
        "last_sample_at": s.get("last_sample_at"),
        "last_precip_at": s.get("last_precip_at"),
        "expires_at": s.get("expires_at"),
        "auto_owned_zones": list(s.get("auto_owned_zones", []) or []),
        "last_owned_state": dict(s.get("last_owned_state", {}) or {}),
        "enabled": bool(s.get("enabled", True)),
    }


def spec_with_defaults(spec: ZoneRuleSpec) -> ZoneRuleSpec:
    raw_trigger = spec.get("trigger_type") or "precipitation"
    trigger: TriggerType = (
        "presence" if raw_trigger == "presence" else "precipitation"
    )
    return {
        "id": spec.get("id", ""),
        "name": spec.get("name", "") or "",
        "device_sn": spec.get("device_sn", ""),
        "zone_ids": list(spec.get("zone_ids", []) or []),
        "trigger_type": trigger,
        "rate_entity": spec.get("rate_entity", "") or "",
        "event_threshold": float(spec.get("event_threshold", 0.5) or 0.5),
        "duration_hours": float(spec.get("duration_hours", 48.0) or 48.0),
        "dry_reset_hours": float(spec.get("dry_reset_hours", 6.0) or 6.0),
        "presence_entities": list(spec.get("presence_entities", []) or []),
    }


# Status reported on the rule's status sensor. Order roughly chronological.
RuleStatus = Literal[
    "paused",     # global or per-rule pause is on
    "idle",       # no trigger, no engagement
    "awaiting",   # precipitation: accumulating but below threshold
    "engaged",    # zones enabled; expires_at counting down
    "released",   # transient: just released this tick (the next tick reports idle)
]


@dataclass
class TickInputs:
    """Everything the per-tick decider needs.

    Kept HA-free: the coordinator pre-resolves the rate from the
    configured rate_entity and the live owned-zone enable flags from
    the map data.
    """

    spec: ZoneRuleSpec
    state: ZoneRuleState
    now_ts: float                    # unix seconds
    rate: float                      # current value of rate_entity, 0 if unavailable
    rate_available: bool             # False = sensor missing/unavailable; we hold rate at 0 and warn
    # Live enable flag of each owned zone, keyed by zone_id (str).
    # Used to detect manual override — when we see a zone whose live
    # enable differs from what we last set, we drop our claim.
    live_zone_enable: dict[str, bool]
    # Presence trigger: True if any of the rule's presence_entities is
    # currently in 'home' state. Coordinator pre-resolves; ignored when
    # spec.trigger_type != "presence".
    presence_home: bool = False


@dataclass
class ZoneAction:
    enable_zones: list[int] = field(default_factory=list)
    disable_zones: list[int] = field(default_factory=list)
    fire_threshold_event: bool = False
    fire_engaged_event: bool = False
    fire_released_event: bool = False


@dataclass
class TickResult:
    """What the decider produces. Coordinator applies the action and
    persists the new_state."""

    new_state: ZoneRuleState
    action: ZoneAction
    status: RuleStatus


# ---------------------------------------------------------------------------
# Decider
# ---------------------------------------------------------------------------


# Defensive clamp on per-tick delta to handle long restart gaps. We
# prefer to under-count vs over-count: if HA was down for an hour
# during a downpour we'd massively over-accumulate, possibly triggering
# falsely. Cap at twice the typical tick (60s) = 2 minutes of rate.
MAX_DELTA_SECONDS = 120


def tick(inputs: TickInputs) -> TickResult:
    """One tick of one zone rule.

    Pure: returns the new state + an action to perform. The coordinator
    is responsible for actually toggling zones and saving state.

    Two engagement models, selected by ``spec.trigger_type``:
      - ``precipitation``: accumulator integrates rate; threshold engages;
        rolling release timer extended each tick rain is present.
      - ``presence``: engages immediately when any presence entity is
        ``home``; rolling release timer extended each tick presence is
        still home; releases ``duration_hours`` after last home sample.
    Manual override of any owned zone drops our claim on that zone.
    """
    spec = spec_with_defaults(inputs.spec)
    state = state_with_defaults(inputs.state)
    action = ZoneAction()

    # Per-rule pause beats everything.
    if not state["enabled"]:
        return TickResult(new_state=state, action=action, status="paused")

    # Manual-override: detect zones that the user has flipped while we
    # owned them. Drop our claim on those zones — manual wins.
    owned = list(state["auto_owned_zones"])
    last_owned = dict(state["last_owned_state"])
    surviving_owned: list[int] = []
    surviving_last: dict[str, bool] = {}
    for z in owned:
        zk = str(z)
        we_set = last_owned.get(zk)
        live = inputs.live_zone_enable.get(zk)
        if we_set is None:
            surviving_owned.append(z)
            if live is not None:
                surviving_last[zk] = live
        elif live is None:
            # Zone disappeared from the map.
            continue
        elif live == we_set:
            surviving_owned.append(z)
            surviving_last[zk] = we_set
        # else: manual override — drop.
    state["auto_owned_zones"] = surviving_owned
    state["last_owned_state"] = surviving_last

    if spec.get("trigger_type") == "presence":
        return _tick_presence(spec, state, action, inputs)
    return _tick_precipitation(spec, state, action, inputs)


def _tick_precipitation(
    spec: ZoneRuleSpec,
    state: ZoneRuleState,
    action: ZoneAction,
    inputs: TickInputs,
) -> TickResult:
    # Sample the rate. Integrate delta-since-last-sample. First sample
    # initializes last_sample_at without accumulating (we'd otherwise
    # back-fill from an undefined start point).
    if inputs.rate_available and inputs.rate > 0:
        if state["last_sample_at"] is not None:
            delta = inputs.now_ts - float(state["last_sample_at"])
            delta = max(0.0, min(delta, MAX_DELTA_SECONDS))
            state["accumulator"] = float(state["accumulator"]) + (
                inputs.rate * delta / 3600.0
            )
        state["last_precip_at"] = inputs.now_ts
    state["last_sample_at"] = inputs.now_ts

    threshold = float(spec["event_threshold"])
    duration_s = float(spec["duration_hours"]) * 3600.0
    dry_reset_s = float(spec["dry_reset_hours"]) * 3600.0

    # ---- ENGAGED branch ----
    if state["auto_owned_zones"]:
        # Rolling timer: extend on any tick where it's actively raining.
        # Once rain stops, the timer counts down naturally.
        if inputs.rate > 0 and inputs.rate_available:
            state["expires_at"] = inputs.now_ts + duration_s
        # Release if expired.
        if (
            state["expires_at"] is not None
            and inputs.now_ts >= float(state["expires_at"])
        ):
            action.disable_zones = list(state["auto_owned_zones"])
            action.fire_released_event = True
            state["auto_owned_zones"] = []
            state["last_owned_state"] = {}
            state["expires_at"] = None
            state["accumulator"] = 0.0
            state["last_precip_at"] = None
            return TickResult(new_state=state, action=action, status="released")
        return TickResult(new_state=state, action=action, status="engaged")

    # ---- NOT-ENGAGED branch ----

    # New event detection.
    if state["accumulator"] >= threshold and threshold > 0:
        zone_ids = list(spec["zone_ids"])
        action.enable_zones = zone_ids
        action.fire_engaged_event = True
        action.fire_threshold_event = True
        state["auto_owned_zones"] = zone_ids
        state["last_owned_state"] = {str(z): True for z in zone_ids}
        state["expires_at"] = inputs.now_ts + duration_s
        return TickResult(new_state=state, action=action, status="engaged")

    # Dry reset (only when not engaged — once engaged, the release
    # timer governs accumulator reset).
    if (
        state["accumulator"] > 0
        and state["last_precip_at"] is not None
        and (inputs.now_ts - float(state["last_precip_at"])) >= dry_reset_s
    ):
        state["accumulator"] = 0.0

    if state["accumulator"] > 0:
        return TickResult(new_state=state, action=action, status="awaiting")
    return TickResult(new_state=state, action=action, status="idle")


def _tick_presence(
    spec: ZoneRuleSpec,
    state: ZoneRuleState,
    action: ZoneAction,
    inputs: TickInputs,
) -> TickResult:
    """Engage while any presence_entity is home; roll a release timer
    after presence ends. ``duration_hours`` = grace period after the
    last 'home' sample.
    """
    duration_s = float(spec["duration_hours"]) * 3600.0
    home = bool(inputs.presence_home)

    # ---- ENGAGED branch ----
    if state["auto_owned_zones"]:
        if home:
            # Still home → keep timer rolling at full duration.
            state["expires_at"] = inputs.now_ts + duration_s
        # Release if grace period elapsed.
        if (
            state["expires_at"] is not None
            and inputs.now_ts >= float(state["expires_at"])
        ):
            action.disable_zones = list(state["auto_owned_zones"])
            action.fire_released_event = True
            state["auto_owned_zones"] = []
            state["last_owned_state"] = {}
            state["expires_at"] = None
            return TickResult(new_state=state, action=action, status="released")
        return TickResult(new_state=state, action=action, status="engaged")

    # ---- NOT-ENGAGED branch ----
    if home:
        zone_ids = list(spec["zone_ids"])
        action.enable_zones = zone_ids
        action.fire_engaged_event = True
        action.fire_threshold_event = True
        state["auto_owned_zones"] = zone_ids
        state["last_owned_state"] = {str(z): True for z in zone_ids}
        state["expires_at"] = inputs.now_ts + duration_s
        return TickResult(new_state=state, action=action, status="engaged")

    return TickResult(new_state=state, action=action, status="idle")


def status_label(status: RuleStatus) -> str:
    return {
        "paused": "Paused",
        "idle": "Idle",
        "awaiting": "Awaiting",
        "engaged": "Engaged",
        "released": "Released",
    }.get(status, status)
