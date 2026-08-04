"""Pure-logic scheduler core for the Yarbo integration.

This module is intentionally HA-free at the import level so it can be
unit-tested without a Home Assistant install. The HA-coupled
persistence wrapper lives in scheduler_state.py; coordinator-side
wiring lives in coordinator.py.

What the scheduler does
-----------------------
For each user-configured schedule (one plan + a set of gates), the
evaluator computes a HoldReason. The runtime tick fires the plan only
when the result is ``"eligible"``. Manual runs (from the card or the
Yarbo app) update the same ``last_run_ts`` via the coordinator so the
cooldown is honored regardless of how a plan was started.

Design notes
------------
* Every gate ALWAYS runs and reports a hold reason in priority order.
  This makes the status sensor's hold attribute deterministic and
  explainable: pause beats skip beats robot-state beats battery beats
  presence beats sleep beats weather beats cooldown.
* The evaluator is a pure function. The coordinator pre-resolves all
  HA-side state (weather entity, presence entities, sun elevation,
  battery, robot state) and passes plain values in. This keeps the
  evaluator unit-testable without any HA mocking and means the rules
  live in exactly one place.
* Slug rule MUST match the entity_id derivation done elsewhere
  (translations, entity unique_ids, the card display). See ``slugify``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Literal, TypedDict


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class ScheduleSpec(TypedDict, total=False):
    """User-configured schedule, persisted in config_entry.options.

    `id` and `device_sn` are required. Everything else has a default so
    older saved schedules don't break when we add new gates.
    """

    id: str  # uuid4 hex; never reused
    device_sn: str
    plan_name: str
    interval_days: float
    weather_entity: str  # "" = no weather gate
    weather_hold_states: list[str]
    sleep_start: str  # "HH:MM" or "HH:MM:SS"
    sleep_end: str
    use_sun_for_sleep: bool
    sun_elevation_threshold: float
    battery_min_pct: int
    presence_entities: list[str]  # any 'home' = hold
    # Notification targets are lists of HA notify service strings
    # (e.g. ["notify.mobile_app_phone", "notify.email"]). Empty list
    # = no notifications fired. Stored as lists so users can fan-out
    # to multiple devices/channels per schedule. The legacy str form
    # is coerced to [str] in spec_with_defaults for backward compat.
    pre_run_notify_target: list[str]    # fired when the run STARTS
    pre_run_notify_minutes: int          # currently ignored — kept for forward-compat with delayed-pre-run mode
    complete_notify_target: list[str]    # fired when the run ENDS (success or otherwise)
    # Hardware precondition. Case-insensitive substring match against
    # head_type sensor's value. "" = any head allowed.
    required_head_type: str
    # Snow accumulation gate. Sums weather forecast precipitation
    # (where condition is snowy/snowy-rainy) over the next
    # snow_forecast_hours. If sum < min_snow_accumulation: hold with
    # reason "snow-insufficient". 0 = gate disabled.
    min_snow_accumulation: float
    snow_forecast_hours: int
    # Rain rate gate. Hold the run while the configured numeric sensor
    # reads >= rain_rate_max. Same units as the sensor reports
    # (in/h or mm/h). "" entity or 0 max = gate disabled. Reported as
    # the same "weather" hold reason since semantically it's the same
    # kind of block.
    rain_rate_entity: str
    rain_rate_max: float
    # When True, fire one cleanup run after the weather gate transitions
    # from holding → clear. Bypasses cooldown + weather +
    # snow-insufficient. Still respects pause/skip/robot/battery/
    # presence/sleep/wrong-head/quiet-hours-stop.
    post_hold_run: bool


HoldReason = Literal[
    "eligible",
    "cooldown",
    "paused",
    "manual-hold",
    "skipped",
    "weather",
    "sleep",
    "battery",
    "presence",
    "robot-offline",
    "robot-busy",
    "wrong-head",
    "snow-insufficient",
    "unknown",
]


# Match the TS card's labels so the front end can stay dumb (just
# render whatever string the integration sends).
HOLD_LABELS: dict[HoldReason, str] = {
    "eligible": "Eligible",
    "cooldown": "Cooldown",
    "paused": "Paused",
    "manual-hold": "Held (manual)",
    "skipped": "Skip queued",
    "weather": "Weather hold",
    "sleep": "Quiet hours",
    "battery": "Battery low",
    "presence": "Presence hold",
    "robot-offline": "Robot offline",
    "robot-busy": "Robot busy",
    "wrong-head": "Wrong attachment",
    "snow-insufficient": "Awaiting snow",
    "unknown": "Unknown",
}


# Default weather states that block a run. Sized to the Met.no /
# OpenWeatherMap vocabulary common in HA. Users can override.
DEFAULT_WEATHER_HOLD_STATES: tuple[str, ...] = (
    "rainy", "pouring", "snowy", "snowy-rainy", "hail",
)


# ---------------------------------------------------------------------------
# Evaluator inputs
# ---------------------------------------------------------------------------


@dataclass
class RobotSnapshot:
    """Robot-side state the evaluator needs.

    All fields are pre-resolved by the coordinator from
    ``coordinator.data``. Keeping this a flat dataclass of plain values
    (no entity-state lookups, no SDK calls) is what lets the evaluator
    stay a pure function.
    """

    online: bool
    error_code: int  # 0 = no error
    is_busy: bool   # actively planning OR actively returning-to-charge


@dataclass
class GateInputs:
    """Everything needed to evaluate one schedule once.

    Many fields are pre-resolved from HA state by the coordinator (see
    docstring at top of module). Anything HA-specific lives here as a
    plain value, which keeps ``evaluate()`` a pure function.
    """

    # State + spec
    paused: bool  # global OR per-schedule disabled
    skipped: bool
    last_run: datetime | None
    interval_days: float

    # Weather
    weather_state: str | None  # None or "" = no weather gate
    weather_hold_states: list[str] = field(
        default_factory=lambda: list(DEFAULT_WEATHER_HOLD_STATES),
    )

    # Quiet hours + sun
    sleep_start: str = "22:00"
    sleep_end: str = "06:00"
    use_sun_for_sleep: bool = False
    sun_elevation_threshold: float = -6.0
    sun_elevation: float | None = None  # None = sun.sun unavailable

    # Battery
    battery_pct: int = 100
    battery_min_pct: int = 0

    # Presence: True if any presence_entity is currently 'home'
    presence_at_home: bool = False

    # Robot
    robot: RobotSnapshot = field(
        default_factory=lambda: RobotSnapshot(True, 0, False),
    )

    # Hardware. head_type comes straight from the head_type sensor's
    # state ("Mower Pro", "Snow Blower", etc.). required_head_type is
    # the user's case-insensitive substring requirement; "" disables.
    head_type: str | None = None
    required_head_type: str = ""

    # Snow accumulation. snow_estimate is what the coordinator computed
    # from the weather entity's forecast (sum of precipitation in
    # snowy periods over the next snow_forecast_hours). Compared
    # against min_snow_accumulation; same units as the weather entity
    # reports. None = no estimate available (entity missing/unavailable).
    snow_estimate: float | None = None
    min_snow_accumulation: float = 0.0

    # Rain rate gate. None = sensor not configured or unavailable
    # (skipped). When rain_rate_max > 0 and rain_rate >= rain_rate_max:
    # hold with reason "weather".
    rain_rate: float | None = None
    rain_rate_max: float = 0.0

    # Post-hold permission slip. When True, the evaluator bypasses
    # cooldown + weather + snow-insufficient gates so a one-time
    # cleanup run fires after a weather event clears.
    post_hold_armed: bool = False

    # Manual hold: the user pressed Pause, or ended a run by hand. Blocks
    # the scheduler for this DEVICE until Resume is pressed. Distinct from
    # ``paused``, which is the schedule/global enable toggle: manual_hold is
    # set implicitly by a user action on the robot rather than by a settings
    # change. Without it the next tick simply resumes what the user just
    # stopped, which is the behaviour this exists to prevent.
    manual_hold: bool = False

    # Time
    now: datetime = field(default_factory=datetime.now)


@dataclass
class Evaluation:
    """Result of evaluating one schedule."""

    hold_reason: HoldReason
    next_eligible_at: datetime | None  # None = either eligible now, or unknown


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


_SLUG_BAD = re.compile(r"[^a-z0-9]+")
_SLUG_TRIM = re.compile(r"^_+|_+$")


def slugify(name: str) -> str:
    """Stable slug for use in entity_ids, log lines, etc.

    Lowercase + collapse non-alphanumeric runs to '_' + trim leading/
    trailing '_'. This is INTENTIONALLY not HA's util.slugify because
    we want the rule simple, deterministic, and free of dependencies
    on HA's slugify changing across versions.
    """
    return _SLUG_TRIM.sub("", _SLUG_BAD.sub("_", name.lower()))


_HHMM = re.compile(r"^(\d{1,2}):(\d{2})(?::\d{2})?$")


def _parse_hhmm(s: str) -> tuple[int, int] | None:
    """Parse 'HH:MM' or 'HH:MM:SS' into (h, m). Seconds discarded."""
    m = _HHMM.match(s)
    if not m:
        return None
    h, mm = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 23 and 0 <= mm <= 59:
        return (h, mm)
    return None


def is_in_sleep_window(now: time, start_hhmm: str, end_hhmm: str) -> bool:
    """Half-open quiet-hours check.

    ``in window`` semantics:
      * same-day window (start < end): ``start <= now < end``
      * overnight window (start > end): ``now >= start OR now < end``
      * zero-width (start == end): NEVER in window (always awake)
      * unparseable input: NEVER in window (fail open)
    """
    s = _parse_hhmm(start_hhmm)
    e = _parse_hhmm(end_hhmm)
    if s is None or e is None:
        return False
    s_min = s[0] * 60 + s[1]
    e_min = e[0] * 60 + e[1]
    if s_min == e_min:
        return False
    n_min = now.hour * 60 + now.minute
    if s_min < e_min:
        return s_min <= n_min < e_min
    return n_min >= s_min or n_min < e_min


def next_eligible_at(
    last_run: datetime | None,
    interval_days: float,
    now: datetime,
) -> datetime | None:
    """When does the cooldown lift?

    Returns None when:
      * no recorded last run (eligible now)
      * interval is non-positive (always eligible — cooldown disabled)
      * the cooldown has already elapsed
    """
    if last_run is None or interval_days <= 0:
        return None
    next_at = last_run + _interval_timedelta(interval_days)
    return next_at if next_at > now else None


def _interval_timedelta(days: float):
    """Avoid importing timedelta at module top to keep imports tight."""
    from datetime import timedelta
    return timedelta(days=days)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


def evaluate(g: GateInputs) -> Evaluation:
    """Compute the current hold reason for a schedule.

    The order below IS the contract — front-end UI and tests rely on
    it. When in doubt about which gate should win, do whatever errors
    on the side of NOT firing the mower. Pause and skip are user
    intent and beat anything mechanical; robot-state beats anything
    environmental; cooldown is the lowest-priority hold (just timing).
    """
    next_at = next_eligible_at(g.last_run, g.interval_days, g.now)

    # User intent first. Pause is the global kill switch; skipped is a
    # one-shot opt-out the user set explicitly for the next run.
    if g.paused:
        return Evaluation("paused", next_at)
    # A manual Pause / hand-stop holds the whole device until Resume. This
    # sits with the other user-intent gates and above every mechanical one:
    # if the user stopped the mower, nothing environmental should restart it.
    if g.manual_hold:
        return Evaluation("manual-hold", next_at)
    if g.skipped:
        return Evaluation("skipped", next_at)

    # Robot must be reachable AND functioning. Both states get reported
    # distinctly so the UI can hint at remediation (turn on the robot
    # vs investigate an error code).
    if not g.robot.online:
        return Evaluation("robot-offline", next_at)
    if g.robot.error_code != 0:
        return Evaluation("robot-busy", next_at)
    if g.robot.is_busy:
        return Evaluation("robot-busy", next_at)

    # Hardware precondition: required attachment must be installed.
    # Sits with the other hardware gates because there's nothing the
    # user can fix by waiting — they need to swap the head physically.
    if g.required_head_type and not _head_matches(
        g.head_type, g.required_head_type
    ):
        return Evaluation("wrong-head", next_at)

    # Resource gates. Battery before presence by convention — a dead
    # battery can't be overridden by the user leaving the house, so
    # report the more fundamental blocker first.
    if g.battery_pct < g.battery_min_pct:
        return Evaluation("battery", next_at)
    if g.presence_at_home:
        return Evaluation("presence", next_at)

    # Environment: quiet hours and weather. Sun-mode is additive to
    # the explicit clock window — both are checked under the same
    # "sleep" reason because to the user they mean the same thing
    # (don't wake the neighbours).
    if is_in_sleep_window(g.now.time(), g.sleep_start, g.sleep_end):
        return Evaluation("sleep", next_at)
    if (
        g.use_sun_for_sleep
        and g.sun_elevation is not None
        and g.sun_elevation < g.sun_elevation_threshold
    ):
        return Evaluation("sleep", next_at)

    # Weather + snow gates. Both bypassed when post_hold_armed is set
    # (the cleanup-after-storm permission slip). Sleep is intentionally
    # NOT bypassed — quiet hours apply even to cleanup runs.
    if not g.post_hold_armed:
        if (
            g.weather_state
            and g.weather_hold_states
            and g.weather_state in g.weather_hold_states
        ):
            return Evaluation("weather", next_at)
        if (
            g.rain_rate_max > 0
            and g.rain_rate is not None
            and g.rain_rate >= g.rain_rate_max
        ):
            return Evaluation("weather", next_at)
        if (
            g.min_snow_accumulation > 0
            and g.snow_estimate is not None
            and g.snow_estimate < g.min_snow_accumulation
        ):
            return Evaluation("snow-insufficient", next_at)

    # Cooldown is the last gate before "go" — also bypassed by
    # post_hold_armed so a cleanup run can fire even mid-cooldown.
    if next_at is not None and not g.post_hold_armed:
        return Evaluation("cooldown", next_at)

    return Evaluation("eligible", None)


def _head_matches(actual: str | None, required: str) -> bool:
    """Case-insensitive substring match. Returns False if actual is
    missing — defensive: we'd rather hold than run with the wrong
    attachment because a sensor is unavailable.
    """
    if not actual:
        return False
    return required.strip().lower() in actual.strip().lower()


# ---------------------------------------------------------------------------
# Spec helpers
# ---------------------------------------------------------------------------


def _coerce_notify_list(value) -> list[str]:
    """Backward-compat: accept None / "" / "notify.x" / list[str]; return list."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [value]
    out: list[str] = []
    for v in value:
        if isinstance(v, str) and v:
            out.append(v)
    return out


def spec_with_defaults(spec: ScheduleSpec) -> ScheduleSpec:
    """Return a copy of `spec` with every field present.

    The options storage may be missing fields when an older schedule is
    loaded after a code update introduces new gates. Apply defaults
    here so callers don't need to know which fields are optional.
    """
    full: ScheduleSpec = {
        "id": spec.get("id", ""),
        "device_sn": spec.get("device_sn", ""),
        "plan_name": spec.get("plan_name", ""),
        "interval_days": float(spec.get("interval_days", 3.0)),
        "weather_entity": spec.get("weather_entity", ""),
        "weather_hold_states": list(
            spec.get("weather_hold_states", DEFAULT_WEATHER_HOLD_STATES)
        ),
        "sleep_start": spec.get("sleep_start", "22:00"),
        "sleep_end": spec.get("sleep_end", "06:00"),
        "use_sun_for_sleep": bool(spec.get("use_sun_for_sleep", False)),
        "sun_elevation_threshold": float(
            spec.get("sun_elevation_threshold", -6.0)
        ),
        "battery_min_pct": int(spec.get("battery_min_pct", 30)),
        "presence_entities": list(spec.get("presence_entities", [])),
        "pre_run_notify_target": _coerce_notify_list(
            spec.get("pre_run_notify_target")
        ),
        "pre_run_notify_minutes": int(spec.get("pre_run_notify_minutes", 5)),
        "complete_notify_target": _coerce_notify_list(
            spec.get("complete_notify_target")
        ),
        "required_head_type": spec.get("required_head_type", "") or "",
        "min_snow_accumulation": float(
            spec.get("min_snow_accumulation", 0.0) or 0.0
        ),
        "snow_forecast_hours": int(
            spec.get("snow_forecast_hours", 12) or 12
        ),
        "rain_rate_entity": spec.get("rain_rate_entity", "") or "",
        "rain_rate_max": float(spec.get("rain_rate_max", 0.0) or 0.0),
        "post_hold_run": bool(spec.get("post_hold_run", False)),
    }
    return full


def schedule_unique_id(sn: str, schedule_id: str, suffix: str) -> str:
    """Stable unique_id for a per-schedule entity.

    Keyed by schedule's UUID, NOT by plan name — so renaming a plan in
    the Yarbo app doesn't rotate the entity_ids and break automations
    or scripts that reference them.
    """
    return f"{sn}_schedule_{schedule_id}_{suffix}"
