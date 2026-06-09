"""Sensor platform for Yarbo integration — configuration-driven."""

from __future__ import annotations

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import YarboDataUpdateCoordinator
from .scheduler import HOLD_LABELS, schedule_unique_id, spec_with_defaults
from .zone_rules import (
    spec_with_defaults as zr_spec_with_defaults,
    state_with_defaults as zr_state_with_defaults,
)

# Sensor device_classes that represent a numeric measurement
MEASUREMENT_CLASSES = {"battery", "temperature", "humidity", "distance", "pressure"}

# on_going_planning status code → display text
PLANNING_STATUS_MAP: dict[int, str] = {
    0: "Not Started",
    1: "Cleaning",
    2: "Calculating Route",
    3: "Heading to Area",
    5: "Completed",
    11: "Waypoint Navigation",
    12: "Waypoint Complete",
    -2: "Error: Create Plan History Failed (WP002)",
    -10: "Error: Plan Not Found (WP003)",
    -11: "Error: Failed to Read Plan (WP004)",
    -12: "Error: Failed to Calculate Route (WP005)",
    -20: "Error: Outside Mapped Area (WP006)",
    -21: "Error: Area Data Error (WP007)",
    -22: "Error: Route Data Error (WP008)",
    -23: "Error: In No-Go Zone",
    -24: "Error: Low Battery",
    -26: "Error: Module Position Failure (WP012)",
    -30: "Error: Location Data Exception (WP013)",
    -31: "Error: Docking Station Exception (WP014)",
    -40: "Error: Obstacle Mark Failed",
    -42: "Error: Out of Boundary",
    -43: "Error: Unable to Navigate Obstacle (WP016)",
    -44: "Error: Exceeded Boundary (WP017)",
    -47: "Error: Out of Boundary >1.5m",
    -88: "Error: In No-Go Zone",
    -92: "Error: Out of Boundary (WP025)",
}

# on_going_recharging status code → display text
RECHARGING_STATUS_MAP: dict[int, str] = {
    0: "Not Started",
    1: "Returning on Path",
    2: "Returning in Area",
    3: "Repositioning",
    4: "Charging",
    99: "Verifying",
    -2: "Error: Server Error",
    -3: "Error: Direction Uninitialized",
    -4: "Error: Docking Station Uninitialized",
    -5: "Error: Recharge Failed (REC005)",
    -6: "Error: Failed to Park",
    -8: "Error: Docking Connection Failed",
    -9: "Error: Stuck",
    -20: "Error: Outside Mapped Area",
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Yarbo sensors dynamically from SDK field definitions."""
    from yarbo_robot_sdk import get_field_definitions

    coordinator: YarboDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]

    entities: list[SensorEntity] = []
    for device in coordinator.devices:
        field_defs = await hass.async_add_executor_job(
            get_field_definitions, device.type_id
        )
        for field_def in field_defs:
            if field_def.entity_type == "sensor":
                entities.append(YarboConfigSensor(coordinator, device, field_def))

    # Add map zone sensors
    from .map_sensor import YarboMapSensor

    for device in coordinator.devices:
        entities.append(YarboMapSensor(coordinator, device))
        # Position-Z sensor — relative-to-dock altitude in meters.
        # Useful for graphing slope traversal during a run.
        entities.append(YarboPositionZSensor(coordinator, device))
        # Onboard rain-sensor numeric reading. Off by default; enable
        # when wiring into a schedule's rain-rate gate or automations.
        entities.append(YarboRainSensor(coordinator, device))
        # Cumulative plan-progress % from the mower's plan_feedback,
        # matches the Yarbo app's progress display (preserved across
        # mid-plan auto-recharges).
        entities.append(YarboPlanProgressSensor(coordinator, device))

    # Per-device "next scheduled run" sensor (not per-subentry).
    for device in coordinator.devices:
        entities.append(YarboNextScheduledRunSensor(coordinator, device))

    async_add_entities(entities)

    # Per-schedule status sensors — added per subentry so HA can
    # surgically remove them when a schedule subentry is deleted.
    for device in coordinator.devices:
        for spec in coordinator.schedules_for(device.sn):
            sub_id = coordinator.subentry_id_for("schedule", spec.get("id"))
            async_add_entities(
                [YarboScheduleStatusSensor(coordinator, device, spec)],
                config_subentry_id=sub_id,
            )
        for rule in coordinator.zone_rules_for(device.sn):
            sub_id = coordinator.subentry_id_for("zone_rule", rule.get("id"))
            async_add_entities(
                [YarboZoneRuleStatusSensor(coordinator, device, rule)],
                config_subentry_id=sub_id,
            )


class YarboConfigSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Configuration-driven sensor — one class for all sensor fields."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device, field_def) -> None:
        super().__init__(coordinator)
        self._device = device
        self._field_def = field_def

        # Unique ID from SN + normalized path
        path_key = field_def.path.replace(".", "_").replace("__", "").lower()
        self._attr_unique_id = f"{device.sn}_{path_key}"
        self._attr_name = field_def.name
        self._attr_entity_registry_enabled_default = field_def.enabled_by_default

        # Device class
        if field_def.value_map:
            self._attr_device_class = SensorDeviceClass.ENUM
            self._attr_options = list(dict.fromkeys(field_def.value_map.values()))
        elif field_def.device_class:
            try:
                self._attr_device_class = SensorDeviceClass(field_def.device_class)
            except ValueError:
                pass

        # State class for numeric measurements
        if (
            field_def.device_class in MEASUREMENT_CLASSES
            and not field_def.value_map
        ):
            self._attr_state_class = SensorStateClass.MEASUREMENT

        # Unit and icon
        if field_def.unit:
            self._attr_native_unit_of_measurement = field_def.unit
        if field_def.icon:
            self._attr_icon = field_def.icon

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @property
    def native_value(self):
        # Special extraction for custom_extractor fields (e.g. network_priority)
        if self._field_def.custom_extractor:
            return self._extract_custom()
        raw = self._extract(self._field_def.path)
        if raw is None:
            return None
        if self._field_def.value_map:
            mapped = self._field_def.value_map.get(str(raw))
            if mapped is not None:
                return mapped
            # For numeric values, check if a negative fallback exists (e.g. all negatives → "Error")
            if isinstance(raw, (int, float)) and raw < 0:
                return self._field_def.value_map.get("-1")
            return None
        return raw

    def _extract_custom(self):
        """Handle fields with custom_extractor logic."""
        data = self._get_device_data()
        if data is None:
            return None
        if self._field_def.custom_extractor == "network_priority":
            from yarbo_robot_sdk.device_helpers import extract_active_network, extract_field
            route_priority = extract_field(data, self._field_def.path)
            return extract_active_network(route_priority)
        if self._field_def.custom_extractor == "volume_scale":
            from yarbo_robot_sdk.device_helpers import extract_field
            raw = extract_field(data, self._field_def.path)
            if raw is None:
                return None
            return int(float(raw) * 100)
        if self._field_def.custom_extractor == "rtk_signal":
            from yarbo_robot_sdk.device_helpers import extract_field
            raw = extract_field(data, self._field_def.path)
            # APP logic: 4=Strong, 5=Medium, everything else=Weak
            raw_int = int(raw) if raw is not None else None
            if raw_int == 4:
                return "Strong"
            if raw_int == 5:
                return "Medium"
            return "Weak"
        if self._field_def.custom_extractor == "planning_status":
            from yarbo_robot_sdk.device_helpers import extract_field
            raw = extract_field(data, self._field_def.path)
            if raw is None:
                return None
            code = int(raw)
            if code in PLANNING_STATUS_MAP:
                return PLANNING_STATUS_MAP[code]
            return "Error" if code < 0 else None
        if self._field_def.custom_extractor == "recharging_status":
            from yarbo_robot_sdk.device_helpers import extract_field
            raw = extract_field(data, self._field_def.path)
            if raw is None:
                return None
            code = int(raw)
            if code in RECHARGING_STATUS_MAP:
                return RECHARGING_STATUS_MAP[code]
            return "Error" if code < 0 else None
        return None

    def _extract(self, field_path: str):
        """Extract a field value from MQTT data."""
        data = self._get_device_data()
        if data is None:
            return None
        from yarbo_robot_sdk.device_helpers import extract_field
        return extract_field(data, field_path)

    def _get_device_data(self) -> dict | None:
        if self.coordinator.data and self._device.sn in self.coordinator.data:
            return self.coordinator.data[self._device.sn]
        return None


# ---- Scheduler status sensor --------------------------------------------


# All possible hold-reason strings so we can declare the sensor as a
# typed enum. Order matches scheduler.HOLD_LABELS — the constant is the
# source of truth, and a runtime check below catches drift.
_HOLD_REASON_OPTIONS: list[str] = list(HOLD_LABELS.keys())


class YarboScheduleStatusSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Status of one schedule: hold reason as state, full evaluation as attrs.

    The state is one of the HoldReason literals defined in scheduler.py.
    "eligible" means the schedule will fire on the next tick (modulo a
    hard preflight failure inside coordinator.async_start_plan).
    "cooldown" means everything else passed but the interval hasn't
    elapsed; ``next_eligible_at`` carries the timestamp.

    Implementation note: there's no ``native_value`` cache — the
    evaluation is cheap (in-memory state), and the coordinator's
    update fires on every MQTT push, so the sensor naturally tracks
    the current state without explicit invalidation.
    """

    _attr_has_entity_name = True
    _attr_icon = "mdi:calendar-check-outline"
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = _HOLD_REASON_OPTIONS
    # Friendly state labels via strings.json's
    # entity.sensor.schedule_status.state.* block. Without this, HA
    # shows raw values like "robot-busy" / "snow-insufficient" in the
    # entity tile and Developer Tools.
    _attr_translation_key = "schedule_status"

    def __init__(
        self,
        coordinator: YarboDataUpdateCoordinator,
        device,
        spec: dict,
    ) -> None:
        super().__init__(coordinator)
        self._device = device
        self._spec = spec  # raw dict from options; resolved with defaults at read
        self._schedule_id: str = spec["id"]
        self._plan_name: str = spec.get("plan_name", "")
        self._attr_unique_id = schedule_unique_id(
            device.sn, self._schedule_id, "status",
        )
        self._attr_name = f"Schedule {self._plan_name}"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @property
    def native_value(self) -> str | None:
        # Re-resolve every read in case the user just edited the
        # spec via options flow — entry.options is the source of truth.
        spec = self._current_spec()
        if spec is None:
            return None
        return self.coordinator.evaluate_schedule(spec).hold_reason

    @property
    def extra_state_attributes(self) -> dict:
        spec = self._current_spec()
        if spec is None:
            return {"schedule_id": self._schedule_id, "missing": True}
        full = spec_with_defaults(spec)
        result = self.coordinator.evaluate_schedule(spec)
        store = self.coordinator.state_store
        last_run = (
            store.get_last_run(self._device.sn, self._schedule_id)
            if store else None
        )
        sched_state = (
            store.get_schedule_state(self._device.sn, self._schedule_id)
            if store else {"enabled": True, "skip_next": False}
        )
        return {
            "schedule_id": self._schedule_id,
            "plan_name": self._plan_name,
            "hold_reason": result.hold_reason,
            "hold_label": HOLD_LABELS.get(result.hold_reason, result.hold_reason),
            "next_eligible_at": (
                result.next_eligible_at.isoformat()
                if result.next_eligible_at else None
            ),
            "last_run": last_run.isoformat() if last_run else None,
            "interval_days": full["interval_days"],
            "skip_next": sched_state["skip_next"],
            "schedule_enabled": sched_state["enabled"],
            # 0 = next start is fresh; N>0 = next start passes
            # percent=N to start_plan, picking up where the last
            # non-success attempt left off. Cleared on successful
            # Completed.
            "resume_percent": sched_state["resume_percent"],
            "required_head_type": full["required_head_type"] or None,
            "min_snow_accumulation": full["min_snow_accumulation"],
            "snow_forecast_hours": full["snow_forecast_hours"],
            "post_hold_run": full["post_hold_run"],
            "post_hold_armed": sched_state.get("post_hold_armed", False),
            "global_enabled": (
                store.get_global_enabled(self._device.sn) if store else False
            ),
            "weather_entity": full["weather_entity"] or None,
            "sleep_window": f"{full['sleep_start']}–{full['sleep_end']}",
            "use_sun_for_sleep": full["use_sun_for_sleep"],
            "battery_min_pct": full["battery_min_pct"],
            "presence_entities": full["presence_entities"],
        }

    def _current_spec(self) -> dict | None:
        """Re-fetch this schedule's spec from entry.options.

        Falls back to the spec captured at construction if the schedule
        is no longer in options (shouldn't happen — integration reload
        on options change recreates entities — but defensive in case
        of races during reload).
        """
        for s in self.coordinator.schedules:
            if (
                s.get("id") == self._schedule_id
                and s.get("device_sn") == self._device.sn
            ):
                return s
        return self._spec


class YarboNextScheduledRunSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Per-device aggregate: when does the next scheduled plan run?

    State = ISO timestamp of the soonest cooldown-end across all this
    device's configured schedules. None (rendered "unknown" by HA) when
    no schedule has a known future timestamp — either because nothing
    is configured, every schedule is currently eligible (will fire on
    the next minute tick), or every schedule is held by an external
    condition (weather/sleep/presence/battery) with no predictable
    end time.

    Attributes carry which plan is next + why it's currently held, plus
    a count of total schedules for context.

    Useful for: a single mobile-card glance ("when's the mower coming
    out next?"), automations triggered by state changes (notify N
    minutes before, refill battery threshold, etc.).
    """

    _attr_has_entity_name = True
    _attr_name = "Next scheduled run"
    _attr_icon = "mdi:calendar-arrow-right"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(
        self,
        coordinator: YarboDataUpdateCoordinator,
        device,
    ) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_next_scheduled_run"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @property
    def native_value(self):
        soonest = self._compute_soonest()
        return soonest["next_at"] if soonest else None

    @property
    def extra_state_attributes(self) -> dict:
        schedules = self.coordinator.schedules_for(self._device.sn)
        soonest = self._compute_soonest()
        if soonest is None:
            # Differentiate the two "no timestamp" cases for clarity.
            if not schedules:
                status = "no_schedules"
            else:
                # Something is configured but no schedule has a future
                # cooldown — they're all eligible now (will fire on
                # next tick) OR held by an external gate.
                statuses = {
                    self.coordinator.evaluate_schedule(s).hold_reason
                    for s in schedules
                }
                status = "eligible" if "eligible" in statuses else "held"
            return {
                "status": status,
                "schedules_count": len(schedules),
            }
        return {
            "plan_name": soonest["plan_name"],
            "schedule_id": soonest["schedule_id"],
            "hold_reason": soonest["hold_reason"],
            "schedules_count": len(schedules),
        }

    def _compute_soonest(self) -> dict | None:
        """Find the schedule with the soonest non-None next_eligible_at.

        Schedules currently `eligible` (next_at None because cooldown
        already elapsed) are intentionally excluded — they fire on the
        next tick, so a "when" timestamp would be misleading.
        """
        best: dict | None = None
        for spec in self.coordinator.schedules_for(self._device.sn):
            ev = self.coordinator.evaluate_schedule(spec)
            if ev.next_eligible_at is None:
                continue
            if best is None or ev.next_eligible_at < best["next_at"]:
                best = {
                    "next_at": ev.next_eligible_at,
                    "plan_name": spec.get("plan_name", ""),
                    "schedule_id": spec.get("id", ""),
                    "hold_reason": ev.hold_reason,
                }
        return best


# ---- Zone rule status sensor --------------------------------------------


_ZONE_RULE_STATUS_OPTIONS = ["paused", "idle", "awaiting", "engaged", "released"]


class YarboZoneRuleStatusSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Status of one zone rule.

    State = current rule status (idle / awaiting / engaged / paused).
    Attributes carry the accumulator value, threshold, expires_at,
    and the zones the rule controls — useful for automations and at-
    a-glance dashboards.
    """

    _attr_has_entity_name = True
    _attr_icon = "mdi:weather-rainy"
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = _ZONE_RULE_STATUS_OPTIONS
    # Friendly state labels via strings.json's
    # entity.sensor.zone_rule_status.state.* block.
    _attr_translation_key = "zone_rule_status"

    def __init__(self, coordinator, device, rule: dict) -> None:
        super().__init__(coordinator)
        self._device = device
        self._rule = rule
        self._rule_id: str = rule["id"]
        self._rule_name: str = rule.get("name", "Zone rule") or "Zone rule"
        # Stable across plan/zone renames — keyed by uuid, not name.
        self._attr_unique_id = f"{device.sn}_zone_rule_{self._rule_id}_status"
        self._attr_name = f"Zone rule {self._rule_name}"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @property
    def native_value(self) -> str | None:
        rule = self._current_rule()
        if rule is None:
            return None
        spec = zr_spec_with_defaults(rule)
        state = self.coordinator._zone_rule_state(self._rule_id)
        if not state.get("enabled", True):
            return "paused"
        if state.get("auto_owned_zones"):
            return "engaged"
        if (state.get("accumulator") or 0) > 0:
            return "awaiting"
        return "idle"

    @property
    def extra_state_attributes(self) -> dict:
        rule = self._current_rule() or self._rule
        spec = zr_spec_with_defaults(rule)
        state = self.coordinator._zone_rule_state(self._rule_id)
        from datetime import datetime, timezone
        expires_iso = None
        if state.get("expires_at") is not None:
            try:
                expires_iso = datetime.fromtimestamp(
                    float(state["expires_at"]), tz=timezone.utc,
                ).isoformat()
            except (TypeError, ValueError):
                expires_iso = None
        last_precip_iso = None
        if state.get("last_precip_at") is not None:
            try:
                last_precip_iso = datetime.fromtimestamp(
                    float(state["last_precip_at"]), tz=timezone.utc,
                ).isoformat()
            except (TypeError, ValueError):
                last_precip_iso = None
        return {
            "rule_id": self._rule_id,
            "rule_name": spec.get("name"),
            "zone_ids": spec.get("zone_ids", []),
            "rate_entity": spec.get("rate_entity"),
            "event_threshold": spec.get("event_threshold"),
            "duration_hours": spec.get("duration_hours"),
            "dry_reset_hours": spec.get("dry_reset_hours"),
            "accumulator": round(float(state.get("accumulator") or 0), 4),
            "auto_owned_zones": state.get("auto_owned_zones", []),
            "expires_at": expires_iso,
            "last_precip_at": last_precip_iso,
            "rule_enabled": state.get("enabled", True),
        }

    def _current_rule(self) -> dict | None:
        for r in self.coordinator.zone_rules():
            if r.get("id") == self._rule_id and r.get("device_sn") == self._device.sn:
                return r
        return None


# ---- Position-Z sensor ----------------------------------------------------


class YarboPositionZSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Relative altitude of the mower above the dock reference, in meters.

    State = ``height_msl - reference_hgt`` from the live RTK fix
    (data_feedback's ``lat_lon_hight`` triple). Positive when the
    mower is uphill from the dock; negative when downhill. ``unknown``
    when no fix has been received yet.

    The mower's own odometry (CombinedOdom) is planar (x/y only), so
    this is the only altitude-aware position field exposed. The
    absolute MSL value is in the ``msl`` attribute for users who want
    that frame.
    """

    _attr_has_entity_name = True
    _attr_name = "Position Z"
    _attr_icon = "mdi:elevation-rise"
    # Intentionally NO device_class. The distance device_class triggers
    # HA's unit-system auto-conversion (m → ft on imperial systems),
    # which makes Position Z mismatch the device_tracker's
    # position_x / position_y attributes (which are raw meters from
    # CombinedOdom). All three planar/altitude position values are now
    # consistently meters. Users wanting imperial can add a template
    # sensor or convert in their card.
    _attr_native_unit_of_measurement = "m"
    _attr_state_class = SensorStateClass.MEASUREMENT
    # Show 3 decimals (mm). The live source is RTK-quality so cm-scale
    # variation between samples is meaningful — without this HA may
    # auto-round to 1 decimal and the value will appear frozen.
    _attr_suggested_display_precision = 3
    # Off by default — most users won't care about altitude tracking.
    # Easy to enable per-entity when wanted.
    _attr_entity_registry_enabled_default = False

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_position_z"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @property
    def native_value(self) -> float | None:
        z_rel, _ = self.coordinator.position_z_for(self._device.sn)
        return None if z_rel is None else round(z_rel, 3)

    @property
    def available(self) -> bool:
        # Live source is RTKMSG.hgt in the continuous DeviceMSG. Fall
        # back to the last on-demand read_gps_ref snapshot. Don't gate
        # on rtkFixType — consumers who want fresh-fix-only can check
        # the rtk_fix_type attribute.
        data = (self.coordinator.data or {}).get(self._device.sn) or {}
        rtk = data.get("RTKMSG")
        if isinstance(rtk, dict) and rtk.get("hgt") is not None:
            return True
        live = self.coordinator.live_positions.get(self._device.sn)
        return bool(live and "height_msl" in live)

    @property
    def extra_state_attributes(self) -> dict:
        data = (self.coordinator.data or {}).get(self._device.sn) or {}
        rtk_raw = data.get("RTKMSG")
        rtk = rtk_raw if isinstance(rtk_raw, dict) else {}
        live = self.coordinator.live_positions.get(self._device.sn) or {}
        _, z_msl = self.coordinator.position_z_for(self._device.sn)
        return {
            "msl": z_msl,
            "reference_hgt": (
                self.coordinator.gps_refs.get(self._device.sn) or {}
            ).get("hgt"),
            "rtk_fix_type": live.get("rtk_fix_type"),
            "lat": rtk.get("lan", live.get("lat")),
            "lon": rtk.get("lon", live.get("lon")),
            "heading": rtk.get("heading"),
        }


class YarboPlanProgressSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Cumulative coverage of the current/most-recent plan run, 0-100%.

    Prefers ``actualCleanArea / totalCleanArea`` (literal mowed area —
    matches the Yarbo app), falling back to ``finishCleanArea /
    totalCleanArea`` (the resume-from value, which slightly
    overestimates since it counts whole zones the firmware no longer
    needs to redo). Preserved across mid-plan auto-recharges.
    None when no plan_feedback has been received yet.
    """

    _attr_has_entity_name = True
    _attr_name = "Plan progress"
    _attr_icon = "mdi:progress-clock"
    _attr_native_unit_of_measurement = "%"
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_plan_progress"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    def _feedback(self) -> dict | None:
        pf = getattr(self.coordinator, "_plan_feedback", None)
        if not isinstance(pf, dict):
            return None
        sn_data = pf.get(self._device.sn)
        return sn_data if isinstance(sn_data, dict) else None

    @staticmethod
    def _to_float(v) -> float | None:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @property
    def native_value(self) -> float | None:
        pf = self._feedback()
        if pf is None:
            return None
        total = self._to_float(pf.get("totalCleanArea"))
        if total is None or total <= 0:
            return None
        actual = self._to_float(pf.get("actualCleanArea"))
        finish = self._to_float(pf.get("finishCleanArea"))
        # Prefer actualCleanArea (matches the app); fall back to
        # finishCleanArea when not present.
        numerator = actual if actual is not None else finish
        if numerator is None:
            return None
        pct = max(0.0, min(100.0, (numerator / total) * 100.0))
        return round(pct, 1)

    @property
    def extra_state_attributes(self) -> dict:
        pf = self._feedback() or {}
        return {
            "plan_id": pf.get("planId"),
            "actual_clean_area": pf.get("actualCleanArea"),
            "finish_clean_area": pf.get("finishCleanArea"),
            "total_clean_area": pf.get("totalCleanArea"),
        }


class YarboRainSensor(
    CoordinatorEntity[YarboDataUpdateCoordinator], SensorEntity
):
    """Onboard rain-sensor reading from RunningStatusMSG.rain_sensor_data.

    Numeric value as reported by the mower. Units are device-defined
    (likely an ADC reading or 0/1). Higher = wetter. Updates only
    while the mower is awake; stale when docked/sleeping.
    """

    _attr_has_entity_name = True
    _attr_name = "Rain sensor"
    _attr_icon = "mdi:weather-rainy"
    _attr_state_class = SensorStateClass.MEASUREMENT
    # Off by default — most users won't care unless they wire it into
    # the schedule's rain-rate gate or their own automations.
    _attr_entity_registry_enabled_default = False

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_rain_sensor"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @property
    def native_value(self) -> float | None:
        data = (self.coordinator.data or {}).get(self._device.sn) or {}
        rs = data.get("RunningStatusMSG")
        if not isinstance(rs, dict):
            return None
        raw = rs.get("rain_sensor_data")
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
