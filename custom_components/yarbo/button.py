"""Button platform for Yarbo integration — data refresh and plan control."""

from __future__ import annotations

import logging

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.exceptions import HomeAssistantError
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import YarboDataUpdateCoordinator
from .entity_filters import control_matches_device
from .scheduler import schedule_unique_id

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Yarbo button entities."""
    from yarbo_robot_sdk import get_control_field_definitions

    coordinator: YarboDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]

    entities = []
    for device in coordinator.devices:
        ctrl_defs = await hass.async_add_executor_job(
            get_control_field_definitions, device.type_id
        )
        for ctrl_def in ctrl_defs:
            if ctrl_def.entity_type == "button" and control_matches_device(
                coordinator, device, ctrl_def
            ):
                entities.append(YarboConfigButton(coordinator, device, ctrl_def))

        # Data refresh buttons
        entities.append(YarboRefreshGpsRefButton(coordinator, device))
        entities.append(YarboRefreshMapDataButton(coordinator, device))
        entities.append(YarboRefreshDeviceMsgButton(coordinator, device))
        entities.append(YarboRefreshPlansButton(coordinator, device))
        # Plan control buttons
        entities.append(YarboStartPlanButton(coordinator, device))
        entities.append(YarboPausePlanButton(coordinator, device))
        entities.append(YarboResumePlanButton(coordinator, device))
        entities.append(YarboStopPlanButton(coordinator, device))
        # Recharge button
        entities.append(YarboRechargeButton(coordinator, device))
    async_add_entities(entities)

    # Per-schedule action buttons. These deliberately do NOT pass
    # config_subentry_id: they attach to the shared mower device, and
    # HA allows a device to belong to only one subentry. With one
    # subentry per schedule, every add moved the device to a different
    # one; as of 2026.8 that move detaches it from the main config
    # entry, which purges every main-entry entity on the next reload.
    # Losing surgical subentry cleanup is the lesser cost — stale
    # entities are pruned on reload instead.
    for device in coordinator.devices:
        for spec in coordinator.schedules_for(device.sn):
            async_add_entities(
                [
                    YarboScheduleRunNowButton(coordinator, device, spec),
                    YarboScheduleSkipNextButton(coordinator, device, spec),
                ],
            )


def _device_info(device) -> DeviceInfo:
    return DeviceInfo(
        identifiers={(DOMAIN, device.sn)},
        name=device.name,
        manufacturer="Yarbo",
        model=device.model,
        serial_number=device.sn,
    )


class YarboConfigButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Configuration-driven button entity."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device, ctrl_def) -> None:
        super().__init__(coordinator)
        self._device = device
        self._ctrl_def = ctrl_def
        self._playing: bool = False  # toggle state for play_sound

        path_key = ctrl_def.path.replace(".", "_").replace("__", "").lower()
        self._attr_unique_id = f"{device.sn}_{path_key}_button"
        self._attr_name = ctrl_def.name
        self._attr_entity_registry_enabled_default = ctrl_def.enabled_by_default

        if ctrl_def.icon:
            self._attr_icon = ctrl_def.icon

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        sn = self._device.sn
        builder = self._ctrl_def.command_builder
        _LOGGER.info(
            "Pressing %s for %s via %s builder=%s",
            self._ctrl_def.name, sn, self._ctrl_def.command_topic, builder,
        )
        try:
            bound = self.coordinator.bound_device(sn)
            if bound is not None:
                if builder == "play_sound":
                    if self._playing:
                        await self.hass.async_add_executor_job(bound.core.song_cmd, "null")
                        self._playing = False
                    else:
                        await self.hass.async_add_executor_job(bound.core.song_cmd)
                        self._playing = True
                    return
                if builder == "ignore_obstacle_zones":
                    await self.hass.async_add_executor_job(bound.core.clear_obstacle_zones)
                    return
                if builder == "empty_payload" and self._ctrl_def.command_topic == "stop":
                    await self.hass.async_add_executor_job(bound.core.stop)
                    return
            # fallback: raw mqtt_publish_command for unrecognised builders
            payload = self._build_payload()
            await self.hass.async_add_executor_job(
                self.coordinator._client.mqtt_publish_command,
                sn,
                self._device.type_id,
                self._ctrl_def.command_topic,
                payload,
            )
        except Exception as exc:
            _LOGGER.error("[button] command FAILED: %s", exc)
            raise HomeAssistantError(
                f"Failed to send {self._ctrl_def.name} command: {exc}"
            ) from exc

    def _build_payload(self) -> dict:
        builder = self._ctrl_def.command_builder
        if builder == "ignore_obstacle_zones":
            return {"zone": []}
        if builder == "play_sound":
            if self._playing:
                self._playing = False
                return {"song_name": "null"}
            self._playing = True
            return {"song_name": "find yarbo"}
        if builder == "empty_payload":
            return {}
        return self._ctrl_def.extra_payload or {}


# ---- Data refresh buttons ----


class YarboRefreshGpsRefButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to refresh GPS reference origin from the device."""

    _attr_has_entity_name = True
    _attr_name = "Refresh GPS Reference"
    _attr_icon = "mdi:crosshairs-gps"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_refresh_gps_ref"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Refreshing GPS reference for %s", self._device.sn)
        await self.coordinator.async_refresh_gps_ref(
            self._device.sn, self._device.type_id
        )


class YarboRefreshMapDataButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to refresh map/zone data from the device."""

    _attr_has_entity_name = True
    _attr_name = "Refresh Map Data"
    _attr_icon = "mdi:map-marker-radius"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_refresh_map_data"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Refreshing map data for %s", self._device.sn)
        await self.coordinator.async_refresh_map_data(
            self._device.sn, self._device.type_id
        )


class YarboRefreshDeviceMsgButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to refresh full DeviceMSG snapshot from the device."""

    _attr_has_entity_name = True
    _attr_name = "Refresh Device Data"
    _attr_icon = "mdi:refresh"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_refresh_device_msg"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Refreshing DeviceMSG for %s", self._device.sn)
        await self.coordinator.async_refresh_device_msg(
            self._device.sn, self._device.type_id
        )


class YarboRefreshPlansButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to refresh auto plan list from the device."""

    _attr_has_entity_name = True
    _attr_name = "Refresh Plans"
    _attr_icon = "mdi:clipboard-list"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_refresh_plans"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Refreshing plans for %s", self._device.sn)
        await self.coordinator.async_refresh_plans(
            self._device.sn, self._device.type_id
        )


# ---- Plan control buttons ----


class YarboStartPlanButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to start a selected auto plan."""

    _attr_has_entity_name = True
    _attr_name = "Start Plan"
    _attr_icon = "mdi:play"

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_start_plan"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        sn = self._device.sn
        plan_id = self.coordinator.get_selected_plan(sn)
        if plan_id is None:
            raise HomeAssistantError("Cannot start plan: no plan selected")
        # Preflight + publish (and last_run stamping for any matching
        # schedule) is centralized on the coordinator; both this button
        # and the scheduler tick share the exact same code path.
        percent = self._get_plan_percent()
        await self.coordinator.async_start_plan(
            sn, plan_id, percent=int(percent) if percent else None,
            triggered_by="start_plan_button",
        )

    def _get_plan_percent(self) -> float | None:
        """Read plan start percent from the entity state registry."""
        entity_id = f"number.{self._device.name.lower().replace(' ', '_')}_plan_start_percent"
        state = self.hass.states.get(entity_id)
        if state and state.state not in (None, "unknown", "unavailable"):
            try:
                return float(state.state)
            except ValueError:
                pass
        return None


class YarboPausePlanButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to pause the current plan."""

    _attr_has_entity_name = True
    _attr_name = "Pause Plan"
    _attr_icon = "mdi:pause"

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_pause_plan"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Pausing plan for %s", self._device.sn)
        try:
            bound = self.coordinator.bound_device(self._device.sn)
            if bound is not None:
                await self.hass.async_add_executor_job(bound.core.pause)
            else:
                await self.hass.async_add_executor_job(
                    self.coordinator._client.core.pause,
                    self._device.sn, self._device.type_id,
                )
        except Exception as exc:
            _LOGGER.error("Failed to pause plan: %s", exc)
            return
        # Hold the scheduler until Resume. Without this the next tick sees
        # an idle robot with a saved resume_percent and restarts the very
        # plan the user just paused.
        await self.coordinator.async_set_manual_hold(self._device.sn, True)


class YarboResumePlanButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to resume the paused plan."""

    _attr_has_entity_name = True
    _attr_name = "Resume Plan"
    _attr_icon = "mdi:play"

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_resume_plan"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Resuming plan for %s", self._device.sn)
        try:
            bound = self.coordinator.bound_device(self._device.sn)
            if bound is not None:
                await self.hass.async_add_executor_job(bound.core.resume)
            else:
                await self.hass.async_add_executor_job(
                    self.coordinator._client.core.resume,
                    self._device.sn, self._device.type_id,
                )
        except Exception as exc:
            # Release the hold even if the resume command itself failed:
            # pressing Resume is the user's intent to release, and the
            # command legitimately errors when the robot is docked/idle.
            # Returning early here would leave the device held with no
            # obvious way out.
            _LOGGER.error("Failed to resume plan: %s", exc)
        await self.coordinator.async_set_manual_hold(self._device.sn, False)


class YarboStopPlanButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to stop the current plan."""

    _attr_has_entity_name = True
    _attr_name = "Stop Plan"
    _attr_icon = "mdi:stop"

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_stop_plan"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        _LOGGER.info("Stopping plan for %s", self._device.sn)
        try:
            bound = self.coordinator.bound_device(self._device.sn)
            if bound is not None:
                await self.hass.async_add_executor_job(bound.core.stop)
            else:
                await self.hass.async_add_executor_job(
                    self.coordinator._client.core.stop,
                    self._device.sn, self._device.type_id,
                )
        except Exception as exc:
            _LOGGER.error("Failed to stop plan: %s", exc)
            return
        # Same intent as a stop issued from the app: the user ended this run
        # deliberately, so hold the scheduler until Resume. Without this,
        # stopping from HA behaved differently from stopping in the app,
        # which observes app/stop_plan and holds.
        await self.coordinator.async_set_manual_hold(self._device.sn, True)


# ---- Recharge button ----


class YarboRechargeButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Button to send the device back to the charging station."""

    _attr_has_entity_name = True
    _attr_name = "Return to Charge"
    _attr_icon = "mdi:battery-charging"

    def __init__(self, coordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_recharge"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        sn = self._device.sn
        data = self.coordinator.data.get(sn, {}) if self.coordinator.data else {}

        # Check 1: Device must be online
        if not data.get("__online__"):
            raise HomeAssistantError(
                "Cannot return to charge: device is offline"
            )

        # Check 2: Not currently charging (BatteryMSG.status > 1 means charging)
        battery_status = (data.get("BatteryMSG") or {}).get("status")
        if isinstance(battery_status, (int, float)) and battery_status > 1:
            raise HomeAssistantError(
                "Cannot return to charge: device is already charging"
            )

        # Check 3: Not already recharging (on_going_recharging > 0 and != 4)
        recharging = (data.get("StateMSG") or {}).get("on_going_recharging", 0)
        if isinstance(recharging, (int, float)) and recharging > 0 and recharging != 4:
            raise HomeAssistantError(
                "Cannot return to charge: device is already returning to charge"
            )

        # Check 4: RTK signal must not be weak (4=Strong, 5=Medium, else=Weak)
        rtk_status = (data.get("RTKMSG") or {}).get("status")
        rtk_val = int(rtk_status) if rtk_status is not None else 0
        if rtk_val not in (4, 5):
            raise HomeAssistantError(
                "Cannot return to charge: RTK/GPS signal is weak"
            )

        _LOGGER.info("Starting recharge for %s", sn)
        try:
            bound = self.coordinator.bound_device(sn)
            if bound is not None:
                await self.hass.async_add_executor_job(bound.core.wireless_charging_cmd, 0)
                await self.hass.async_add_executor_job(bound.core.return_to_charge)
            else:
                await self.hass.async_add_executor_job(
                    self.coordinator._client.core.wireless_charging_cmd,
                    sn, 0, self._device.type_id,
                )
                await self.hass.async_add_executor_job(
                    self.coordinator._client.core.return_to_charge,
                    sn, self._device.type_id,
                )
        except Exception as exc:
            _LOGGER.error("Failed to send recharge command: %s", exc)


# ---- Scheduler buttons --------------------------------------------------


class YarboScheduleRunNowButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Trigger one schedule now, bypassing its cooldown.

    Goes through coordinator.async_start_plan_by_name so the same
    preflight checks apply as for the device's main Start Plan button.
    last_run is stamped on success, satisfying any future scheduled
    run on the same plan (no double-run a few minutes later).
    """

    _attr_has_entity_name = True
    _attr_icon = "mdi:play-circle-outline"

    def __init__(
        self,
        coordinator: YarboDataUpdateCoordinator,
        device,
        spec: dict,
    ) -> None:
        super().__init__(coordinator)
        self._device = device
        self._schedule_id: str = spec["id"]
        self._plan_name: str = spec.get("plan_name", "")
        self._attr_unique_id = schedule_unique_id(
            device.sn, self._schedule_id, "run_now",
        )
        self._attr_name = f"Schedule {self._plan_name} run now"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        # If a previous attempt at this schedule ended without success,
        # pick up where it left off rather than re-mowing already-done
        # area. resume_percent is cleared on the next successful
        # Completed transition.
        store = self.coordinator.state_store
        percent = None
        if store is not None:
            pct = store.get_schedule_state(
                self._device.sn, self._schedule_id,
            )["resume_percent"]
            if pct > 0:
                percent = pct
        await self.coordinator.async_start_plan_by_name(
            self._device.sn, self._plan_name, percent=percent,
            triggered_by="schedule_run_now_button",
        )


class YarboScheduleSkipNextButton(
    CoordinatorEntity[YarboDataUpdateCoordinator], ButtonEntity
):
    """Toggle the per-schedule one-shot skip flag.

    Pressing while skip is OFF sets it ON — the next eligible window is
    bypassed and the flag clears automatically when the schedule next
    fires (or when the user toggles it off via the per-schedule pause
    switch in advance).
    """

    _attr_has_entity_name = True
    _attr_icon = "mdi:skip-next-circle-outline"

    def __init__(
        self,
        coordinator: YarboDataUpdateCoordinator,
        device,
        spec: dict,
    ) -> None:
        super().__init__(coordinator)
        self._device = device
        self._schedule_id: str = spec["id"]
        self._plan_name: str = spec.get("plan_name", "")
        self._attr_unique_id = schedule_unique_id(
            device.sn, self._schedule_id, "skip_next",
        )
        self._attr_name = f"Schedule {self._plan_name} skip next"

    @property
    def device_info(self) -> DeviceInfo:
        return _device_info(self._device)

    async def async_press(self) -> None:
        store = self.coordinator.state_store
        if store is None:
            return
        current = store.get_schedule_state(
            self._device.sn, self._schedule_id,
        )["skip_next"]
        await self.coordinator.async_set_skip_next(
            self._device.sn, self._schedule_id, not current,
        )
