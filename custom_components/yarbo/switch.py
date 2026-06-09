"""Switch platform for Yarbo integration — configuration-driven."""

from __future__ import annotations

import logging
import time

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import YarboDataUpdateCoordinator
from .scheduler import schedule_unique_id

_LOGGER = logging.getLogger(__name__)

# Seconds to ignore coordinator updates after sending a command,
# preventing UI flicker from stale DeviceMSG data.
COMMAND_COOLDOWN_SECONDS = 5


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Yarbo switch entities from SDK control field definitions."""
    from yarbo_robot_sdk import get_control_field_definitions

    coordinator: YarboDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]

    entities: list[SwitchEntity] = []
    for device in coordinator.devices:
        ctrl_defs = await hass.async_add_executor_job(
            get_control_field_definitions, device.type_id
        )
        for ctrl_def in ctrl_defs:
            if ctrl_def.entity_type == "switch":
                entities.append(YarboConfigSwitch(coordinator, device, ctrl_def))
        # Per-device global scheduler pause. Always present, even when
        # zero schedules are configured — gives the user a single place
        # to halt everything before they create their first schedule.
        entities.append(YarboSchedulerGlobalSwitch(coordinator, device))

    async_add_entities(entities)

    # Per-schedule pause — added per subentry so HA can remove it
    # surgically when the schedule is deleted.
    for device in coordinator.devices:
        for spec in coordinator.schedules_for(device.sn):
            sub_id = coordinator.subentry_id_for("schedule", spec.get("id"))
            async_add_entities(
                [YarboScheduleEnabledSwitch(coordinator, device, spec)],
                config_subentry_id=sub_id,
            )


class YarboConfigSwitch(
    CoordinatorEntity[YarboDataUpdateCoordinator], SwitchEntity
):
    """Configuration-driven switch entity for sound and light control."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device, ctrl_def) -> None:
        super().__init__(coordinator)
        self._device = device
        self._ctrl_def = ctrl_def
        self._command_sent_at: float = 0  # monotonic timestamp of last command

        path_key = ctrl_def.path.replace(".", "_").replace("__", "").lower()
        self._attr_unique_id = f"{device.sn}_{path_key}_switch"
        self._attr_name = ctrl_def.name
        self._attr_entity_registry_enabled_default = ctrl_def.enabled_by_default
        self._attr_is_on: bool | None = None

        if ctrl_def.icon:
            self._attr_icon = ctrl_def.icon

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.sn)},
            name=self._device.name,
            manufacturer="Yarbo",
            model=self._device.model,
            serial_number=self._device.sn,
        )

    @callback
    def _handle_coordinator_update(self) -> None:
        """Sync switch state from coordinator data.

        Skips state sync during the cooldown period after a command to prevent
        UI flicker from stale DeviceMSG data arriving before the device
        processes the command.
        """
        if time.monotonic() - self._command_sent_at < COMMAND_COOLDOWN_SECONDS:
            # Within cooldown — keep optimistic state, just refresh HA state
            self.async_write_ha_state()
            return

        raw = self._get_state_value()
        if raw is not None:
            if self._ctrl_def.command_builder == "light_switch":
                new_state = raw != 0 and raw is not False
            else:
                new_state = bool(raw)
            if new_state != self._attr_is_on:
                self._attr_is_on = new_state
        self.async_write_ha_state()

    async def async_turn_on(self, **kwargs) -> None:
        """Turn on the switch."""
        self._attr_is_on = True
        self.async_write_ha_state()
        await self._async_send_command(True)

    async def async_turn_off(self, **kwargs) -> None:
        """Turn off the switch."""
        self._attr_is_on = False
        self.async_write_ha_state()
        await self._async_send_command(False)

    async def _async_send_command(self, turn_on: bool) -> None:
        """Build and send the MQTT command payload."""
        self._command_sent_at = time.monotonic()
        payload = self._build_payload(turn_on)
        try:
            await self.hass.async_add_executor_job(
                self.coordinator._client.mqtt_publish_command,
                self._device.sn,
                self._device.type_id,
                self._ctrl_def.command_topic,
                payload,
            )
        except Exception as exc:
            _LOGGER.error("[switch] mqtt_publish_command FAILED: %s", exc)
            self._handle_coordinator_update()

    def _build_payload(self, turn_on: bool) -> dict:
        """Build command payload based on command_builder type."""
        builder = self._ctrl_def.command_builder
        if builder == "sound_switch":
            current_vol = self._get_sibling_value("StateMSG.volume")
            vol = round(float(current_vol), 1) if current_vol is not None else 1.0
            return {"enable": turn_on, "vol": vol, "mode": 0}
        elif builder == "light_switch":
            val = 255 if turn_on else 0
            return {
                "body_left_r": val,
                "body_right_r": val,
                "led_head": val,
                "led_left_w": val,
                "led_right_w": val,
                "tail_left_r": val,
                "tail_right_r": val,
            }
        return {}

    def _get_state_value(self):
        """Extract current state value from coordinator data."""
        if not self.coordinator.data:
            return None
        device_data = self.coordinator.data.get(self._device.sn)
        if device_data is None:
            return None
        from yarbo_robot_sdk.device_helpers import extract_field
        return extract_field(device_data, self._ctrl_def.path)

    def _get_sibling_value(self, field_path: str):
        """Extract a sibling field value from coordinator data."""
        if not self.coordinator.data:
            return None
        device_data = self.coordinator.data.get(self._device.sn)
        if device_data is None:
            return None
        from yarbo_robot_sdk.device_helpers import extract_field
        return extract_field(device_data, field_path)


# ---- Scheduler switches --------------------------------------------------


def _scheduler_device_info(device) -> DeviceInfo:
    return DeviceInfo(
        identifiers={(DOMAIN, device.sn)},
        name=device.name,
        manufacturer="Yarbo",
        model=device.model,
        serial_number=device.sn,
    )


class YarboSchedulerGlobalSwitch(
    CoordinatorEntity[YarboDataUpdateCoordinator], SwitchEntity
):
    """Global scheduler pause switch for one device.

    ON  = scheduler is enabled (will fire schedules when conditions pass)
    OFF = paused (no schedule on this device fires until re-enabled)

    The "ON = enabled" framing matches user intuition for a switch
    labeled "Scheduler enabled". Per-schedule pause is a separate
    switch and ANDs with this one.
    """

    _attr_has_entity_name = True
    _attr_name = "Scheduler enabled"
    _attr_icon = "mdi:calendar-clock"

    def __init__(self, coordinator: YarboDataUpdateCoordinator, device) -> None:
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{device.sn}_scheduler_enabled"

    @property
    def device_info(self) -> DeviceInfo:
        return _scheduler_device_info(self._device)

    @property
    def is_on(self) -> bool | None:
        store = self.coordinator.state_store
        if store is None:
            return None
        return store.get_global_enabled(self._device.sn)

    async def async_turn_on(self, **kwargs) -> None:
        await self.coordinator.async_set_global_enabled(self._device.sn, True)

    async def async_turn_off(self, **kwargs) -> None:
        await self.coordinator.async_set_global_enabled(self._device.sn, False)


class YarboScheduleEnabledSwitch(
    CoordinatorEntity[YarboDataUpdateCoordinator], SwitchEntity
):
    """Per-schedule enable/disable switch.

    ON = this schedule may fire when its conditions pass.
    OFF = this schedule is permanently held (until re-enabled).

    Use this for "I want to disable mowing the back yard until next
    season" without deleting the schedule entirely.
    """

    _attr_has_entity_name = True
    _attr_icon = "mdi:calendar-arrow-right"

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
            device.sn, self._schedule_id, "enabled",
        )
        self._attr_name = f"Schedule {self._plan_name} enabled"

    @property
    def device_info(self) -> DeviceInfo:
        return _scheduler_device_info(self._device)

    @property
    def is_on(self) -> bool | None:
        store = self.coordinator.state_store
        if store is None:
            return None
        return store.get_schedule_state(
            self._device.sn, self._schedule_id,
        )["enabled"]

    async def async_turn_on(self, **kwargs) -> None:
        await self.coordinator.async_set_schedule_enabled(
            self._device.sn, self._schedule_id, True,
        )

    async def async_turn_off(self, **kwargs) -> None:
        await self.coordinator.async_set_schedule_enabled(
            self._device.sn, self._schedule_id, False,
        )
