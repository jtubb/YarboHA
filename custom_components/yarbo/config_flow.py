"""Config flow for Yarbo integration.

Layout:
    async_step_user           — login
    async_step_select_devices — pick which devices to add (initial setup)
    async_step_reauth*        — re-enter password when refresh token dies

Options flow (multi-step menu):
    init               — menu hub
    ├─ select_devices  — change which managed devices are subscribed
    ├─ add_schedule    — create a new schedule (new uuid)
    └─ manage_schedules
        ├─ edit_schedule    — load one schedule into the same form
        └─ confirm_delete_schedule — two-tap delete
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentryFlow,
    OptionsFlow,
    SubentryFlowResult,
)
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import callback
from homeassistant.data_entry_flow import section
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import selector

from .const import (
    CONF_SCHEDULES,
    CONF_SELECTED_DEVICES,
    CONF_ZONE_RULES,
    DATA_ACCESS_TOKEN,
    DATA_REFRESH_TOKEN,
    DOMAIN,
)
from .scheduler import DEFAULT_WEATHER_HOLD_STATES, spec_with_defaults
from .zone_rules import spec_with_defaults as zr_spec_with_defaults

_LOGGER = logging.getLogger(__name__)

USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
    }
)

REAUTH_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_PASSWORD): str,
    }
)


class YarboConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Yarbo."""

    VERSION = 1

    _reauth_entry: ConfigEntry | None = None
    _email: str | None = None
    _password: str | None = None
    _token: str | None = None
    _refresh_token: str | None = None
    _available_devices: list = []

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> YarboOptionsFlow:
        """Get the options flow for this handler."""
        return YarboOptionsFlow()

    @classmethod
    @callback
    def async_get_supported_subentry_types(
        cls, config_entry: ConfigEntry,
    ) -> dict[str, type]:
        """Subentry types this integration manages.

        Schedules and zone rules are subentries — each appears as its
        own card under the Yarbo device with native HA add / edit /
        delete UI. Replaces the previous list-stored-in-options
        approach where the user had to drill through Configure to
        manage them.
        """
        return {
            "schedule": ScheduleSubentryFlow,
            "zone_rule": ZoneRuleSubentryFlow,
        }

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step — user enters email and password."""
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input[CONF_EMAIL]
            password = user_input[CONF_PASSWORD]

            try:
                token, refresh_token = await self._async_login(email, password)
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            else:
                await self.async_set_unique_id(email.lower())
                self._abort_if_unique_id_configured()

                # Store credentials temporarily and fetch device list
                self._email = email
                self._password = password
                self._token = token
                self._refresh_token = refresh_token

                try:
                    self._available_devices = await self._async_fetch_devices(
                        email, token, refresh_token
                    )
                except CannotConnect:
                    errors["base"] = "fetch_devices_failed"
                else:
                    if not self._available_devices:
                        errors["base"] = "no_devices_found"
                    else:
                        return await self.async_step_select_devices()

        return self.async_show_form(
            step_id="user",
            data_schema=USER_SCHEMA,
            errors=errors,
        )

    async def async_step_select_devices(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle device selection step — user picks which devices to add."""
        errors: dict[str, str] = {}

        if user_input is not None:
            selected = user_input.get(CONF_SELECTED_DEVICES, [])
            if not selected:
                errors["base"] = "no_devices_selected"
            else:
                return self.async_create_entry(
                    title=self._email,
                    data={
                        CONF_EMAIL: self._email,
                        CONF_PASSWORD: self._password,
                        DATA_ACCESS_TOKEN: self._token,
                        DATA_REFRESH_TOKEN: self._refresh_token,
                    },
                    options={CONF_SELECTED_DEVICES: selected},
                )

        return self.async_show_form(
            step_id="select_devices",
            data_schema=self._build_device_schema(),
            errors=errors,
        )

    def _build_device_schema(self) -> vol.Schema:
        """Build multi-select schema from available devices."""
        device_options = {
            device.sn: f"{device.name} ({device.model}) - {device.sn}"
            for device in self._available_devices
        }
        return vol.Schema(
            {
                vol.Optional(CONF_SELECTED_DEVICES, default=[]): cv.multi_select(
                    device_options
                ),
            }
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Handle reauth when refresh token expires."""
        self._reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Ask user for new password during reauth."""
        errors: dict[str, str] = {}

        if user_input is not None and self._reauth_entry is not None:
            email = self._reauth_entry.data[CONF_EMAIL]
            password = user_input[CONF_PASSWORD]

            try:
                token, refresh_token = await self._async_login(email, password)
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            else:
                self.hass.config_entries.async_update_entry(
                    self._reauth_entry,
                    data={
                        **self._reauth_entry.data,
                        CONF_PASSWORD: password,
                        DATA_ACCESS_TOKEN: token,
                        DATA_REFRESH_TOKEN: refresh_token,
                    },
                )
                await self.hass.config_entries.async_reload(
                    self._reauth_entry.entry_id
                )
                return self.async_abort(reason="reauth_successful")

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=REAUTH_SCHEMA,
            errors=errors,
        )

    async def _async_login(self, email: str, password: str) -> tuple[str, str]:
        """Login via SDK. Returns (access_token, refresh_token).

        Raises InvalidAuth or CannotConnect.
        """
        import os

        from yarbo_robot_sdk import AuthenticationError, YarboClient, YarboSDKError

        def _login():
            api_url = os.environ.get("YARBO_API_BASE_URL")
            client = YarboClient(api_base_url=api_url) if api_url else YarboClient()
            client.login(email, password)
            token = client.token
            refresh_token = client.refresh_token
            client.close()
            return token, refresh_token

        try:
            token, refresh_token = await self.hass.async_add_executor_job(_login)
            if not token or not refresh_token:
                raise InvalidAuth
            return token, refresh_token
        except AuthenticationError as err:
            raise InvalidAuth from err
        except YarboSDKError as err:
            raise CannotConnect from err

    async def _async_fetch_devices(
        self, email: str, token: str, refresh_token: str
    ) -> list:
        """Fetch device list using provided tokens.

        Creates a temporary SDK client, restores the session, fetches devices,
        then closes the client. Raises CannotConnect on failure.
        """
        import os

        from yarbo_robot_sdk import YarboClient, YarboSDKError

        def _fetch():
            api_url = os.environ.get("YARBO_API_BASE_URL")
            client = YarboClient(api_base_url=api_url) if api_url else YarboClient()
            try:
                client.restore_session(email, token, refresh_token)
                return client.get_devices()
            finally:
                client.close()

        try:
            return await self.hass.async_add_executor_job(_fetch)
        except YarboSDKError as err:
            _LOGGER.error("Failed to fetch devices: %s", err)
            raise CannotConnect from err


# ---------------------------------------------------------------------------
# Options flow — multi-step menu for device selection + scheduler management.
# ---------------------------------------------------------------------------


# Encoded key joining device_sn + plan_name for the schedule form's plan
# dropdown. Picking one of these gives us both pieces in a single field.
_PLAN_KEY_SEP = "\u241f"  # unit-separator-ish; rare in plan names


def _encode_plan_key(sn: str, plan_name: str) -> str:
    return f"{sn}{_PLAN_KEY_SEP}{plan_name}"


def _decode_plan_key(key: str) -> tuple[str, str] | None:
    if _PLAN_KEY_SEP not in key:
        return None
    sn, plan = key.split(_PLAN_KEY_SEP, 1)
    return (sn, plan)


# Form field names (only used inside this module — schedules persisted
# in options use the ScheduleSpec field names).
_F_PLAN = "plan"
_F_INTERVAL = "interval_days"
_F_WEATHER = "weather_entity"
_F_WEATHER_STATES = "weather_hold_states"
_F_SLEEP_START = "sleep_start"
_F_SLEEP_END = "sleep_end"
_F_USE_SUN = "use_sun_for_sleep"
_F_SUN_THRESHOLD = "sun_elevation_threshold"
_F_BATTERY_MIN = "battery_min_pct"
_F_PRESENCE = "presence_entities"
_F_NOTIFY = "pre_run_notify_target"
_F_NOTIFY_MINUTES = "pre_run_notify_minutes"
_F_NOTIFY_COMPLETE = "complete_notify_target"
_F_REQUIRED_HEAD = "required_head_type"
_F_MIN_SNOW = "min_snow_accumulation"
_F_SNOW_HOURS = "snow_forecast_hours"
_F_POST_HOLD = "post_hold_run"
_F_RAIN_RATE_ENTITY = "rain_rate_entity"
_F_RAIN_RATE_MAX = "rain_rate_max"

# Section keys. The HA frontend renders each as a collapsible accordion;
# user_input arrives nested (one key per section). We flatten before
# the existing save logic via _flatten_sectioned_input.
_SEC_SCHEDULE = "schedule"
_SEC_QUIET = "quiet_hours"
_SEC_CONDITIONS = "conditions"
_SEC_NOTIFICATIONS = "notifications"
# Advanced section holds nerd knobs whose defaults work for 99% of
# users. Surfaced separately (collapsed) so the main form stays
# readable; users who care can find them.
_SEC_ADVANCED = "advanced"

# Map each form field to its section. Used by the flattener.
_FIELD_SECTIONS: dict[str, str] = {
    _F_PLAN: _SEC_SCHEDULE,
    _F_INTERVAL: _SEC_SCHEDULE,
    _F_SLEEP_START: _SEC_QUIET,
    _F_SLEEP_END: _SEC_QUIET,
    _F_USE_SUN: _SEC_QUIET,
    _F_SUN_THRESHOLD: _SEC_ADVANCED,        # tied to use_sun_for_sleep
    _F_BATTERY_MIN: _SEC_CONDITIONS,
    _F_PRESENCE: _SEC_CONDITIONS,
    _F_WEATHER: _SEC_CONDITIONS,
    _F_WEATHER_STATES: _SEC_CONDITIONS,
    _F_REQUIRED_HEAD: _SEC_CONDITIONS,
    _F_MIN_SNOW: _SEC_CONDITIONS,
    _F_SNOW_HOURS: _SEC_ADVANCED,           # tied to min_snow_accumulation
    _F_NOTIFY: _SEC_NOTIFICATIONS,
    _F_NOTIFY_MINUTES: _SEC_ADVANCED,       # tied to pre_run_notify_target
    _F_NOTIFY_COMPLETE: _SEC_NOTIFICATIONS,
    _F_POST_HOLD: _SEC_NOTIFICATIONS,
    _F_RAIN_RATE_ENTITY: _SEC_CONDITIONS,
    _F_RAIN_RATE_MAX: _SEC_CONDITIONS,
}


# ---------------------------------------------------------------------------
# Schedule presets
# ---------------------------------------------------------------------------

# Each preset is a partial spec used to seed defaults on the add form.
# The schema's `default=` clauses fall back to integration defaults
# when a preset doesn't specify a field, so presets can be as terse
# as they want. ``Custom`` is intentionally empty.
_PRESET_CUSTOM = "custom"
_PRESET_STANDARD_MOWING = "standard_mowing"
_PRESET_SNOWBLOWER_CLEANUP = "snowblower_cleanup"
_PRESET_WEEKLY_LAWN = "weekly_lawn"

_SCHEDULE_PRESETS: dict[str, dict[str, Any]] = {
    _PRESET_CUSTOM: {},
    _PRESET_STANDARD_MOWING: {
        "interval_days": 3.0,
        "weather_hold_states": ["rainy", "pouring"],
        "sleep_start": "22:00:00",
        "sleep_end": "06:00:00",
        "use_sun_for_sleep": False,
        "battery_min_pct": 30,
        "required_head_type": "mower",
        "min_snow_accumulation": 0.0,
        "post_hold_run": False,
    },
    _PRESET_SNOWBLOWER_CLEANUP: {
        "interval_days": 0.0,
        "weather_hold_states": ["snowy", "snowy-rainy"],
        # Zero-width quiet hours = always awake. Snow needs early-AM
        # clearing more than mowing needs sleep-time discretion.
        "sleep_start": "00:00:00",
        "sleep_end": "00:00:00",
        "use_sun_for_sleep": False,
        "battery_min_pct": 30,
        "required_head_type": "snow blower",
        "min_snow_accumulation": 0.5,
        "snow_forecast_hours": 12,
        "post_hold_run": True,
    },
    _PRESET_WEEKLY_LAWN: {
        "interval_days": 7.0,
        "weather_hold_states": ["pouring"],   # only block on pouring
        "sleep_start": "21:00:00",
        "sleep_end": "07:00:00",
        "use_sun_for_sleep": False,
        "battery_min_pct": 25,
        "required_head_type": "mower",
        "min_snow_accumulation": 0.0,
        "post_hold_run": False,
    },
}


def _flatten_sectioned_input(user_input: dict[str, Any]) -> dict[str, Any]:
    """Flatten {section: {field: value}} into {field: value}.

    HA's section() wraps each section's fields under the section key in
    user_input. Our existing save logic expects flat keys, so this
    bridges. Pass-through keys (not under any section) are kept as-is.
    """
    flat: dict[str, Any] = {}
    for k, v in user_input.items():
        if isinstance(v, dict):
            flat.update(v)
        else:
            flat[k] = v
    return flat
# Sentinel for "no head requirement" — selectbox option vs the empty
# string we persist. Voluptuous + selector dropdowns don't render an
# empty-string choice cleanly.
_HEAD_ANY = "(any)"


class YarboOptionsFlow(OptionsFlow):
    """Multi-step options: device selection + scheduler CRUD."""

    def __init__(self) -> None:
        super().__init__()
        # Cross-step state. None outside of an in-flight edit/delete.
        self._editing_schedule_id: str | None = None
        self._deleting_schedule_id: str | None = None
        self._editing_rule_id: str | None = None
        self._deleting_rule_id: str | None = None
        # Preset chosen on the add-schedule preset picker. Cleared once
        # the form is submitted (or the user backs out via Configure).
        self._pending_preset: dict[str, Any] | None = None

    # ---- Menu hub -------------------------------------------------------

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Top-level menu.

        Schedules and zone rules now live as HA subentries — added /
        edited / deleted via the device card's native UI rather than
        through this Configure dialog. Only account-level options
        remain here.
        """
        return self.async_show_menu(
            step_id="init", menu_options=["select_devices"],
        )

    # ---- Device selection (preserves existing behaviour) ----------------

    async def async_step_select_devices(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Pick which devices the integration manages."""
        errors: dict[str, str] = {}

        if user_input is not None:
            selected = user_input.get(CONF_SELECTED_DEVICES, [])
            if not selected:
                errors["base"] = "no_devices_selected"
            else:
                return self._async_save_options(
                    {CONF_SELECTED_DEVICES: selected}
                )

        coordinator = self.hass.data.get(DOMAIN, {}).get(
            self.config_entry.entry_id
        )
        if coordinator and coordinator._client:
            try:
                devices = await self.hass.async_add_executor_job(
                    coordinator._client.get_devices
                )
            except Exception as err:
                _LOGGER.error("Failed to fetch devices in options flow: %s", err)
                errors["base"] = "fetch_devices_failed"
                devices = []
        else:
            errors["base"] = "fetch_devices_failed"
            devices = []

        if not devices and not errors:
            errors["base"] = "no_devices_found"

        current_selected = self.config_entry.options.get(
            CONF_SELECTED_DEVICES, []
        )
        valid_sns = {d.sn for d in devices}
        current_selected = [sn for sn in current_selected if sn in valid_sns]

        device_options = {
            d.sn: f"{d.name} ({d.model}) - {d.sn}" for d in devices
        }
        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_SELECTED_DEVICES, default=current_selected
                ): cv.multi_select(device_options),
            }
        )

        return self.async_show_form(
            step_id="select_devices",
            data_schema=schema,
            errors=errors,
        )


# =====================================================================
# Subentry flow helpers — module-level so both ScheduleSubentryFlow
# and ZoneRuleSubentryFlow can use them without OOP gymnastics.
# Each takes (hass, entry) explicitly because subentry flows don't
# have a self.config_entry attribute the way OptionsFlow does.
# =====================================================================


def _coordinator_for(hass, entry):
    return hass.data.get(DOMAIN, {}).get(entry.entry_id)


def _collect_plan_choices(hass, entry) -> dict[str, str]:
    """Build the plan dropdown — every plan on every managed device.

    Encoded as ``<sn><sep><plan_name>`` so a single field carries
    both pieces. Labels include the device name to disambiguate.
    """
    coordinator = _coordinator_for(hass, entry)
    if coordinator is None:
        return {}
    choices: dict[str, str] = {}
    for device in coordinator.devices:
        plans = coordinator.plan_data.get(device.sn, []) or []
        for p in plans:
            name = p.get("name")
            if not name:
                continue
            choices[_encode_plan_key(device.sn, name)] = (
                f"{device.name}: {name}"
            )
    return choices


def _collect_notify_services(hass) -> list[str]:
    """Return all registered notify.* services as `notify.<name>` strings.

    Excludes the meta-services that take a target list (notify.notify,
    notify.persistent_notification) — these are usable but rarely
    what someone wiring a per-plan notification wants. They can be
    typed in by hand thanks to ``custom_value=True``.
    """
    services = hass.services.async_services().get("notify", {})
    skip = {"notify", "persistent_notification", "send_message"}
    return sorted(
        f"notify.{name}" for name in services if name not in skip
    )


async def _ensure_plans_loaded(hass, entry) -> None:
    """Trigger a plan-list refresh for any device whose cache is empty.

    Called before ``_collect_plan_choices`` in flows that need the
    dropdown populated. Recovers from the case where the SDK's
    initial ``read_all_plan`` HTTP call timed out at startup —
    instead of the user seeing an empty dropdown and having to find
    the refresh button manually, opening the add-schedule flow
    triggers the refetch transparently.

    Failures are logged-and-ignored — the caller's own
    ``no_plans_loaded`` error message will surface to the user if
    refresh didn't help.
    """
    coordinator = _coordinator_for(hass, entry)
    if coordinator is None:
        return
    for device in coordinator.devices:
        if coordinator.plan_data.get(device.sn):
            continue
        try:
            await coordinator.async_refresh_plans(device.sn, device.type_id)
        except Exception as err:
            _LOGGER.warning(
                "[plan-refresh] %s failed during form open: %s",
                device.sn, err,
            )


def _plan_exists_on_device(hass, entry, sn: str, plan_name: str) -> bool:
    coordinator = _coordinator_for(hass, entry)
    if coordinator is None:
        return False
    plans = coordinator.plan_data.get(sn, []) or []
    return any(p.get("name") == plan_name for p in plans)


def _collect_head_choices(hass, entry) -> list[str]:
    """Heads available across all managed devices, plus an "(any)" sentinel."""
    from homeassistant.util import slugify
    from homeassistant.helpers import entity_registry as er

    coordinator = _coordinator_for(hass, entry)
    choices: list[str] = [_HEAD_ANY]
    if coordinator is None:
        return choices
    seen: set[str] = set()
    ent_reg = er.async_get(hass)
    for device in coordinator.devices:
        ent_id = f"sensor.{slugify(device.name)}_head_type"
        state = hass.states.get(ent_id)
        if state is None:
            for ent in ent_reg.entities.values():
                if (
                    ent.domain == "sensor"
                    and ent.unique_id
                    and device.sn in ent.unique_id
                    and "head_type" in ent.unique_id.lower()
                ):
                    state = hass.states.get(ent.entity_id)
                    break
        if state is None:
            continue
        for opt in state.attributes.get("options") or []:
            if opt and opt not in seen:
                seen.add(opt)
                choices.append(opt)
    return choices


def _collect_zone_choices(hass, entry) -> dict[str, dict[str, str]]:
    """Live nogo-zone dict per device. Keyed by sn → {zone_id_str: label}."""
    coordinator = _coordinator_for(hass, entry)
    out: dict[str, dict[str, str]] = {}
    if coordinator is None:
        return out
    for device in coordinator.devices:
        raw = (coordinator._map_raw.get(device.sn) or {})
        zones = raw.get("nogozones") or []
        zmap = {}
        for z in zones:
            zid = z.get("id")
            if zid is None:
                continue
            name = z.get("name") or f"Zone {zid}"
            zmap[str(zid)] = f"{device.name}: {name}"
        if zmap:
            out[device.sn] = zmap
    return out


def _build_schedule_schema(
    hass,
    entry,
    *,
    defaults: dict | None,
    plan_choices: dict[str, str],
    head_choices: list[str],
    notify_services: list[str] | None = None,
) -> vol.Schema:
    """Schema for add/edit schedule form. Pure builder."""
    if defaults and defaults.get("device_sn") and defaults.get("plan_name"):
        default_plan = _encode_plan_key(
            defaults["device_sn"], defaults["plan_name"]
        )
    else:
        default_plan = next(iter(plan_choices), "")

    weather_options_full = list(DEFAULT_WEATHER_HOLD_STATES) + [
        "fog", "lightning", "lightning-rainy",
    ]

    schedule_section = vol.Schema({
        vol.Required(_F_PLAN, default=default_plan): vol.In(plan_choices),
        vol.Required(
            _F_INTERVAL,
            default=defaults["interval_days"] if defaults else 3.0,
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0, max=30, step=0.5, mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="days",
            )
        ),
    })
    quiet_section = vol.Schema({
        vol.Required(
            _F_SLEEP_START,
            default=(defaults["sleep_start"] if defaults else "22:00:00"),
        ): selector.TimeSelector(),
        vol.Required(
            _F_SLEEP_END,
            default=(defaults["sleep_end"] if defaults else "06:00:00"),
        ): selector.TimeSelector(),
        vol.Required(
            _F_USE_SUN,
            default=(defaults["use_sun_for_sleep"] if defaults else False),
        ): selector.BooleanSelector(),
    })
    conditions_section = vol.Schema({
        vol.Required(
            _F_BATTERY_MIN,
            default=(defaults["battery_min_pct"] if defaults else 30),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0, max=100, step=5, mode=selector.NumberSelectorMode.SLIDER,
                unit_of_measurement="%",
            )
        ),
        vol.Optional(
            _F_PRESENCE,
            default=(defaults["presence_entities"] if defaults else []),
        ): selector.EntitySelector(
            selector.EntitySelectorConfig(
                domain=["person", "device_tracker", "zone"], multiple=True,
            )
        ),
        vol.Optional(
            _F_WEATHER,
            default=(defaults["weather_entity"] if defaults else ""),
        ): selector.EntitySelector(
            selector.EntitySelectorConfig(domain="weather")
        ),
        vol.Required(
            _F_WEATHER_STATES,
            default=(
                defaults["weather_hold_states"] if defaults
                else list(DEFAULT_WEATHER_HOLD_STATES)
            ),
        ): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=weather_options_full,
                multiple=True, custom_value=True,
                mode=selector.SelectSelectorMode.LIST,
            )
        ),
        vol.Required(
            _F_REQUIRED_HEAD,
            default=(
                defaults["required_head_type"] or _HEAD_ANY
                if defaults else _HEAD_ANY
            ),
        ): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=head_choices, custom_value=True,
                mode=selector.SelectSelectorMode.DROPDOWN,
            )
        ),
        vol.Required(
            _F_MIN_SNOW,
            default=(defaults["min_snow_accumulation"] if defaults else 0.0),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0, max=100, step=0.1,
                mode=selector.NumberSelectorMode.BOX,
            )
        ),
        vol.Optional(
            _F_RAIN_RATE_ENTITY,
            default=(defaults["rain_rate_entity"] if defaults else ""),
        ): selector.EntitySelector(
            selector.EntitySelectorConfig(domain="sensor")
        ),
        vol.Required(
            _F_RAIN_RATE_MAX,
            default=(defaults["rain_rate_max"] if defaults else 0.0),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0, max=10000, step=1,
                mode=selector.NumberSelectorMode.BOX,
            )
        ),
    })
    # Coerce stored notify-target to a list so the multi-select widget
    # can pre-select existing values regardless of whether storage has
    # the legacy single string or the new list.
    notify_default: list[str] = []
    if defaults:
        raw = defaults.get("pre_run_notify_target") or ""
        if isinstance(raw, list):
            notify_default = list(raw)
        elif isinstance(raw, str) and raw:
            notify_default = [raw]
    complete_default: list[str] = []
    if defaults:
        raw_c = defaults.get("complete_notify_target") or []
        if isinstance(raw_c, list):
            complete_default = list(raw_c)
        elif isinstance(raw_c, str) and raw_c:
            complete_default = [raw_c]
    notifications_section = vol.Schema({
        vol.Optional(
            _F_NOTIFY,
            default=notify_default,
        ): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=notify_services or [],
                multiple=True,
                custom_value=True,   # let users type a service we missed
                mode=selector.SelectSelectorMode.DROPDOWN,
            )
        ),
        vol.Optional(
            _F_NOTIFY_COMPLETE,
            default=complete_default,
        ): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=notify_services or [],
                multiple=True,
                custom_value=True,
                mode=selector.SelectSelectorMode.DROPDOWN,
            )
        ),
        vol.Required(
            _F_POST_HOLD,
            default=(defaults["post_hold_run"] if defaults else False),
        ): selector.BooleanSelector(),
    })
    advanced_section = vol.Schema({
        vol.Required(
            _F_SUN_THRESHOLD,
            default=(
                defaults["sun_elevation_threshold"] if defaults else -6.0
            ),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=-18, max=5, step=0.5,
                mode=selector.NumberSelectorMode.SLIDER,
                unit_of_measurement="°",
            )
        ),
        vol.Required(
            _F_SNOW_HOURS,
            default=(defaults["snow_forecast_hours"] if defaults else 12),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=1, max=48, step=1,
                mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="h",
            )
        ),
        vol.Required(
            _F_NOTIFY_MINUTES,
            default=(defaults["pre_run_notify_minutes"] if defaults else 5),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0, max=30, step=1,
                mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="min",
            )
        ),
    })
    return vol.Schema({
        vol.Required(_SEC_SCHEDULE): section(schedule_section, {"collapsed": False}),
        vol.Required(_SEC_QUIET): section(quiet_section, {"collapsed": True}),
        vol.Required(_SEC_CONDITIONS): section(conditions_section, {"collapsed": True}),
        vol.Required(_SEC_NOTIFICATIONS): section(notifications_section, {"collapsed": True}),
        vol.Required(_SEC_ADVANCED): section(advanced_section, {"collapsed": True}),
    })


def _normalize_notify_targets(value) -> list[str]:
    """Coerce form value to a clean list[str].

    Accepts None, str (legacy single value), or list[str]. Trims
    whitespace, drops empties, dedupes while preserving order.
    """
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    seen: set[str] = set()
    out: list[str] = []
    for v in value:
        if not isinstance(v, str):
            continue
        s = v.strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _user_input_to_schedule_spec(
    user_input: dict[str, Any], schedule_id: str, sn: str, plan_name: str,
) -> dict:
    """Convert flat user_input to a stored schedule spec."""
    return {
        "id": schedule_id,
        "device_sn": sn,
        "plan_name": plan_name,
        "interval_days": float(user_input.get(_F_INTERVAL, 3.0)),
        "weather_entity": user_input.get(_F_WEATHER, "") or "",
        "weather_hold_states": list(
            user_input.get(_F_WEATHER_STATES, list(DEFAULT_WEATHER_HOLD_STATES))
        ),
        "sleep_start": user_input.get(_F_SLEEP_START, "22:00:00"),
        "sleep_end": user_input.get(_F_SLEEP_END, "06:00:00"),
        "use_sun_for_sleep": bool(user_input.get(_F_USE_SUN, False)),
        "sun_elevation_threshold": float(
            user_input.get(_F_SUN_THRESHOLD, -6.0)
        ),
        "battery_min_pct": int(user_input.get(_F_BATTERY_MIN, 30)),
        "presence_entities": list(user_input.get(_F_PRESENCE, [])),
        "pre_run_notify_target": _normalize_notify_targets(
            user_input.get(_F_NOTIFY)
        ),
        "pre_run_notify_minutes": int(user_input.get(_F_NOTIFY_MINUTES, 5)),
        "complete_notify_target": _normalize_notify_targets(
            user_input.get(_F_NOTIFY_COMPLETE)
        ),
        "required_head_type": (
            ""
            if user_input.get(_F_REQUIRED_HEAD, _HEAD_ANY) == _HEAD_ANY
            else user_input.get(_F_REQUIRED_HEAD, "")
        ),
        "rain_rate_entity": user_input.get(_F_RAIN_RATE_ENTITY, "") or "",
        "rain_rate_max": float(user_input.get(_F_RAIN_RATE_MAX, 0.0) or 0.0),
        "min_snow_accumulation": float(
            user_input.get(_F_MIN_SNOW, 0.0) or 0.0
        ),
        "snow_forecast_hours": int(user_input.get(_F_SNOW_HOURS, 12) or 12),
        "post_hold_run": bool(user_input.get(_F_POST_HOLD, False)),
    }


# =====================================================================
# ScheduleSubentryFlow — add / edit one schedule via the device card's
# native subentry UI. Replaces the previous OptionsFlow drill-down.
# =====================================================================


class ScheduleSubentryFlow(ConfigSubentryFlow):
    """One schedule = one HA subentry under the Yarbo device card."""

    def __init__(self) -> None:
        super().__init__()
        self._pending_preset: dict[str, Any] | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Add path: start with the preset picker."""
        self._pending_preset = None
        return await self.async_step_pick_preset()

    async def async_step_pick_preset(
        self, user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        if user_input is not None:
            preset_key = user_input.get("preset", _PRESET_CUSTOM)
            self._pending_preset = dict(_SCHEDULE_PRESETS.get(preset_key, {}))
            return await self.async_step_add()
        preset_choices = [
            selector.SelectOptionDict(
                value=_PRESET_CUSTOM, label="Custom (start blank)",
            ),
            selector.SelectOptionDict(
                value=_PRESET_STANDARD_MOWING,
                label="Standard mowing — every 3 days, quiet 22-06, weather hold",
            ),
            selector.SelectOptionDict(
                value=_PRESET_SNOWBLOWER_CLEANUP,
                label="Snowblower with cleanup — daily, post-hold sweep",
            ),
            selector.SelectOptionDict(
                value=_PRESET_WEEKLY_LAWN,
                label="Weekly lawn (lenient) — every 7 days, only pouring blocks",
            ),
        ]
        return self.async_show_form(
            step_id="pick_preset",
            data_schema=vol.Schema({
                vol.Required(
                    "preset", default=_PRESET_CUSTOM,
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=preset_choices,
                        mode=selector.SelectSelectorMode.LIST,
                    )
                ),
            }),
        )

    async def async_step_add(
        self, user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        return await self._async_form(
            user_input=user_input, step_id="add", existing=None,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        existing_sub = self._get_reconfigure_subentry()
        return await self._async_form(
            user_input=user_input,
            step_id="reconfigure",
            existing=dict(existing_sub.data),
        )

    async def _async_form(
        self,
        *,
        user_input: dict[str, Any] | None,
        step_id: str,
        existing: dict | None,
    ) -> SubentryFlowResult:
        entry = self._get_entry()
        errors: dict[str, str] = {}
        # Auto-recover from a startup-time plan-fetch timeout: trigger
        # a refresh now if the cache is empty, then build choices.
        plan_choices = _collect_plan_choices(self.hass, entry)
        if not plan_choices:
            await _ensure_plans_loaded(self.hass, entry)
            plan_choices = _collect_plan_choices(self.hass, entry)
        if not plan_choices:
            errors["base"] = "no_plans_loaded"

        if user_input is not None:
            user_input = _flatten_sectioned_input(user_input)
        if user_input is not None and not errors:
            decoded = _decode_plan_key(user_input.get(_F_PLAN, ""))
            sn, plan_name = (None, None)
            if decoded is None:
                errors[_F_PLAN] = "invalid_plan"
            else:
                sn, plan_name = decoded
                if not _plan_exists_on_device(self.hass, entry, sn, plan_name):
                    errors[_F_PLAN] = "plan_not_found"
            if not errors:
                schedule_id = (
                    existing.get("id") if existing else uuid.uuid4().hex
                )
                spec = _user_input_to_schedule_spec(
                    user_input, schedule_id, sn, plan_name,
                )
                if existing is None:
                    self._pending_preset = None
                    return self.async_create_entry(
                        title=plan_name or "Schedule",
                        data=spec,
                        unique_id=schedule_id,
                    )
                return self.async_update_and_abort(
                    entry,
                    self._get_reconfigure_subentry(),
                    title=plan_name or "Schedule",
                    data_updates=spec,
                )

        # Defaults priority: existing > pending_preset > schema defaults.
        if existing:
            from .scheduler import spec_with_defaults as _sd
            defaults = _sd(existing)
        elif self._pending_preset:
            from .scheduler import spec_with_defaults as _sd
            defaults = _sd(self._pending_preset)
        else:
            defaults = None
        head_choices = _collect_head_choices(self.hass, entry)
        notify_services = _collect_notify_services(self.hass)
        schema = _build_schedule_schema(
            self.hass, entry,
            defaults=defaults,
            plan_choices=plan_choices,
            head_choices=head_choices,
            notify_services=notify_services,
        )
        return self.async_show_form(
            step_id=step_id, data_schema=schema, errors=errors,
        )


# =====================================================================
# ZoneRuleSubentryFlow — add / edit one zone rule via the device card.
# =====================================================================


_ZR_SEC_PRECIP = "precipitation_settings"
_ZR_SEC_PRESENCE = "presence_settings"


def _build_zone_rule_schema(
    *, defaults: dict | None, zone_options: dict[str, str],
) -> vol.Schema:
    default_zones: list[str] = []
    if defaults:
        sn = defaults.get("device_sn", "")
        for z in defaults.get("zone_ids", []):
            default_zones.append(f"{sn}|{z}")
    default_trigger = (
        defaults.get("trigger_type") if defaults else "precipitation"
    ) or "precipitation"
    precip_section = vol.Schema({
        vol.Optional(
            "rate_entity",
            default=(defaults["rate_entity"] if defaults else ""),
        ): selector.EntitySelector(
            selector.EntitySelectorConfig(
                domain="sensor",
                device_class="precipitation_intensity",
            )
        ),
        vol.Required(
            "event_threshold",
            default=(defaults["event_threshold"] if defaults else 0.5),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0.01, max=10, step=0.01,
                mode=selector.NumberSelectorMode.BOX,
            )
        ),
        vol.Required(
            "dry_reset_hours",
            default=(defaults["dry_reset_hours"] if defaults else 6),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0.5, max=72, step=0.5,
                mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="h",
            )
        ),
    })
    presence_section = vol.Schema({
        vol.Optional(
            "presence_entities",
            default=(defaults["presence_entities"] if defaults else []),
        ): selector.EntitySelector(
            selector.EntitySelectorConfig(
                domain=["person", "device_tracker", "zone"],
                multiple=True,
            )
        ),
    })
    return vol.Schema({
        vol.Required(
            "name", default=(defaults["name"] if defaults else "Wet ground"),
        ): selector.TextSelector(),
        vol.Required(
            "zones", default=default_zones,
        ): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=[
                    selector.SelectOptionDict(value=v, label=l)
                    for v, l in zone_options.items()
                ],
                multiple=True,
                mode=selector.SelectSelectorMode.DROPDOWN,
            )
        ),
        vol.Required(
            "trigger_type", default=default_trigger,
        ): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=[
                    selector.SelectOptionDict(
                        value="precipitation", label="Precipitation",
                    ),
                    selector.SelectOptionDict(
                        value="presence", label="Presence",
                    ),
                ],
                mode=selector.SelectSelectorMode.DROPDOWN,
                translation_key="zone_rule_trigger_type",
            )
        ),
        vol.Required(
            "duration_hours",
            default=(defaults["duration_hours"] if defaults else 48),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0.5, max=240, step=0.5,
                mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="h",
            )
        ),
        vol.Required(_ZR_SEC_PRECIP): section(
            precip_section, {"collapsed": default_trigger != "precipitation"},
        ),
        vol.Required(_ZR_SEC_PRESENCE): section(
            presence_section, {"collapsed": default_trigger != "presence"},
        ),
    })


class ZoneRuleSubentryFlow(ConfigSubentryFlow):
    """One zone rule = one HA subentry."""

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        return await self._async_form(
            user_input=user_input, step_id="add", existing=None,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        existing_sub = self._get_reconfigure_subentry()
        return await self._async_form(
            user_input=user_input,
            step_id="reconfigure",
            existing=dict(existing_sub.data),
        )

    async def _async_form(
        self,
        *,
        user_input: dict[str, Any] | None,
        step_id: str,
        existing: dict | None,
    ) -> SubentryFlowResult:
        entry = self._get_entry()
        errors: dict[str, str] = {}
        zone_choices = _collect_zone_choices(self.hass, entry)
        zone_options: dict[str, str] = {}
        for sn, zmap in zone_choices.items():
            for zid, label in zmap.items():
                zone_options[f"{sn}|{zid}"] = label
        if not zone_options:
            errors["base"] = "no_zones_loaded"

        if user_input is not None:
            user_input = _flatten_sectioned_input(user_input)
        if user_input is not None and not errors:
            picked = user_input.get("zones") or []
            sns = {p.split("|", 1)[0] for p in picked if "|" in p}
            trigger = user_input.get("trigger_type") or "precipitation"
            if not picked:
                errors["zones"] = "no_zones_selected"
            elif len(sns) != 1:
                errors["zones"] = "single_device_only"
            elif trigger == "presence" and not (
                user_input.get("presence_entities") or []
            ):
                errors["base"] = "no_presence_entities"
            elif trigger == "precipitation" and not (
                user_input.get("rate_entity") or ""
            ):
                errors["base"] = "no_rate_entity"
            else:
                rule_id = (existing.get("id") if existing else uuid.uuid4().hex)
                sn = next(iter(sns))
                zone_ids = [
                    int(p.split("|", 1)[1]) for p in picked
                    if "|" in p and p.startswith(f"{sn}|")
                ]
                spec = {
                    "id": rule_id,
                    "name": user_input.get("name") or "Zone rule",
                    "device_sn": sn,
                    "zone_ids": zone_ids,
                    "trigger_type": trigger,
                    "rate_entity": user_input.get("rate_entity") or "",
                    "event_threshold": float(
                        user_input.get("event_threshold", 0.5) or 0.5
                    ),
                    "duration_hours": float(
                        user_input.get("duration_hours", 48) or 48
                    ),
                    "dry_reset_hours": float(
                        user_input.get("dry_reset_hours", 6) or 6
                    ),
                    "presence_entities": list(
                        user_input.get("presence_entities") or []
                    ),
                }
                if existing is None:
                    return self.async_create_entry(
                        title=spec["name"],
                        data=spec,
                        unique_id=rule_id,
                    )
                return self.async_update_and_abort(
                    entry,
                    self._get_reconfigure_subentry(),
                    title=spec["name"],
                    data_updates=spec,
                )

        from .zone_rules import spec_with_defaults as _zr_sd
        defaults = _zr_sd(existing) if existing else None
        schema = _build_zone_rule_schema(
            defaults=defaults, zone_options=zone_options,
        )
        return self.async_show_form(
            step_id=step_id, data_schema=schema, errors=errors,
        )


class InvalidAuth(Exception):
    """Error to indicate invalid credentials."""


class CannotConnect(Exception):
    """Error to indicate connection failure."""
