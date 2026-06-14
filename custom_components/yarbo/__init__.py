"""The Yarbo integration."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN, PLATFORMS
from .coordinator import YarboDataUpdateCoordinator
from .websocket_api import async_register as async_register_websockets

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Yarbo from a config entry."""
    coordinator = YarboDataUpdateCoordinator(hass, entry)
    await coordinator.async_setup()
    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    # Register the on-demand map (GeoJSON) WebSocket command once. Kept out of
    # entity attributes to stay under the recorder's 16 KB limit.
    async_register_websockets(hass)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    _register_set_nogozone_enabled(hass)
    _register_export_altitude_mesh(hass)
    _register_get_altitude_mesh(hass)
    _register_clear_altitude_area(hass)
    _register_probe_topic(hass)
    _register_crud_services(hass)

    # Reload integration when options change (e.g. device selection)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload integration when device selection changes."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator: YarboDataUpdateCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()
        if not hass.data.get(DOMAIN):
            hass.services.async_remove(DOMAIN, "set_nogozone_enabled")
            hass.services.async_remove(DOMAIN, "export_altitude_mesh")
            hass.services.async_remove(DOMAIN, "get_altitude_mesh")
            hass.services.async_remove(DOMAIN, "clear_altitude_area")
            hass.services.async_remove(DOMAIN, "probe_topic")
            hass.services.async_remove(DOMAIN, "probe_topics_batch")
            for name in _CRUD_SERVICE_NAMES:
                hass.services.async_remove(DOMAIN, name)
    return unload_ok


def _register_set_nogozone_enabled(hass: HomeAssistant) -> None:
    """Register the yarbo.set_nogozone_enabled service (idempotent)."""
    if hass.services.has_service(DOMAIN, "set_nogozone_enabled"):
        return

    import voluptuous as vol
    from homeassistant.core import ServiceCall
    from homeassistant.exceptions import ServiceValidationError
    from homeassistant.helpers import config_validation as cv
    from homeassistant.helpers import device_registry as dr

    schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("zone_id"): vol.Any(int, str),
        vol.Required("enabled"): cv.boolean,
    })

    async def handle(call: ServiceCall) -> None:
        device_id = call.data["device_id"]
        zone_id = call.data["zone_id"]
        enabled = call.data["enabled"]
        ha_device = dr.async_get(hass).async_get(device_id)
        if ha_device is None:
            raise ServiceValidationError(f"Device {device_id} not found")
        if not ha_device.config_entries:
            raise ServiceValidationError(
                f"Device {device_id} has no config entry"
            )
        entry_id = next(iter(ha_device.config_entries))
        coordinator = hass.data.get(DOMAIN, {}).get(entry_id)
        if coordinator is None:
            raise ServiceValidationError(
                f"Device {device_id} not managed by yarbo"
            )
        sn = next(
            (i[1] for i in ha_device.identifiers if i[0] == DOMAIN), None,
        )
        if sn is None:
            raise ServiceValidationError(
                f"Device {device_id} has no Yarbo identifier"
            )
        yarbo_dev = next(
            (d for d in coordinator.devices if d.sn == sn), None
        )
        if yarbo_dev is None:
            raise ServiceValidationError(
                f"Device {sn} not in coordinator.devices"
            )
        await coordinator.async_set_nogozone_enabled(
            sn, yarbo_dev.type_id, zone_id, enabled,
        )

    hass.services.async_register(
        DOMAIN, "set_nogozone_enabled", handle, schema=schema,
    )


def _register_export_altitude_mesh(hass: HomeAssistant) -> None:
    """Register yarbo.export_altitude_mesh — dump per-area samples to JSON.

    Writes one file per device to ``<config>/yarbo_altitude_<sn>.json``.
    Schema: ``{"areas": {"<area_id>": [[lat, lon, z_msl, ts], ...]}}``
    suitable for Delaunay triangulation downstream.
    """
    if hass.services.has_service(DOMAIN, "export_altitude_mesh"):
        return

    import json
    from pathlib import Path
    from homeassistant.core import (
        ServiceCall, ServiceResponse, SupportsResponse,
    )

    async def handle(call: ServiceCall) -> ServiceResponse:
        out_paths: dict[str, str] = {}
        stats: dict[str, dict[str, int]] = {}
        for entry_id, coord in (hass.data.get(DOMAIN) or {}).items():
            store = getattr(coord, "_altitude_store", None)
            if store is None:
                continue
            # Flush in case there are pending samples.
            try:
                await store.async_save()
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("[altitude] flush before export failed: %s", err)
            for sn, areas in store.all_data().items():
                payload = {"sn": sn, "areas": areas}
                path = Path(hass.config.path(f"yarbo_altitude_{sn}.json"))
                await hass.async_add_executor_job(
                    path.write_text,
                    json.dumps(payload, separators=(",", ":")),
                )
                out_paths[sn] = str(path)
                stats[sn] = {a: len(pts) for a, pts in areas.items()}
        return {"files": out_paths, "samples_per_area": stats}

    hass.services.async_register(
        DOMAIN, "export_altitude_mesh", handle,
        supports_response=SupportsResponse.OPTIONAL,
    )


def _register_get_altitude_mesh(hass: HomeAssistant) -> None:
    """Register yarbo.get_altitude_mesh — return per-area samples in the response.

    Card-facing: lets the frontend pull mesh data without writing files.
    Per-device payload includes the gps_ref so the card can project
    lat/lon → local meters consistently.
    """
    if hass.services.has_service(DOMAIN, "get_altitude_mesh"):
        return

    from homeassistant.core import (
        ServiceCall, ServiceResponse, SupportsResponse,
    )

    async def handle(call: ServiceCall) -> ServiceResponse:
        out: dict[str, Any] = {"devices": {}}
        for entry_id, coord in (hass.data.get(DOMAIN) or {}).items():
            store = getattr(coord, "_altitude_store", None)
            if store is None:
                continue
            for sn, areas in store.all_data().items():
                gps_ref = (coord.gps_refs.get(sn) or {}).get("ref") or {}
                out["devices"][sn] = {
                    "gps_ref": {
                        "latitude": gps_ref.get("latitude"),
                        "longitude": gps_ref.get("longitude"),
                    },
                    "areas": areas,
                }
        return out

    hass.services.async_register(
        DOMAIN, "get_altitude_mesh", handle,
        supports_response=SupportsResponse.ONLY,
    )


def _register_clear_altitude_area(hass: HomeAssistant) -> None:
    """Register yarbo.clear_altitude_area — wipe one area's mesh samples.

    Pass ``area_id="*"`` to wipe all areas for the device. Useful when
    travel-tagged areas pollute the store; the cleaning-phase gate
    introduced in coordinator should prevent recurrence going forward.
    """
    if hass.services.has_service(DOMAIN, "clear_altitude_area"):
        return

    import voluptuous as vol
    from homeassistant.core import ServiceCall
    from homeassistant.helpers import config_validation as cv

    schema = vol.Schema({
        vol.Required("area_id"): cv.string,
        vol.Optional("device_sn"): cv.string,
    })

    async def handle(call: ServiceCall) -> None:
        target_sn = call.data.get("device_sn")
        area_id = call.data["area_id"]
        for entry_id, coord in (hass.data.get(DOMAIN) or {}).items():
            store = getattr(coord, "_altitude_store", None)
            if store is None:
                continue
            for sn in list(store.all_data().keys()):
                if target_sn and sn != target_sn:
                    continue
                if area_id == "*":
                    store.clear(sn=sn)
                else:
                    store.clear(sn=sn, area_id=area_id)
            try:
                await store.async_save()
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("[altitude] clear save failed: %s", err)

    hass.services.async_register(
        DOMAIN, "clear_altitude_area", handle, schema=schema,
    )


def _register_probe_topic(hass: HomeAssistant) -> None:
    """Register yarbo.probe_topic + yarbo.probe_topics_batch.

    Diagnostic-only: publish to arbitrary ``snowbot/{sn}/app/<name>``
    topics that the SDK doesn't declare, then watch ``data_feedback``
    for any matching response. Used to discover undocumented
    command topics (e.g. plan/area writes).

    USE WITH CARE — a successful probe means the firmware accepted the
    payload, which may mutate state. Default payloads should use
    clearly fake IDs and read-only-feeling names.
    """
    if hass.services.has_service(DOMAIN, "probe_topic"):
        return

    import voluptuous as vol
    from homeassistant.core import (
        ServiceCall, ServiceResponse, SupportsResponse,
    )
    from homeassistant.exceptions import ServiceValidationError
    from homeassistant.helpers import config_validation as cv
    from homeassistant.helpers import device_registry as dr

    def _resolve(call: ServiceCall):
        device_id = call.data["device_id"]
        ha_device = dr.async_get(hass).async_get(device_id)
        if ha_device is None:
            raise ServiceValidationError(f"Device {device_id} not found")
        if not ha_device.config_entries:
            raise ServiceValidationError(
                f"Device {device_id} has no config entry"
            )
        entry_id = next(iter(ha_device.config_entries))
        coordinator = hass.data.get(DOMAIN, {}).get(entry_id)
        if coordinator is None:
            raise ServiceValidationError(
                f"Device {device_id} not managed by yarbo"
            )
        sn = next(
            (i[1] for i in ha_device.identifiers if i[0] == DOMAIN), None,
        )
        if sn is None:
            raise ServiceValidationError(
                f"Device {device_id} has no Yarbo identifier"
            )
        return coordinator, sn

    single_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("topic_name"): cv.string,
        vol.Optional("payload", default={}): dict,
        vol.Optional("timeout", default=10.0): vol.All(
            vol.Coerce(float), vol.Range(min=1, max=60),
        ),
        vol.Optional("response_topic"): cv.string,
    })

    async def handle_single(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        return await coord.async_probe_topic(
            sn=sn,
            topic_name=call.data["topic_name"],
            payload=call.data.get("payload") or {},
            timeout=call.data.get("timeout", 10.0),
            response_topic=call.data.get("response_topic"),
        )

    hass.services.async_register(
        DOMAIN, "probe_topic", handle_single,
        schema=single_schema,
        supports_response=SupportsResponse.ONLY,
    )

    batch_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("topic_names"): vol.All(cv.ensure_list, [cv.string]),
        vol.Optional("payload", default={}): dict,
        vol.Optional("timeout", default=5.0): vol.All(
            vol.Coerce(float), vol.Range(min=1, max=30),
        ),
        vol.Optional("inter_probe_delay", default=1.5): vol.All(
            vol.Coerce(float), vol.Range(min=0, max=10),
        ),
    })

    async def handle_batch(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        results: dict[str, Any] = {}
        topics = call.data["topic_names"]
        payload = call.data.get("payload") or {}
        timeout = call.data.get("timeout", 5.0)
        delay = call.data.get("inter_probe_delay", 1.5)
        import asyncio as _asyncio
        for i, name in enumerate(topics):
            try:
                results[name] = await coord.async_probe_topic(
                    sn=sn, topic_name=name, payload=payload, timeout=timeout,
                )
            except Exception as err:  # noqa: BLE001
                results[name] = {"error": str(err)}
            if i < len(topics) - 1 and delay > 0:
                await _asyncio.sleep(delay)
        # Summary view that's easy to skim in the UI.
        summary = {
            name: ("MATCHED" if r.get("matched")
                   else "no-response" if r.get("published")
                   else "publish-failed")
            for name, r in results.items()
        }
        return {"summary": summary, "detail": results}

    hass.services.async_register(
        DOMAIN, "probe_topics_batch", handle_batch,
        schema=batch_schema,
        supports_response=SupportsResponse.ONLY,
    )


# Names registered by _register_crud_services. Listed here so the
# unload path can tear them down without re-deriving the list.
_CRUD_SERVICE_NAMES: tuple[str, ...] = (
    "list_plans", "save_plan", "delete_plan",
    "list_clean_areas", "read_clean_area",
    "save_clean_area", "delete_clean_area",
    "list_nogo_zones", "save_nogo_zone", "delete_nogo_zone",
    "list_novision_zones", "save_novision_zone", "delete_novision_zone",
    "goto_waypoints",
)


def _register_crud_services(hass: HomeAssistant) -> None:
    """Register the discovered plan/area/zone CRUD topics as HA services.

    All services take a ``device_id`` to pick the target Yarbo. List/
    read services return data via ``SupportsResponse.ONLY``. Save and
    delete services return a ``{"success": bool}`` response so callers
    in scripts/automations can react to failures.

    These wrap the MQTT topics we discovered empirically; the published
    SDK doesn't expose them. See the probe scripts in /tmp/yarbo_probe*
    for the discovery work.
    """
    if hass.services.has_service(DOMAIN, "list_plans"):
        return

    import voluptuous as vol
    from homeassistant.core import (
        ServiceCall, ServiceResponse, SupportsResponse,
    )
    from homeassistant.exceptions import (
        HomeAssistantError, ServiceValidationError,
    )
    from homeassistant.helpers import config_validation as cv
    from homeassistant.helpers import device_registry as dr

    def _resolve(call: ServiceCall):
        device_id = call.data["device_id"]
        ha_device = dr.async_get(hass).async_get(device_id)
        if ha_device is None:
            raise ServiceValidationError(f"Device {device_id} not found")
        if not ha_device.config_entries:
            raise ServiceValidationError(
                f"Device {device_id} has no config entry"
            )
        entry_id = next(iter(ha_device.config_entries))
        coordinator = hass.data.get(DOMAIN, {}).get(entry_id)
        if coordinator is None:
            raise ServiceValidationError(
                f"Device {device_id} not managed by yarbo"
            )
        sn = next(
            (i[1] for i in ha_device.identifiers if i[0] == DOMAIN), None,
        )
        if sn is None:
            raise ServiceValidationError(
                f"Device {device_id} has no Yarbo identifier"
            )
        return coordinator, sn

    device_id_only = vol.Schema({vol.Required("device_id"): cv.string})
    id_int_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("id"): vol.All(vol.Coerce(int)),
    })

    # --- plans ---
    async def handle_list_plans(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        plans = await coord.async_list_plans(sn)
        if plans is None:
            raise HomeAssistantError("Timed out reading plans from mower")
        return {"plans": plans}

    hass.services.async_register(
        DOMAIN, "list_plans", handle_list_plans,
        schema=device_id_only, supports_response=SupportsResponse.ONLY,
    )

    save_plan_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Optional("id"): vol.All(vol.Coerce(int)),
        vol.Required("name"): cv.string,
        vol.Required("area_ids"): vol.All(cv.ensure_list, [vol.Coerce(int)]),
        vol.Optional("enable_self_order", default=False): cv.boolean,
    })

    async def handle_save_plan(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        plan = {
            "name": call.data["name"],
            "areaIds": call.data["area_ids"],
            "enable_self_order": call.data.get("enable_self_order", False),
        }
        if "id" in call.data:
            plan["id"] = call.data["id"]
        ok = await coord.async_save_plan(sn, plan)
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "save_plan", handle_save_plan,
        schema=save_plan_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    async def handle_delete_plan(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        plan_id = call.data["id"]
        try:
            ok = await coord.async_delete_plan(sn, plan_id)
        except ValueError as err:
            raise ServiceValidationError(str(err)) from err
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "delete_plan", handle_delete_plan,
        schema=id_int_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    # --- clean areas ---
    async def handle_list_areas(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        areas = await coord.async_list_clean_areas(sn)
        if areas is None:
            raise HomeAssistantError("Timed out reading areas")
        # Summarise — the full polygons are large; expose id/name/area only
        # by default. Callers needing geometry should use read_clean_area.
        summary = [
            {
                "id": a.get("id"),
                "name": a.get("name"),
                "area_m2": a.get("area"),
                "verts": len(a.get("range") or []),
            }
            for a in areas
        ]
        return {"areas": summary}

    hass.services.async_register(
        DOMAIN, "list_clean_areas", handle_list_areas,
        schema=device_id_only, supports_response=SupportsResponse.ONLY,
    )

    async def handle_read_area(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        area = await coord.async_read_clean_area(sn, call.data["id"])
        if area is None:
            # Some firmwares return an empty echo for read_<thing>; fall
            # back to filtering the list response.
            areas = await coord.async_list_clean_areas(sn) or []
            area = next(
                (a for a in areas if a.get("id") == call.data["id"]), None,
            )
        if area is None:
            raise HomeAssistantError(
                f"Area id={call.data['id']} not found"
            )
        return {"area": area}

    hass.services.async_register(
        DOMAIN, "read_clean_area", handle_read_area,
        schema=id_int_schema, supports_response=SupportsResponse.ONLY,
    )

    save_area_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("area"): dict,
    })

    async def handle_save_area(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        ok = await coord.async_save_clean_area(sn, call.data["area"])
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "save_clean_area", handle_save_area,
        schema=save_area_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    async def handle_delete_area(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        ok = await coord.async_delete_clean_area(sn, call.data["id"])
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "delete_clean_area", handle_delete_area,
        schema=id_int_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    # --- no-go zones ---
    async def handle_list_nogo(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        zones = await coord.async_list_nogo_zones(sn)
        if zones is None:
            raise HomeAssistantError("Timed out reading no-go zones")
        summary = [
            {
                "id": z.get("id"),
                "name": z.get("name"),
                "enable": z.get("enable"),
                "verts": len(z.get("range") or []),
            }
            for z in zones
        ]
        return {"nogo_zones": summary}

    hass.services.async_register(
        DOMAIN, "list_nogo_zones", handle_list_nogo,
        schema=device_id_only, supports_response=SupportsResponse.ONLY,
    )

    save_zone_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("zone"): dict,
    })

    async def handle_save_nogo(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        ok = await coord.async_save_nogo_zone(sn, call.data["zone"])
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "save_nogo_zone", handle_save_nogo,
        schema=save_zone_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    async def handle_delete_nogo(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        ok = await coord.async_delete_nogo_zone(sn, call.data["id"])
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "delete_nogo_zone", handle_delete_nogo,
        schema=id_int_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    # --- no-vision zones ---
    async def handle_list_novision(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        zones = await coord.async_list_novision_zones(sn)
        if zones is None:
            raise HomeAssistantError("Timed out reading no-vision zones")
        summary = [
            {
                "id": z.get("id"),
                "name": z.get("name"),
                "enable": z.get("enable"),
                "by_pass_level": z.get("by_pass_level"),
                "verts": len(z.get("range") or []),
            }
            for z in zones
        ]
        return {"novision_zones": summary}

    hass.services.async_register(
        DOMAIN, "list_novision_zones", handle_list_novision,
        schema=device_id_only, supports_response=SupportsResponse.ONLY,
    )

    async def handle_save_novision(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        ok = await coord.async_save_novision_zone(sn, call.data["zone"])
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "save_novision_zone", handle_save_novision,
        schema=save_zone_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    async def handle_delete_novision(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        ok = await coord.async_delete_novision_zone(sn, call.data["id"])
        return {"success": ok}

    hass.services.async_register(
        DOMAIN, "delete_novision_zone", handle_delete_novision,
        schema=id_int_schema, supports_response=SupportsResponse.OPTIONAL,
    )

    # --- goto_waypoints (start_way_point) ---
    waypoints_schema = vol.Schema({
        vol.Required("device_id"): cv.string,
        vol.Required("points"): vol.All(
            cv.ensure_list,
            [vol.Schema({
                vol.Required("x"): vol.Coerce(float),
                vol.Required("y"): vol.Coerce(float),
                vol.Optional("phi", default=0.0): vol.Coerce(float),
            }, extra=vol.ALLOW_EXTRA)],
        ),
        vol.Optional("type", default=0): vol.All(
            vol.Coerce(int), vol.In([0, 1, 2]),
        ),
        vol.Optional("wake", default=True): cv.boolean,
    })

    async def handle_goto_waypoints(call: ServiceCall) -> ServiceResponse:
        coord, sn = _resolve(call)
        points = [
            {"x": float(p["x"]), "y": float(p["y"]),
             "phi": float(p.get("phi", 0.0))}
            for p in call.data["points"]
        ]
        try:
            await coord.async_goto_waypoints(
                sn=sn, points=points,
                type_hint=call.data.get("type", 0),
                wake=call.data.get("wake", True),
            )
        except ValueError as err:
            raise ServiceValidationError(str(err)) from err
        return {"success": True, "points_sent": len(points)}

    hass.services.async_register(
        DOMAIN, "goto_waypoints", handle_goto_waypoints,
        schema=waypoints_schema,
        supports_response=SupportsResponse.OPTIONAL,
    )
