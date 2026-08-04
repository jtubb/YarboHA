"""Data coordinator for Yarbo integration — MQTT push only, no polling."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import CALLBACK_TYPE, HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, HomeAssistantError
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util
from yarbo_robot_sdk import (
    AuthenticationError,
    TokenExpiredError,
    YarboClient,
    YarboSDKError,
)
from yarbo_robot_sdk.device_helpers import convert_map_to_geojson

from .const import (
    CONF_KEEP_AWAKE_MODE,
    CONF_SCHEDULES,
    CONF_SELECTED_DEVICES,
    CONF_ZONE_RULES,
    DATA_ACCESS_TOKEN,
    DATA_REFRESH_TOKEN,
    DOMAIN,
    EVENT_PLAN_FINISHED,
    EVENT_PLAN_STARTED,
    EVENT_QUIET_HOURS_STOP,
    EVENT_ZONE_RULE_ENGAGED,
    EVENT_ZONE_RULE_RELEASED,
    EVENT_ZONE_RULE_THRESHOLD,
    KEEP_AWAKE_ALWAYS,
    KEEP_AWAKE_DOCKED,
    KEEP_AWAKE_OFF,
    SCHEDULER_TICK_SECONDS,
)
from .mqtt_recorder import MqttRecorder
from .scheduler import (
    Evaluation,
    GateInputs,
    RobotSnapshot,
    ScheduleSpec,
    evaluate,
    is_in_sleep_window,
    spec_with_defaults,
)
from .scheduler_state import ScheduleStateStore
from . import zone_rules as _zr

_LOGGER = logging.getLogger(__name__)

def _deep_merge(target: dict, source: dict) -> bool:
    """Deep merge source into target, preserving existing nested dict values.

    For nested dicts, merges one level deep instead of replacing. Special keys
    '__online__' and 'HeartBeatMSG' in target are always preserved (not
    overwritten by device status pushes).

    Returns True if any value was added or changed, so callers can skip a
    coordinator refresh when a push carries nothing new.
    """
    changed = False
    for key, value in source.items():
        if key in ("__online__", "HeartBeatMSG"):
            continue  # Never overwrite these from device status
        if (
            key in target
            and isinstance(target[key], dict)
            and isinstance(value, dict)
        ):
            for k2, v2 in value.items():
                if k2 not in target[key] or target[key][k2] != v2:
                    target[key][k2] = v2
                    changed = True
        elif key not in target or target[key] != value:
            target[key] = value
            changed = True
    return changed


# Heartbeats arrive irregularly (~30s per spec, but observed 30–300s in the
# field), so the offline threshold must comfortably exceed the cadence or the
# device flaps online/offline. 90s ≈ 3× the spec interval.
HEARTBEAT_TIMEOUT_SECONDS = 90
HEARTBEAT_CHECK_INTERVAL = timedelta(seconds=5)
WAKEUP_RENEWAL_INTERVAL = timedelta(minutes=4)

# Persisted-map storage (survives restarts; re-fetched only on user refresh).
MAP_STORE_VERSION = 1
MAP_STORE_SAVE_DELAY = 5  # seconds, debounce writes

# Persisted user-standby preferences (so a restart doesn't wake devices the
# user explicitly put to sleep).
STANDBY_STORE_VERSION = 1




def _parse_gga_altitude(gga: object) -> float | None:
    """Extract altitude (m, MSL) from an NMEA GGA sentence.

    Field index 9 is the orthometric height per NMEA 0183. Returns
    None for any non-string input or malformed sentence — caller
    falls back to a coarser source.

        $GNGGA,hhmmss.ss,lat,N,lon,W,fix,sats,hdop,ALT,M,geoid,M,age,stn*cs
                                                ^^^ field 9
    """
    if not isinstance(gga, str) or not gga.startswith(("$GNGGA", "$GPGGA")):
        return None
    try:
        # Strip optional checksum + trailing CRLF before splitting.
        body = gga.split("*", 1)[0].strip()
        parts = body.split(",")
        if len(parts) < 11:
            return None
        return float(parts[9])
    except (TypeError, ValueError):
        return None


def _decode_map_data(raw, sn: str):
    """Decode get_map response's ``data`` field to a dict.

    Accepts a dict (normal case) or a string — some firmware serializes
    the response with ``data`` as a zlib-compressed binary blob that
    Python's json reader returns as a latin-1 str. Falls back through
    json-parse then zlib-decompress then base64+zlib.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        import json as _json
        import zlib as _zlib
        # 1) plain JSON string?
        try:
            parsed = _json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        # 2) zlib-compressed binary, bytes-as-str (latin-1)?
        try:
            decompressed = _zlib.decompress(raw.encode("latin-1"))
            parsed = _json.loads(decompressed.decode("utf-8"))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        # 3) base64-wrapped zlib?
        try:
            import base64 as _b64
            decompressed = _zlib.decompress(_b64.b64decode(raw))
            parsed = _json.loads(decompressed.decode("utf-8"))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        _LOGGER.warning(
            "Map data for %s: could not decode 'data' string (len=%d)",
            sn, len(raw),
        )
    elif raw is not None:
        _LOGGER.warning(
            "Map data for %s: unexpected data type %s",
            sn, type(raw).__name__,
        )
    return {}


def _subscribe_raw_topic(client, sn: str, topic: str, callback) -> None:
    """Subscribe to an arbitrary device push topic.

    yarbo-data-sdk >= 0.2.2 exposes the public ``subscribe_topic`` helper.
    On older SDKs it does not exist, so fall back to the private per-device
    MQTT accessor that the public helper is itself a thin wrapper around.
    Without this fallback the integration hard-depends on an SDK release and
    silently loses plan_feedback / cloud_points push when running against an
    older one.

    Blocking — callers must invoke via async_add_executor_job.
    """
    if (subscribe_topic := getattr(client, "subscribe_topic", None)) is not None:
        subscribe_topic(sn, topic, callback)
        return
    if (ensure_mqtt := getattr(client, "_ensure_mqtt_for", None)) is None:
        raise AttributeError(
            "SDK exposes neither subscribe_topic nor _ensure_mqtt_for; "
            "upgrade yarbo-data-sdk to >=0.2.2"
        )
    ensure_mqtt(sn).subscribe(topic, callback)


class YarboDataUpdateCoordinator(DataUpdateCoordinator[dict[str, dict]]):
    """Coordinate data from Yarbo SDK.

    Data channel: MQTT push (real-time) only.
    Token refresh: handled on-demand by SDK RestClient (auto-refresh on 401).
    """

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=None,  # No polling — MQTT is the only data channel
        )
        self.entry = entry
        self._client = None
        self.devices: list = []
        self._gps_refs: dict[str, dict] = {}
        self._map_data: dict[str, dict] = {}
        # Raw zone data (with enable flags, names, range points, etc.)
        # as returned by get_map. Persisted so entities can expose
        # per-zone metadata without re-fetching.
        self._map_raw: dict[str, dict] = {}
        self._plan_data: dict[str, list[dict]] = {}
        self._last_heartbeat: dict[str, float] = {}
        self._user_standby: dict[str, bool] = {}
        # Online-recovery bookkeeping for the request/response data a device can
        # miss while offline (full DeviceMSG snapshot + Wi-Fi info). ``_inflight``
        # guards against overlapping fetches (a flapping device); ``_loaded``
        # records SNs fetched at least once, so a device that missed its startup
        # fetch (offline at the time) is retried when it first comes online.
        self._device_msg_inflight: set[str] = set()
        self._device_msg_loaded: set[str] = set()
        self._wifi_inflight: set[str] = set()
        self._wifi_loaded: set[str] = set()
        self._selected_plan: dict[str, int | None] = {}
        # Live plan state (projected path, actual_clean_area, areaIds)
        # from the undocumented plan_feedback topic.
        self._plan_feedback: dict[str, dict] = {}
        # Dynamic obstacles (tmp_barrier_points) from the undocumented
        # cloud_points_feedback topic.
        self._cloud_points: dict[str, dict] = {}
        # Track the previous on_going_planning code per device so we
        # can detect "plan just ended" transitions and clear stale
        # cloud-points (obstacles) accumulated during the run.
        self._last_planning_status: dict[str, int] = {}
        # Plan id currently being run on each device. Set when we
        # start a plan (via async_start_plan) AND when we see the
        # mower transition into an active state from MQTT (so plans
        # initiated from the Yarbo app are also tracked). Cleared
        # when the run ends. Used to attribute end-based last_run
        # stamping to the correct schedule.
        self._active_plan_id: dict[str, int | None] = {}
        # Set to True for one tick cycle when quiet-hours enforcement
        # has just issued a stop_plan, so the resulting active→idle
        # transition can be reported as reason="quiet_hours_stop"
        # instead of "user_stopped". Cleared after the finished event
        # is emitted.
        self._quiet_stop_pending: dict[str, bool] = {}
        # Per-tick snow accumulation cache. Key: (weather_entity_id,
        # snow_forecast_hours). Value: estimated upcoming snow
        # precipitation (sum of forecast precipitation in periods with
        # snowy condition). Repopulated at the top of every tick so
        # the gate evaluator (which is sync) can read it.
        self._snow_forecast_cache: dict[tuple[str, int], float] = {}
        # Live position from data_feedback's read_gps_ref topic, parsed
        # from the lat_lon_hight string. Updated whenever the mower
        # broadcasts a fresh GPS fix; frozen at last good reading
        # while docked or rtk-degraded. Per-sn dict:
        #   {"lat": float, "lon": float, "height_msl": float (m),
        #    "rtk_fix_type": int, "ts": float (unix)}
        self._live_position: dict[str, dict] = {}
        # MQTT recorder — captures TX/RX to a JSONL log for diagnostics.
        # Optional (no-op if recorder failed to start). Configured in
        # async_setup, stopped in async_shutdown.
        self._recorder: MqttRecorder | None = None
        self._unsub_heartbeat_check: CALLBACK_TYPE | None = None
        self._unsub_wakeup_renewal: CALLBACK_TYPE | None = None
        # Scheduler runtime state — populated in async_setup, cleaned up
        # in async_shutdown. Tick is per-minute (cheap; just evaluates
        # in-memory state) and only fires plans when every gate passes.
        self._state_store: ScheduleStateStore | None = None
        self._unsub_scheduler_tick: CALLBACK_TYPE | None = None
        self._unsub_altitude_save: CALLBACK_TYPE | None = None
        self._altitude_store = None  # set in async_setup
        # Topic-probe machinery: per-(sn, response_topic) one-shot futures
        # that the data_feedback dispatcher resolves when a matching
        # response arrives. Used by yarbo.probe_topic to test unknown
        # command-topic names without going through the SDK's
        # control_topic resolver.
        self._probe_listeners: dict[tuple[str, str], asyncio.Future] = {}
        # When set, captures every data_feedback inner-topic seen during
        # a probe window so we can report unexpected responses too.
        self._probe_capture: dict[str, list[dict]] = {}
        # Persist last-known map + GPS reference so they survive restarts and
        # remain available while the device is offline. Re-fetched only on
        # explicit user refresh, never on a timer.
        self._map_store: Store = Store(
            hass, MAP_STORE_VERSION, f"{DOMAIN}_{entry.entry_id}_maps"
        )
        self._standby_store: Store = Store(
            hass, STANDBY_STORE_VERSION, f"{DOMAIN}_{entry.entry_id}_standby"
        )

    def _persist_maps(self) -> None:
        """Schedule a debounced save of the current map + GPS reference cache.

        Also persists the raw get_map payload (``_map_raw``) so no-go-zone
        attributes derived from it survive restarts, not just the rendered
        GeoJSON.
        """
        self._map_store.async_delay_save(
            lambda: {
                "map_data": self._map_data,
                "map_raw": self._map_raw,
                "gps_refs": self._gps_refs,
            },
            MAP_STORE_SAVE_DELAY,
        )

    async def _async_restore_maps(self) -> None:
        """Load persisted map + GPS reference cache into memory (best effort)."""
        try:
            stored = await self._map_store.async_load()
        except Exception as err:  # noqa: BLE001 - storage must never block setup
            _LOGGER.warning("Failed to restore persisted map data: %s", err)
            return
        if not stored:
            return
        self._map_data = stored.get("map_data") or {}
        self._map_raw = stored.get("map_raw") or {}
        self._gps_refs = stored.get("gps_refs") or {}
        _LOGGER.debug(
            "Restored persisted maps for %d device(s)", len(self._map_data)
        )

    async def async_setup(self) -> None:
        """Initialize SDK client, restore session, connect MQTT, subscribe."""
        # Show the last-known map immediately, before any device round-trip.
        await self._async_restore_maps()
        api_url = os.environ.get("YARBO_API_BASE_URL")

        def _create_client():
            return YarboClient(api_base_url=api_url) if api_url else YarboClient()

        client = await self.hass.async_add_executor_job(_create_client)
        self._client = client

        # Try to restore session from stored tokens
        token = self.entry.data.get(DATA_ACCESS_TOKEN)
        refresh_token = self.entry.data.get(DATA_REFRESH_TOKEN)

        try:
            if token and refresh_token:
                await self.hass.async_add_executor_job(
                    client.restore_session,
                    self.entry.data[CONF_EMAIL],
                    token,
                    refresh_token,
                )
            else:
                await self.hass.async_add_executor_job(
                    client.login,
                    self.entry.data[CONF_EMAIL],
                    self.entry.data[CONF_PASSWORD],
                )
        except (AuthenticationError, TokenExpiredError) as err:
            raise ConfigEntryAuthFailed from err

        # Get device list and filter by selection
        try:
            all_devices = await self.hass.async_add_executor_job(client.get_devices)
        except TokenExpiredError as err:
            raise ConfigEntryAuthFailed from err
        except YarboSDKError as err:
            raise UpdateFailed(f"Failed to get devices: {err}") from err

        selected_sns = set(
            self.entry.options.get(CONF_SELECTED_DEVICES, [])
        )
        if selected_sns:
            self.devices = [d for d in all_devices if d.sn in selected_sns]
        else:
            self.devices = all_devices

        # MQTT diagnostics recorder. Wraps mqtt_publish_command so every
        # outbound command lands in the JSONL log alongside RX traffic.
        # Failure to start the recorder is non-fatal — integration runs
        # fine without it.
        if self.devices:
            recorder = MqttRecorder(
                storage_dir=Path(self.hass.config.path()),
                serial_number=self.devices[0].sn,
            )
            try:
                await self.hass.async_add_executor_job(recorder.start)
                self._recorder = recorder
                # Monkey-patch the SDK client's publish to fan out a TX
                # entry. Has to be a closure over the original because
                # the SDK doesn't expose a hook.
                _orig_publish = client.mqtt_publish_command
                def _publish_and_record(sn, type_id, command_topic_name, payload=None):
                    try:
                        recorder.record_tx(
                            f"app/{command_topic_name}",
                            payload if payload is not None else {},
                        )
                    except Exception:
                        pass
                    return _orig_publish(sn, type_id, command_topic_name, payload)
                client.mqtt_publish_command = _publish_and_record
            except Exception as err:
                _LOGGER.warning("MQTT recorder failed to start: %s", err)
                self._recorder = None

        # Connect MQTT and subscribe to selected devices only
        try:
            await self.hass.async_add_executor_job(client.mqtt_connect)
            for device in self.devices:
                _LOGGER.info(
                    "Subscribing MQTT for %s (type_id=%s)",
                    device.sn, device.type_id,
                )
                await self.hass.async_add_executor_job(
                    client.subscribe_device_message,
                    device.sn,
                    device.type_id,
                    self._on_device_status,
                )
                try:
                    await self.hass.async_add_executor_job(
                        client.subscribe_heart_beat,
                        device.sn,
                        device.type_id,
                        self._on_heart_beat,
                    )
                except YarboSDKError as err:
                    _LOGGER.warning(
                        "Heart beat subscription failed for %s: %s", device.sn, err
                    )
        except YarboSDKError as err:
            _LOGGER.warning("MQTT connection failed: %s", err)

        # Subscribe to data_feedback for selected devices
        for device in self.devices:
            try:
                def _feedback_dispatch(topic_str, data, _sn=device.sn):
                    if self._recorder:
                        try:
                            self._recorder.record_rx(topic_str, data)
                        except Exception:
                            pass
                    try:
                        if not isinstance(data, dict):
                            return
                        rtopic = data.get("topic")
                        # Topic-probe: snapshot every inner topic seen
                        # while a capture is active for this SN, and
                        # resolve any matching one-shot listener.
                        if _sn in self._probe_capture and rtopic:
                            self._probe_capture[_sn].append({
                                "topic": rtopic,
                                "data": data.get("data"),
                            })
                        key = (_sn, rtopic) if rtopic else None
                        if key and key in self._probe_listeners:
                            fut = self._probe_listeners.pop(key)
                            if not fut.done():
                                self.hass.loop.call_soon_threadsafe(
                                    fut.set_result, data,
                                )
                        if rtopic == "save_nogozone":
                            payload = data.get("data") or {}
                            zid = payload.get("id")
                            raw = self._map_raw.get(_sn) or {}
                            for z in raw.get("nogozones") or []:
                                if z.get("id") == zid or str(z.get("id")) == str(zid):
                                    z["enable"] = bool(payload.get("enable", True))
                                    break
                            if self.data is not None:
                                self.hass.loop.call_soon_threadsafe(
                                    self.async_set_updated_data, self.data,
                                )
                        elif rtopic == "get_plan_feedback":
                            pf = data.get("data")
                            if isinstance(pf, dict):
                                self._plan_feedback[_sn] = pf
                                if self.data is not None:
                                    self.hass.loop.call_soon_threadsafe(
                                        self.async_set_updated_data, self.data,
                                    )
                        elif rtopic == "read_gps_ref":
                            # Live position update. lat_lon_hight is a
                            # space-separated "lat lon height_msl"
                            # string; height is meters above sea level
                            # per the RTK fix. Store along with the
                            # current rtkFixType so consumers can gate
                            # on fix quality.
                            inner = data.get("data") or {}
                            llh = inner.get("lat_lon_hight")
                            rfx = inner.get("rtkFixType")
                            ref_hgt = inner.get("hgt")
                            if isinstance(llh, str):
                                parts = llh.split()
                                if len(parts) >= 3:
                                    try:
                                        live = {
                                            "lat": float(parts[0]),
                                            "lon": float(parts[1]),
                                            "height_msl": float(parts[2]),
                                            "rtk_fix_type": (
                                                int(rfx)
                                                if rfx is not None else None
                                            ),
                                            "ts": time.time(),
                                        }
                                        # Capture reference altitude
                                        # too — this is the dock's MSL
                                        # height; relative position_z
                                        # = height_msl - reference_hgt.
                                        if ref_hgt is not None:
                                            try:
                                                live["reference_hgt"] = float(ref_hgt)
                                            except (TypeError, ValueError):
                                                pass
                                        self._live_position[_sn] = live
                                        if self.data is not None:
                                            self.hass.loop.call_soon_threadsafe(
                                                self.async_set_updated_data,
                                                self.data,
                                            )
                                    except (ValueError, TypeError):
                                        pass
                    except Exception as err:
                        _LOGGER.warning(
                            "data_feedback dispatcher failed: %s", err,
                        )
                await self.hass.async_add_executor_job(
                    client.subscribe_data_feedback,
                    device.sn,
                    device.type_id,
                    _feedback_dispatch,
                )
                plan_topic = f"snowbot/{device.sn}/device/plan_feedback"

                def _on_plan_feedback(topic_str, payload, _sn=device.sn):
                    from yarbo_robot_sdk.codec import decode_mqtt_payload
                    try:
                        data = decode_mqtt_payload(payload)
                    except Exception as err:
                        _LOGGER.warning(
                            "plan_feedback decode failed for %s: %s", _sn, err,
                        )
                        return
                    if not isinstance(data, dict):
                        return
                    self._plan_feedback[_sn] = data
                    if self.data is not None:
                        self.hass.loop.call_soon_threadsafe(
                            self.async_set_updated_data, self.data,
                        )

                try:
                    # Runs entirely in the executor; the callback receives
                    # (topic_str, payload_bytes). Uses the public SDK helper
                    # when available, else an older-SDK fallback.
                    await self.hass.async_add_executor_job(
                        _subscribe_raw_topic, client, device.sn, plan_topic,
                        _on_plan_feedback,
                    )
                except Exception as err:
                    _LOGGER.warning(
                        "plan_feedback subscribe failed: %s", err,
                    )

                cloud_topic = f"snowbot/{device.sn}/device/cloud_points_feedback"

                def _on_cloud_points(topic_str, payload, _sn=device.sn):
                    from yarbo_robot_sdk.codec import decode_mqtt_payload
                    try:
                        data = decode_mqtt_payload(payload)
                    except Exception as err:
                        _LOGGER.warning(
                            "cloud_points decode failed for %s: %s", _sn, err,
                        )
                        return
                    if not isinstance(data, dict):
                        return
                    self._cloud_points[_sn] = data
                    if self.data is not None:
                        self.hass.loop.call_soon_threadsafe(
                            self.async_set_updated_data, self.data,
                        )

                try:
                    await self.hass.async_add_executor_job(
                        _subscribe_raw_topic, client, device.sn, cloud_topic,
                        _on_cloud_points,
                    )
                except Exception as err:
                    _LOGGER.warning(
                        "cloud_points subscribe failed: %s", err,
                    )
            except YarboSDKError as err:
                _LOGGER.warning(
                    "data_feedback subscription failed for %s: %s", device.sn, err
                )

        # Auto wake-up per the configured keep-awake policy. Restore persisted
        # standby preferences first so a restart doesn't wake devices the user
        # explicitly put to sleep. In "docked" mode no battery data has arrived
        # yet, so the initial wake-up is skipped; the renewal timer picks the
        # device up within one interval once charging status is known.
        await self._async_restore_standby()
        for device in self.devices:
            self._user_standby.setdefault(device.sn, False)
            if self._should_keep_awake(device.sn):
                await self._async_send_wakeup(device.sn, device.type_id)

        # Start heartbeat check timer (every 5s)
        self._unsub_heartbeat_check = async_track_time_interval(
            self.hass, self._async_check_heartbeats, HEARTBEAT_CHECK_INTERVAL
        )

        # Start wake-up renewal timer (every 4min)
        self._unsub_wakeup_renewal = async_track_time_interval(
            self.hass, self._async_renew_wakeup, WAKEUP_RENEWAL_INTERVAL
        )

        # Persist tokens (may have been refreshed during restore)
        self._update_stored_tokens()

        # ---- Migrate options-stored schedules / zone rules to subentries.
        # Older versions of this integration stored these as lists in
        # config_entry.options[CONF_SCHEDULES] / [CONF_ZONE_RULES]. The
        # subentry refactor turns each into its own HA subentry (own
        # card, native add/edit/delete UI, surgical entity teardown).
        # Migration is idempotent: re-runs are no-ops once options are
        # cleared. Done before the platforms are forwarded so the new
        # subentry-based entity setup sees the migrated state.
        await self._async_migrate_to_subentries()

        # ---- Scheduler ------------------------------------------------
        # Load persistent state (last_run timestamps, skip flags, pause
        # flags) from .storage, then start the per-minute tick.
        self._state_store = ScheduleStateStore(self.hass, self.entry.entry_id)
        await self._state_store.async_load()
        # Drop state for schedules the user has since deleted, so the
        # Store doesn't accumulate orphans across the integration's
        # lifetime. Only prunes per device we manage.
        for device in self.devices:
            known = {
                spec.get("id")
                for spec in self._iter_subentry_data("schedule")
                if spec.get("device_sn") == device.sn and spec.get("id")
            }
            removed = self._state_store.prune_unknown_schedules(device.sn, known)
            if removed:
                _LOGGER.debug(
                    "[scheduler] pruned %d orphan schedule state(s) for %s",
                    removed, device.sn,
                )
        await self._state_store.async_save()

        self._unsub_scheduler_tick = async_track_time_interval(
            self.hass,
            self._async_scheduler_tick,
            timedelta(seconds=SCHEDULER_TICK_SECONDS),
        )

        # ---- Altitude sample buffer (per-area mesh data) -------------
        from .altitude_store import AltitudeStore
        self._altitude_store = AltitudeStore(self.hass, self.entry.entry_id)
        await self._altitude_store.async_load()
        self._unsub_altitude_save = async_track_time_interval(
            self.hass,
            self._async_save_altitude,
            timedelta(seconds=30),
        )
        _LOGGER.debug(
            "[scheduler] tick started (every %ds), %d schedule(s) configured",
            SCHEDULER_TICK_SECONDS,
            len(self._iter_subentry_data("schedule")),
        )

        # Fetch initial per-device data in the background so setup returns fast.
        # Each request can block up to its timeout when a device is offline;
        # running them inline would stall (and risk cancelling) entry setup.
        self.entry.async_create_background_task(
            self.hass,
            self._async_initial_data_fetch(),
            name=f"{DOMAIN}_initial_fetch",
        )

    async def _async_initial_data_fetch(self) -> None:
        """Fetch initial snapshots for each device and publish to entities.

        DeviceMSG is fetched first because many entities are only available from
        the full snapshot; other command responses can be slower.

        Map data is intentionally NOT fetched here: it is restored from the
        persistent store on setup and only re-fetched from the device on an
        explicit user refresh (the "Refresh Map Data" button / card).
        """
        for device in self.devices:
            await self._async_fetch_device_msg(device.sn, device.type_id)
            await self._async_fetch_wifi_info(device.sn, device.type_id)
            await self._async_fetch_plans(device.sn, device.type_id)
            await self._async_fetch_gps_ref(device.sn, device.type_id)
            await self.async_refresh_plan_feedback(device.sn, device.type_id)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    # ---- MQTT callbacks ----

    def _on_device_status(self, topic: str, data: dict[str, Any]) -> None:
        """Handle MQTT real-time status push — deep merge into coordinator data.

        Real-time pushes may contain only a subset of fields within nested dicts
        (e.g. StateMSG with only changed fields). A top-level update() would
        overwrite the entire nested dict, losing fields from the initial snapshot.
        Deep merge preserves existing nested values while updating changed ones.
        """
        if self._recorder:
            try:
                self._recorder.record_rx(topic, data)
            except Exception:
                pass
        parts = topic.split("/")
        if len(parts) >= 2:
            sn = parts[1]
            # Track plan transitions so we can clear obstacles when a
            # plan ends. Active = on_going_planning > 0 and != 5
            # (5 = Completed). Anything else is an idle-ish state.
            state_msg = data.get("StateMSG") if isinstance(data, dict) else None
            if isinstance(state_msg, dict):
                planning = state_msg.get("on_going_planning")
                if planning is not None:
                    prev = self._last_planning_status.get(sn)
                    try:
                        cur = int(planning)
                    except (TypeError, ValueError):
                        cur = 0
                    self._last_planning_status[sn] = cur
                    def _is_active(p: int) -> bool:
                        return p > 0 and p != 5
                    if (
                        prev is not None
                        and _is_active(prev)
                        and not _is_active(cur)
                    ):
                        self._cloud_points.pop(sn, None)
                        # Resolve the running plan from the firmware's
                        # own plan_feedback (field "planId"), which
                        # survives mid-run auto-recharge cycles. This
                        # is more reliable than us trying to track
                        # idle→active transitions: those collapse on
                        # auto-recharge (the mower briefly hits
                        # ogp=0 and recharging>0) which our handler
                        # below classified as "end of run".
                        # Falls back to:
                        #   1. _active_plan_id (set by async_start_plan
                        #      for runs we initiated)
                        #   2. _selected_plan (last user dropdown pick)
                        plan_id = (
                            (self._plan_feedback.get(sn) or {}).get("planId")
                            or self._active_plan_id.get(sn)
                            or self._selected_plan.get(sn)
                        )
                        # Success detection. Two firmware variants
                        # observed in the wild:
                        #   (A) cur transitions 1 → 5 → 0. Code 5 is
                        #       "Completed". Some firmware emits this
                        #       briefly.
                        #   (B) cur transitions 1 → 0 directly, with
                        #       on_going_recharging becoming > 0 at the
                        #       same time (the mower auto-docks). The
                        #       user's mower (firmware seen 2026-05) does
                        #       this — never emits code 5 at all.
                        # Either pattern AND error_code == 0 = success.
                        try:
                            recharging = int(
                                state_msg.get("on_going_recharging", 0) or 0
                            )
                        except (TypeError, ValueError):
                            recharging = 0
                        try:
                            error_code = int(
                                state_msg.get("error_code", 0) or 0
                            )
                        except (TypeError, ValueError):
                            error_code = 0

                        # Mid-plan auto-recharge looks identical to a
                        # real plan-end on this firmware (ogp=0 +
                        # recharging>0, no explicit "Completed" code).
                        # Disambiguate via the firmware's own progress
                        # report: if it claims much less than full
                        # coverage, the run is paused for a refuel —
                        # not done. Skip the end handler entirely; the
                        # mower will resume and the real end-of-run
                        # transition fires later. 99% — only treat as
                        # genuine completion when the firmware reports
                        # near-total coverage; anything less is almost
                        # certainly a refuel.
                        MID_RECHARGE_PROGRESS_THRESHOLD = 99
                        is_mid_recharge = False
                        if (
                            cur == 0 and recharging > 0
                            and error_code == 0
                        ):
                            pct_now = self._compute_progress_percent(sn)
                            if 0 < pct_now < MID_RECHARGE_PROGRESS_THRESHOLD:
                                _LOGGER.info(
                                    "[scheduler] suppressing premature "
                                    "'finished' for sn=%s: progress %d%% "
                                    "below %d%% threshold — treating as "
                                    "mid-plan auto-recharge",
                                    sn, pct_now,
                                    MID_RECHARGE_PROGRESS_THRESHOLD,
                                )
                                is_mid_recharge = True

                        if is_mid_recharge:
                            # Skip the entire end-of-run block. Don't
                            # clear _active_plan_id — the run continues
                            # after the recharge.
                            pass
                        else:
                            success = (
                                error_code == 0
                                and (cur == 5 or (cur == 0 and recharging > 0))
                            )
                            # Determine human-readable reason. Order
                            # matters: quiet_hours_stop wins because it
                            # reflects why we issued the stop.
                            resume_pct_saved = 0
                            if self._quiet_stop_pending.pop(sn, False):
                                reason = "quiet_hours_stop"
                            elif success:
                                reason = "completed"
                            elif error_code != 0 or cur < 0:
                                reason = "error"
                            elif cur == 0 and recharging == 0:
                                reason = "user_stopped"
                            else:
                                reason = "unknown"

                            # A hand-stop holds the device until Resume.
                            # Only "user_stopped" qualifies: "completed",
                            # "error" and "quiet_hours_stop" are not the
                            # user reaching for the stop button, and holding
                            # on those would strand the schedule silently.
                            if reason == "user_stopped":
                                self.hass.add_job(
                                    self._async_set_manual_hold, sn, True,
                                )

                            if success:
                                if plan_id is not None:
                                    self.hass.add_job(
                                        self._async_stamp_run_for_plan,
                                        sn, plan_id,
                                    )
                            else:
                                if plan_id is not None:
                                    resume_pct_saved = self._compute_progress_percent(sn)
                                    if resume_pct_saved > 0:
                                        self.hass.add_job(
                                            self._async_save_resume_percent,
                                            sn, plan_id, resume_pct_saved,
                                        )
                            _LOGGER.warning(
                                "[scheduler] plan ended sn=%s prev=%d cur=%d "
                                "recharging=%d err=%d active_plan_id=%s "
                                "reason=%s success=%s",
                                sn, prev, cur, recharging, error_code,
                                self._active_plan_id.get(sn), reason, success,
                            )
                            # Bus event for HA automations. Fired via
                            # call_soon_threadsafe because we're on the
                            # MQTT worker thread, not the event loop.
                            self.hass.loop.call_soon_threadsafe(
                                self._fire_finished_event,
                                sn, plan_id, success, reason,
                                error_code, cur, recharging, resume_pct_saved,
                            )
                            # Run is genuinely over — clear the cached
                            # plan id; the next start refills it. (Skipped
                            # for mid-plan recharge so resume can resolve
                            # the same plan.)
                            self._active_plan_id.pop(sn, None)
            if self.data is None:
                self.data = {}
            if sn not in self.data:
                self.data[sn] = {}
            changed = _deep_merge(self.data[sn], data)
            # Altitude sampling runs on every DeviceMSG (spatial dedup in the
            # store caps growth); it must not be gated on the merge changing.
            self._maybe_record_altitude_sample(sn)
            # Only push a coordinator update (which re-runs every entity) when
            # the merge actually changed something; status pushes often repeat
            # identical fields, which would otherwise refresh entities ~constantly.
            if changed:
                self.hass.loop.call_soon_threadsafe(
                    self.async_set_updated_data, self.data
                )

    def _maybe_record_altitude_sample(self, sn: str) -> None:
        """Capture (lat, lon, z, ts) into the altitude store if
        the mower is actively running and we have a fresh fix.

        Lat/lon is derived from CombinedOdom (continuous) + gps_ref
        (static dock anchor) — fresher than RTKMSG.lan/lon which the
        firmware refreshes infrequently.

        Called from the MQTT thread on every DeviceMSG. Spatial dedup
        in the store caps storage growth, so an over-aggressive
        trigger here is harmless. Skips silently when any required
        field is unavailable.
        """
        if self._altitude_store is None:
            return
        merged = (self.data or {}).get(sn) or {}
        # Cleaning-phase gate. Records only while the mower is actively
        # mowing/clearing the area, not while transiting between zones
        # (the firmware sets cleanAreaId during transit too, which
        # would otherwise pollute the destination area with a near-
        # linear travel track).
        #   1  = Cleaning
        #   11 = Waypoint Navigation
        #   12 = Waypoint Complete
        # Codes: 2 (Calculating Route), 3 (Heading to Area), 5 (Completed)
        # and negatives (errors) are excluded.
        state_msg = merged.get("StateMSG") or {}
        try:
            planning = int(state_msg.get("on_going_planning", 0) or 0)
        except (TypeError, ValueError):
            planning = 0
        if planning not in (1, 11, 12):
            return
        # Altitude (live source).
        rbd = merged.get("rtk_base_data")
        z_msl: float | None = None
        if isinstance(rbd, dict):
            rover = rbd.get("rover")
            if isinstance(rover, dict):
                z_msl = _parse_gga_altitude(rover.get("gngga"))
        if z_msl is None:
            return
        # Planar position from CombinedOdom (continuous).
        odom = merged.get("CombinedOdom") or {}
        try:
            x = float(odom.get("x"))
            y = float(odom.get("y"))
        except (TypeError, ValueError):
            return
        # Lat/lon: always derive from CombinedOdom + gps_ref so the
        # georef is as fresh as the planar fix.
        ref = (self._gps_refs.get(sn) or {}).get("ref") or {}
        ref_lat = ref.get("latitude")
        ref_lon = ref.get("longitude")
        if ref_lat is None or ref_lon is None:
            return
        try:
            from yarbo_robot_sdk.device_helpers import convert_local_to_gps
            lat, lon = convert_local_to_gps(
                float(ref_lat), float(ref_lon), x, y,
            )
        except Exception:  # noqa: BLE001
            return
        # Area tag from plan_feedback. None = run started before
        # plan_feedback arrived (e.g. mid-restart) — bucket as
        # "_unknown".
        pf = self._plan_feedback.get(sn) or {}
        area_id = pf.get("cleanAreaId")
        if area_id is None:
            area_id = "_unknown"
        ts = time.time()
        self._altitude_store.maybe_record(
            sn, area_id, lat, lon, z_msl, ts,
        )

    async def _async_save_altitude(self, _now=None) -> None:
        """Periodic save callback — flushes only when dirty."""
        if self._altitude_store is None:
            return
        try:
            await self._altitude_store.async_save()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("[altitude] save failed: %s", err)

    def _on_heart_beat(self, topic: str, data: dict[str, Any]) -> None:
        """Handle heart beat push — update timestamp and online state.

        The device publishes a heartbeat every ~1-2s. Always refresh the
        liveness timestamp, but only push a coordinator update (which re-runs
        every entity) when something user-visible actually changed — the
        online state flipped or the HeartBeatMSG payload differs. This avoids a
        full entity refresh on every heartbeat.
        """
        if self._recorder:
            try:
                self._recorder.record_rx(topic, data)
            except Exception:
                pass
        parts = topic.split("/")
        if len(parts) >= 2:
            sn = parts[1]
            self._last_heartbeat[sn] = time.monotonic()
            if self.data is None:
                self.data = {}
            if sn not in self.data:
                self.data[sn] = {}

            was_online = self.data[sn].get("__online__")
            prev_payload = self.data[sn].get("HeartBeatMSG")
            self.data[sn]["HeartBeatMSG"] = data
            self.data[sn]["__online__"] = True

            # Re-fetch the request/response data a device may have missed while
            # offline — the full DeviceMSG snapshot and Wi-Fi info — when it
            # comes online. Triggered when we never loaded it (offline during the
            # startup fetch) or on an explicit offline→online transition (values
            # may be stale). Each fetch dedupes via its own in-flight guard, so
            # the steady-state case (already loaded, continuous heartbeats) is a
            # no-op. DeviceMSG and Wi-Fi are gated independently so a slow/missing
            # one doesn't suppress the other.
            came_online = was_online is False
            if (
                came_online or sn not in self._device_msg_loaded
            ) and sn not in self._device_msg_inflight:
                _LOGGER.info(
                    "[heart_beat] sn=%s online → re-fetch DeviceMSG", sn
                )
                self._schedule_refetch(
                    sn,
                    self._device_msg_inflight,
                    self.async_refresh_device_msg,
                    "refetch_device_msg",
                )
            if (
                came_online or sn not in self._wifi_loaded
            ) and sn not in self._wifi_inflight:
                _LOGGER.info(
                    "[heart_beat] sn=%s online → re-fetch Wi-Fi info", sn
                )
                self._schedule_refetch(
                    sn,
                    self._wifi_inflight,
                    self.async_refresh_wifi_info,
                    "refetch_wifi",
                )

            if was_online and prev_payload == data:
                return  # No user-visible change; skip the entity refresh.

            # Logged only when the payload changed (the dedup above), so this
            # stays quiet in steady state despite the 1-2s heartbeat cadence.
            _LOGGER.debug("[heart_beat] sn=%s → online, payload=%s", sn, data)
            self.hass.loop.call_soon_threadsafe(
                self.async_set_updated_data, self.data
            )

    def _schedule_refetch(self, sn, inflight, refresh, label) -> None:
        """Schedule a one-shot online-recovery re-fetch for a device.

        Called from the MQTT heartbeat thread, so it hops onto the event loop to
        spawn the fetch. The underlying ``_async_fetch_*`` owns the in-flight
        guard, so overlapping triggers (a flapping device) collapse to a single
        fetch; the cheap ``inflight`` pre-check here just avoids spawning a task
        that would no-op, which also keeps the retry cadence at the fetch's
        timeout rather than every heartbeat.

        Args:
            sn: Device serial number.
            inflight: The resource's in-flight guard set, pre-checked here.
            refresh: Coroutine function ``(sn, type_id) -> Awaitable`` to run.
            label: Short tag for the background task name.
        """
        if sn in inflight:
            return
        device = next((d for d in self.devices if d.sn == sn), None)
        if device is None:
            return
        self.hass.loop.call_soon_threadsafe(
            lambda: self.entry.async_create_background_task(
                self.hass,
                refresh(sn, device.type_id),
                name=f"{DOMAIN}_{label}_{sn}",
            )
        )

    # ---- Heartbeat online detection ----

    async def _async_check_heartbeats(self, _now=None) -> None:
        """Check heartbeat timestamps and mark devices offline if timed out."""
        if self.data is None:
            return
        now = time.monotonic()
        changed = False
        for device in self.devices:
            sn = device.sn
            last = self._last_heartbeat.get(sn)
            was_online = self.data.get(sn, {}).get("__online__")
            if last is None or (now - last) > HEARTBEAT_TIMEOUT_SECONDS:
                if was_online is not False:
                    if sn not in self.data:
                        self.data[sn] = {}
                    self.data[sn]["__online__"] = False
                    _LOGGER.debug("[heartbeat_check] sn=%s → offline", sn)
                    changed = True
        if changed:
            self.async_set_updated_data(self.data)

    # ---- Auto wake-up and renewal ----

    def bound_device(self, sn: str):
        """Return a BoundYarboDevice for sn, with the current coordinator data snapshot.

        The bound device injects sn and type_id automatically so callers do not
        need to pass them.  Pass the current data so SDK-side head_type validation
        works when applicable.

        Falls back to raw client.mqtt_publish_command if the device is not found.
        """
        device = next((d for d in self.devices if d.sn == sn), None)
        if device is None or self._client is None:
            return None
        data = (self.data or {}).get(sn)
        return self._client.device(device, data=data)

    async def _async_send_wakeup(self, sn: str, type_id: str) -> None:
        """Send set_working_state {state:1, source:smart_home} to wake device."""
        if self._client is None:
            return
        try:
            bound = self.bound_device(sn)
            if bound is not None:
                await self.hass.async_add_executor_job(bound.core.set_working_state, 1)
            else:
                # Fallback to raw API if device not in registry yet
                await self.hass.async_add_executor_job(
                    self._client.mqtt_publish_command,
                    sn, type_id, "set_working_state",
                    {"state": 1, "source": "smart_home"},
                )
            _LOGGER.debug("[wakeup] Sent wake-up to %s", sn)
        except Exception as err:
            _LOGGER.warning("Failed to send wake-up to %s: %s", sn, err)

    @property
    def keep_awake_mode(self) -> str:
        """Current keep-awake policy from the config entry options."""
        return self.entry.options.get(CONF_KEEP_AWAKE_MODE, KEEP_AWAKE_ALWAYS)

    def _is_charging(self, sn: str) -> bool:
        """Whether the device reports charging (BatteryMSG.status > 1)."""
        data = (self.data or {}).get(sn) or {}
        status = (data.get("BatteryMSG") or {}).get("status")
        return isinstance(status, (int, float)) and status > 1

    def _should_keep_awake(self, sn: str) -> bool:
        """Whether the keep-awake policy says to renew the wake-up for sn."""
        if self._user_standby.get(sn, False):
            return False
        mode = self.keep_awake_mode
        if mode == KEEP_AWAKE_OFF:
            return False
        if mode == KEEP_AWAKE_DOCKED:
            return self._is_charging(sn)
        return True

    async def _async_renew_wakeup(self, _now=None) -> None:
        """Renew wake-up per the keep-awake policy (called every 4min)."""
        for device in self.devices:
            if self._should_keep_awake(device.sn):
                await self._async_send_wakeup(device.sn, device.type_id)

    def set_user_standby(self, sn: str, is_standby: bool) -> None:
        """Mark whether the user has manually set a device to standby."""
        self._user_standby[sn] = is_standby
        _LOGGER.debug("[standby] sn=%s standby=%s", sn, is_standby)
        self._standby_store.async_delay_save(
            lambda: dict(self._user_standby), MAP_STORE_SAVE_DELAY
        )

    async def _async_restore_standby(self) -> None:
        """Load persisted user-standby preferences (best effort)."""
        try:
            stored = await self._standby_store.async_load()
        except Exception as err:  # noqa: BLE001 - storage must never block setup
            _LOGGER.warning("Failed to restore standby preferences: %s", err)
            return
        if stored:
            self._user_standby.update({sn: bool(v) for sn, v in stored.items()})
            _LOGGER.debug("Restored standby preferences: %s", self._user_standby)

    # ---- Plan list storage ----

    @property
    def plan_data(self) -> dict[str, list[dict]]:
        """Auto plan list per device: {sn: [{id, name, areaIds, ...}]}."""
        return self._plan_data

    def set_selected_plan(self, sn: str, plan_id: int | None) -> None:
        """Record the user's plan selection for Start Plan button."""
        self._selected_plan[sn] = plan_id

    def get_selected_plan(self, sn: str) -> int | None:
        """Get the currently selected plan ID for a device."""
        return self._selected_plan.get(sn)

    async def _async_fetch_plans(self, sn: str, type_id: str) -> None:
        """Fetch auto plan list for a device. Non-blocking on failure."""
        if self._client is None:
            return
        try:
            bound = self.bound_device(sn)
            if bound is not None:
                result = await self.hass.async_add_executor_job(
                    bound.core.read_all_plan
                )
            else:
                result = await self.hass.async_add_executor_job(
                    self._client.read_all_plan, sn, type_id
                )
            plans = result.get("data", {}).get("data", [])
            self._plan_data[sn] = plans
            _LOGGER.info("Plans for %s: %d plans loaded", sn, len(plans))
        except TimeoutError:
            _LOGGER.warning(
                "Plan list request timed out for %s. "
                "Plan selection will be unavailable.",
                sn,
            )
        except Exception as err:
            _LOGGER.warning("Failed to fetch plans for %s: %s", sn, err)

    async def async_refresh_plans(self, sn: str, type_id: str) -> None:
        """Re-fetch plan list and trigger entity update."""
        await self._async_fetch_plans(sn, type_id)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    # ---- Full DeviceMSG snapshot ----

    async def _async_fetch_device_msg(self, sn: str, type_id: str) -> None:
        """Fetch full DeviceMSG snapshot and merge into coordinator data."""
        if self._client is None:
            return
        # Dedupe concurrent fetches (startup fetch vs. an online-transition
        # retry, or a mashed Refresh button) so we never run two 20s round-trips
        # for the same device at once.
        if sn in self._device_msg_inflight:
            return
        self._device_msg_inflight.add(sn)
        try:
            bound = self.bound_device(sn)
            if bound is not None:
                result = await self.hass.async_add_executor_job(
                    bound.core.get_device_msg, 20.0
                )
            else:
                result = await self.hass.async_add_executor_job(
                    self._client.get_device_msg, sn, type_id, 20.0
                )
            msg_data = result.get("data", {})
            if self.data is None:
                self.data = {}
            if sn not in self.data:
                self.data[sn] = {}
            _deep_merge(self.data[sn], msg_data)
            self._device_msg_loaded.add(sn)
            _LOGGER.info(
                "Full DeviceMSG snapshot for %s loaded (%d top-level keys: %s)",
                sn, len(msg_data), list(msg_data.keys()),
            )
            # Debug: check specific fields
            state_msg = msg_data.get("StateMSG", {})
            _LOGGER.debug(
                "DeviceMSG snapshot StateMSG keys: %s, enable_sound=%s, volume=%s",
                list(state_msg.keys()) if isinstance(state_msg, dict) else "not-dict",
                state_msg.get("enable_sound") if isinstance(state_msg, dict) else "N/A",
                state_msg.get("volume") if isinstance(state_msg, dict) else "N/A",
            )
        except TimeoutError:
            _LOGGER.warning(
                "DeviceMSG request timed out for %s. "
                "Using real-time push data only.",
                sn,
            )
        except Exception as err:
            _LOGGER.warning("Failed to fetch DeviceMSG for %s: %s", sn, err)
        finally:
            self._device_msg_inflight.discard(sn)

    async def async_refresh_device_msg(self, sn: str, type_id: str) -> None:
        """Re-fetch full DeviceMSG snapshot and trigger entity update."""
        await self._async_fetch_device_msg(sn, type_id)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    # ---- Wi-Fi info ----

    async def _async_fetch_wifi_info(self, sn: str, type_id: str) -> None:
        """Fetch connected Wi-Fi info and merge it into coordinator data."""
        if self._client is None:
            return
        # Dedupe concurrent fetches (startup fetch vs. an online-transition
        # retry, or a mashed Refresh button) — same guard as DeviceMSG.
        if sn in self._wifi_inflight:
            return
        self._wifi_inflight.add(sn)
        try:
            bound = self.bound_device(sn)
            if bound is not None:
                result = await self.hass.async_add_executor_job(
                    bound.core.get_connect_wifi_name, 30.0
                )
            else:
                result = await self.hass.async_add_executor_job(
                    self._client.get_connect_wifi_name, sn, type_id, 30.0
                )
            wifi_data = result.get("data", {})
            if self.data is None:
                self.data = {}
            if sn not in self.data:
                self.data[sn] = {}
            self.data[sn]["WifiInfo"] = wifi_data
            self._wifi_loaded.add(sn)
            _LOGGER.info(
                "Wi-Fi info for %s loaded (signal=%s)",
                sn,
                wifi_data.get("signal") if isinstance(wifi_data, dict) else None,
            )
        except TimeoutError:
            _LOGGER.warning(
                "Wi-Fi info request timed out for %s. "
                "Wi-Fi RSSI will be unavailable.",
                sn,
            )
        except Exception as err:
            _LOGGER.warning("Failed to fetch Wi-Fi info for %s: %s", sn, err)
        finally:
            self._wifi_inflight.discard(sn)

    async def async_refresh_wifi_info(self, sn: str, type_id: str) -> None:
        """Re-fetch connected Wi-Fi info and trigger entity update."""
        await self._async_fetch_wifi_info(sn, type_id)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    # ---- GPS reference ----

    @property
    def gps_refs(self) -> dict[str, dict]:
        """GPS reference origins per device."""
        return self._gps_refs

    @property
    def live_positions(self) -> dict[str, dict]:
        """Live position broadcasts per device.

        Each entry: {lat, lon, height_msl (m), rtk_fix_type, ts}.
        Updated whenever the mower broadcasts a fresh GPS fix via the
        data_feedback ``read_gps_ref`` topic; frozen at the last good
        reading while docked or RTK-degraded. Use rtk_fix_type to gate
        consumers.
        """
        return self._live_position

    def position_z_for(self, sn: str) -> tuple[float | None, float | None]:
        """Return (relative_to_dock_m, msl_m) for a device.

        Live altitude source priority:
          1. ``rtk_base_data.rover.gngga`` — the rover's raw NMEA GGA
             sentence, updated by the RTK module every ~1s. Altitude
             is NMEA field 9. This is the only continuously-fresh
             source in the DeviceMSG broadcast.
          2. ``RTKMSG.hgt`` — present but the firmware refreshes it
             much less frequently than CombinedOdom; treat as a
             coarse fallback.
          3. Last on-demand ``read_gps_ref`` snapshot in
             ``_live_position`` — a one-shot.

        Reference altitude is the static dock height from the initial
        ``read_gps_ref`` (in ``_gps_refs``), with the live
        ``rtk_base_data.base.gngga`` as a fallback.
        """
        msl: float | None = None
        data = (self.data or {}).get(sn) or {}
        rbd = data.get("rtk_base_data")
        if isinstance(rbd, dict):
            rover = rbd.get("rover")
            if isinstance(rover, dict):
                msl = _parse_gga_altitude(rover.get("gngga"))
        if msl is None:
            rtk = data.get("RTKMSG")
            if isinstance(rtk, dict):
                try:
                    hgt = rtk.get("hgt")
                    if hgt is not None:
                        msl = float(hgt)
                except (TypeError, ValueError):
                    pass
        if msl is None:
            live = self._live_position.get(sn) or {}
            try:
                hm = live.get("height_msl")
                if hm is not None:
                    msl = float(hm)
            except (TypeError, ValueError):
                pass
        if msl is None:
            return (None, None)
        # Reference: static dock height from initial gps_ref fetch,
        # falling back to the base station's GGA altitude.
        ref = (self._gps_refs.get(sn) or {}).get("hgt")
        if ref is None and isinstance(rbd, dict):
            base = rbd.get("base")
            if isinstance(base, dict):
                ref = _parse_gga_altitude(base.get("gngga"))
        if ref is None:
            ref = (self._live_position.get(sn) or {}).get("reference_hgt")
        if ref is None:
            return (None, msl)
        try:
            return (msl - float(ref), msl)
        except (TypeError, ValueError):
            return (None, msl)

    async def _async_fetch_gps_ref(self, sn: str, type_id: str) -> None:
        """Fetch GPS reference origin for a device. Non-blocking on failure."""
        if self._client is None:
            return
        try:
            bound = self.bound_device(sn)
            if bound is not None:
                result = await self.hass.async_add_executor_job(
                    bound.core.read_gps_ref, 30.0
                )
            else:
                result = await self.hass.async_add_executor_job(
                    self._client.read_gps_ref, sn, type_id, 30.0
                )
            gps_data = result.get("data", {})
            self._gps_refs[sn] = gps_data
            self._persist_maps()
            rtk_fix = gps_data.get("rtkFixType")
            if rtk_fix != 1:
                _LOGGER.warning(
                    "GPS reference for %s has rtkFixType=%s (not fixed). "
                    "Device tracker will be unavailable until device is "
                    "initialized via the Yarbo app.",
                    sn, rtk_fix,
                )
            else:
                ref = gps_data.get("ref", {})
                _LOGGER.info(
                    "GPS reference for %s: lat=%s, lon=%s",
                    sn, ref.get("latitude"), ref.get("longitude"),
                )
        except TimeoutError:
            _LOGGER.warning(
                "GPS reference request timed out for %s. "
                "Device tracker will be unavailable.",
                sn,
            )
        except Exception as err:
            _LOGGER.warning("Failed to fetch GPS reference for %s: %s", sn, err)

    async def async_refresh_gps_ref(self, sn: str, type_id: str) -> None:
        """Re-fetch GPS reference origin and trigger entity update."""
        await self._async_fetch_gps_ref(sn, type_id)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    # ---- Map data ----

    @property
    def map_raw(self) -> dict[str, dict]:
        """Raw get_map response per device (with enable flags, names, etc)."""
        return self._map_raw

    @property
    def plan_feedback(self) -> dict[str, dict]:
        """Latest plan_feedback payload per device (may be empty)."""
        return self._plan_feedback

    @property
    def cloud_points(self) -> dict[str, dict]:
        """Latest cloud_points_feedback (dynamic obstacles) per device."""
        return self._cloud_points

    @property
    def map_data(self) -> dict[str, dict]:
        """Map zone data per device: {sn: GeoJSON FeatureCollection}."""
        return self._map_data

    async def _async_fetch_map_data(self, sn: str, type_id: str) -> None:
        """Fetch map/zone data for a device. Non-blocking on failure."""
        if self._client is None:
            return
        try:
            result = await self.hass.async_add_executor_job(
                self._client.get_map, sn, type_id
            )
            # _decode_map_data handles dict, plain-JSON-string, zlib, and
            # base64+zlib payloads — a superset of upstream's json.loads
            # normalization — and always returns a dict.
            raw_data = _decode_map_data(result.get("data", {}), sn)
            fallback_ref = self._gps_refs.get(sn)
            geojson = convert_map_to_geojson(raw_data, fallback_ref)
            self._map_data[sn] = geojson
            self._map_raw[sn] = raw_data
            self._persist_maps()
            feature_count = len(geojson.get("features", []))
            _LOGGER.info("Map data for %s: %d features loaded", sn, feature_count)
        except TimeoutError:
            _LOGGER.warning(
                "Map data request timed out for %s. "
                "Map zones will be unavailable.",
                sn,
            )
        except Exception as err:
            _LOGGER.warning("Failed to fetch map data for %s: %s", sn, err)

    async def async_set_nogozone_enabled(
        self, sn: str, type_id: str, zone_id, enabled: bool
    ) -> None:
        """Toggle a single no-go zone's enable flag and persist it.

        Mirrors the Yarbo app: forbidden while the robot is actively
        running a plan. Re-sends the full zone payload with ``enable``
        flipped. ``save_nogozone`` isn't in the SDK's control_topics
        allow-list, so we encode + publish directly.
        """
        from homeassistant.exceptions import HomeAssistantError

        if self._client is None:
            raise HomeAssistantError("Yarbo client not initialised")
        # Refuse while a plan is running (mirrors app UX).
        device_data = (self.data or {}).get(sn) or {}
        state_msg = device_data.get("StateMSG") or {}
        planning = state_msg.get("on_going_planning", 0)
        if isinstance(planning, (int, float)) and planning > 0 and planning != 5:
            raise HomeAssistantError(
                "Cannot change no-go zones while a plan is running."
            )
        raw = self._map_raw.get(sn) or {}
        zone = None
        for z in raw.get("nogozones") or []:
            if z.get("id") == zone_id or str(z.get("id")) == str(zone_id):
                zone = z
                break
        if zone is None:
            raise HomeAssistantError(
                f"No-go zone {zone_id} not found in cached map"
            )
        payload = dict(zone)
        payload["enable"] = bool(enabled)
        from yarbo_robot_sdk.codec import encode_mqtt_payload, should_compress
        import json as _json

        topic = f"snowbot/{sn}/app/save_nogozone"
        fw = getattr(self._client, "_firmware_versions", {}).get(sn)
        if should_compress(fw):
            encoded = encode_mqtt_payload(payload)
        else:
            encoded = _json.dumps(payload, separators=(",", ":")).encode("utf-8")
        mqtt = getattr(self._client, "_mqtt", None)
        if mqtt is None:
            raise HomeAssistantError("MQTT broker not connected")
        await self.hass.async_add_executor_job(mqtt.publish, topic, encoded)
        # Optimistic local update so the UI reflects the change
        # immediately. The get_plan_feedback round-trip will reconfirm.
        zone["enable"] = bool(enabled)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    async def async_refresh_plan_feedback(self, sn: str, type_id: str) -> None:
        """Ask the robot for the current plan_feedback state.

        Not in the SDK's control_topics allow-list, so we publish
        directly to snowbot/<sn>/app/get_plan_feedback.
        """
        if self._client is None:
            return
        try:
            from yarbo_robot_sdk.codec import encode_mqtt_payload, should_compress
            import json as _json

            topic = f"snowbot/{sn}/app/get_plan_feedback"
            fw = getattr(self._client, "_firmware_versions", {}).get(sn)
            payload: dict = {}
            if should_compress(fw):
                encoded = encode_mqtt_payload(payload)
            else:
                encoded = _json.dumps(payload, separators=(",", ":")).encode("utf-8")
            mqtt = getattr(self._client, "_mqtt", None)
            if mqtt is None:
                return
            await self.hass.async_add_executor_job(mqtt.publish, topic, encoded)
        except Exception as err:
            _LOGGER.warning(
                "plan_feedback request failed for %s: %s", sn, err,
            )

    async def async_refresh_map_data(self, sn: str, type_id: str) -> None:
        """Re-fetch map data and trigger entity update."""
        await self._async_fetch_map_data(sn, type_id)
        if self.data is not None:
            self.async_set_updated_data(self.data)

    async def async_probe_topic(
        self,
        sn: str,
        topic_name: str,
        payload: dict,
        timeout: float = 10.0,
        response_topic: str | None = None,
    ) -> dict:
        """Probe an arbitrary app-side MQTT command topic.

        Publishes ``payload`` to ``snowbot/{sn}/app/{topic_name}`` and
        waits up to ``timeout`` seconds for a matching ``data_feedback``
        response. Bypasses the SDK's ``control_topic`` resolver so we
        can test names the SDK doesn't declare (e.g. probing for a
        plan-write topic).

        ``response_topic`` defaults to ``topic_name`` — the firmware
        convention is to mirror the command name in the reply's
        ``topic`` field. Override if you've already observed a
        different reply name for this command.

        Returns a dict:
          - ``published``: bool (the publish call itself succeeded)
          - ``matched``: bool (a feedback message with the expected
            response topic arrived inside the timeout)
          - ``response``: the matched feedback payload (or None)
          - ``feedback_in_window``: every other inner-topic seen during
            the window — useful when the firmware replies on an
            unexpected name.
          - ``topic``: the resolved MQTT topic string we published to.
        """
        if self._client is None or not self._client._mqtt:
            raise RuntimeError("MQTT client not connected")
        topic = f"snowbot/{sn}/app/{topic_name}"
        match_name = response_topic or topic_name
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        key = (sn, match_name)
        self._probe_listeners[key] = future
        self._probe_capture.setdefault(sn, [])
        captured_at_entry = len(self._probe_capture[sn])

        # SDK auto-compresses for FW >= 3.9.0. Reuse its encoder so the
        # firmware accepts the payload format.
        try:
            from yarbo_robot_sdk.codec import encode_mqtt_payload  # type: ignore
        except Exception:  # noqa: BLE001
            encode_mqtt_payload = None
        if encode_mqtt_payload is not None:
            encoded = await self.hass.async_add_executor_job(
                encode_mqtt_payload, payload,
            )
        else:
            import json as _json
            encoded = _json.dumps(payload, separators=(",", ":")).encode(
                "utf-8"
            )

        published_ok = True
        try:
            await self.hass.async_add_executor_job(
                self._client._mqtt.publish, topic, encoded,
            )
            _LOGGER.warning(
                "[probe] published → %s payload=%s", topic, payload,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("[probe] publish failed for %s: %s", topic, err)
            published_ok = False
            self._probe_listeners.pop(key, None)

        matched_payload: dict | None = None
        if published_ok:
            try:
                matched_payload = await asyncio.wait_for(future, timeout)
            except asyncio.TimeoutError:
                self._probe_listeners.pop(key, None)
        # Anything else seen during the window that wasn't our match.
        captured = self._probe_capture.get(sn) or []
        new_entries = captured[captured_at_entry:]
        # Clear capture if we hold the last reference.
        if not self._probe_listeners or all(
            k[0] != sn for k in self._probe_listeners
        ):
            self._probe_capture.pop(sn, None)
        other_feedback = [
            e for e in new_entries
            if e.get("topic") != match_name
        ]
        return {
            "topic": topic,
            "published": published_ok,
            "matched": matched_payload is not None,
            "response": matched_payload,
            "feedback_in_window": other_feedback,
        }

    # ---- Plan / area / zone CRUD wrappers --------------------------------

    async def _async_mqtt_request(
        self,
        sn: str,
        topic_name: str,
        payload: dict | None = None,
        timeout: float = 10.0,
    ) -> dict | None:
        """Thin convenience over async_probe_topic for the discovered
        topic vocabulary. Returns the *inner data* of the firmware
        echo (the ``data`` field of the matched data_feedback message),
        or None when no echo arrived inside ``timeout`` seconds.

        Used by every public CRUD helper below.
        """
        result = await self.async_probe_topic(
            sn=sn,
            topic_name=topic_name,
            payload=payload or {},
            timeout=timeout,
        )
        if not result.get("matched"):
            return None
        resp = result.get("response") or {}
        # data_feedback wraps the actual body in a "data" field.
        return resp.get("data") if isinstance(resp, dict) else None

    # ---- plans
    async def async_list_plans(self, sn: str) -> list[dict] | None:
        data = await self._async_mqtt_request(sn, "read_all_plan", {})
        if isinstance(data, dict):
            return data.get("data") or []
        if isinstance(data, list):
            return data
        return None

    async def async_save_plan(self, sn: str, plan: dict) -> bool:
        """Create-or-update a plan. ``plan`` is a bare object with at
        minimum ``name`` and ``areaIds``; an existing ``id`` updates,
        no id (or id=0) creates with the next free id assigned by the
        firmware.
        """
        return await self._async_mqtt_request(sn, "save_plan", plan) is not None

    async def async_delete_plan(self, sn: str, plan_id: int) -> bool:
        if plan_id <= 0:
            # del_plan rejects non-positive ids (verified empirically).
            raise ValueError("plan_id must be a positive integer")
        return await self._async_mqtt_request(sn, "del_plan", {"id": plan_id}) is not None

    # ---- clean areas
    async def async_list_clean_areas(self, sn: str) -> list[dict] | None:
        data = await self._async_mqtt_request(sn, "read_all_clean_area", {})
        if isinstance(data, dict):
            return data.get("data") or []
        if isinstance(data, list):
            return data
        return None

    async def async_read_clean_area(self, sn: str, area_id: int) -> dict | None:
        return await self._async_mqtt_request(
            sn, "read_clean_area", {"id": area_id},
        )

    async def async_save_clean_area(self, sn: str, area: dict) -> bool:
        return await self._async_mqtt_request(sn, "save_clean_area", area) is not None

    async def async_delete_clean_area(self, sn: str, area_id: int) -> bool:
        return await self._async_mqtt_request(
            sn, "del_clean_area", {"id": area_id},
        ) is not None

    # ---- no-go zones
    async def async_list_nogo_zones(self, sn: str) -> list[dict] | None:
        data = await self._async_mqtt_request(sn, "read_all_nogozone", {})
        if isinstance(data, dict):
            return data.get("data") or []
        if isinstance(data, list):
            return data
        return None

    async def async_save_nogo_zone(self, sn: str, zone: dict) -> bool:
        return await self._async_mqtt_request(sn, "save_nogozone", zone) is not None

    async def async_delete_nogo_zone(self, sn: str, zone_id: int) -> bool:
        return await self._async_mqtt_request(
            sn, "del_nogozone", {"id": zone_id},
        ) is not None

    # ---- no-vision zones
    async def async_list_novision_zones(self, sn: str) -> list[dict] | None:
        data = await self._async_mqtt_request(sn, "read_all_novisionzone", {})
        if isinstance(data, dict):
            return data.get("data") or []
        if isinstance(data, list):
            return data
        return None

    async def async_save_novision_zone(self, sn: str, zone: dict) -> bool:
        return await self._async_mqtt_request(sn, "save_novisionzone", zone) is not None

    async def async_delete_novision_zone(self, sn: str, zone_id: int) -> bool:
        return await self._async_mqtt_request(
            sn, "del_novisionzone", {"id": zone_id},
        ) is not None

    # ---- start_way_point (ad-hoc goto-waypoints) ----

    async def async_goto_waypoints(
        self,
        sn: str,
        points: list[dict],
        type_hint: int = 0,
        wake: bool = True,
    ) -> bool:
        """Drive the mower through a sequence of waypoints.

        Each point is ``{"x": float, "y": float, "phi": float}`` in the
        device's local CombinedOdom frame (meters / radians).

        The firmware uses planId=9999 as a sentinel for ad-hoc waypoint
        navigation — no real plan or area is engaged, no accessory runs.
        Routes are still validated against no-go zones; a path crossing
        one returns planning_status=-23 ("In No-Go Zone") and the mower
        won't move.

        ``type_hint`` is forwarded as the top-level ``type`` field. At
        meter-scale paths the three observed values (0=transit, 1=in-
        area, 2=dead-end) behave essentially identically; default 0 is
        safest. The firmware always computes its own route from the
        waypoints regardless of this hint.

        ``wake`` issues two ``set_working_state=1`` pulses first,
        mirroring the mobile app's pre-roll. Set False if you've
        already woken the device.
        """
        if not points:
            raise ValueError("points must be a non-empty list")
        from yarbo_robot_sdk.codec import encode_mqtt_payload
        if wake:
            await self.hass.async_add_executor_job(
                self._client._mqtt.publish,
                f"snowbot/{sn}/app/set_working_state",
                encode_mqtt_payload({"state": 1}),
            )
            await asyncio.sleep(0.6)
            await self.hass.async_add_executor_job(
                self._client._mqtt.publish,
                f"snowbot/{sn}/app/set_working_state",
                encode_mqtt_payload({"state": 1}),
            )
            await asyncio.sleep(0.6)
        body: dict = {"points": points}
        if type_hint is not None:
            body["type"] = int(type_hint)
        await self.hass.async_add_executor_job(
            self._client._mqtt.publish,
            f"snowbot/{sn}/app/start_way_point",
            encode_mqtt_payload(body),
        )
        return True

    # ---- Internal ----

    async def _async_update_data(self) -> dict[str, dict]:
        """No-op — MQTT is the sole data channel. Polling is disabled."""
        return self.data or {}

    def _update_stored_tokens(self) -> None:
        """Persist current tokens to config_entry if changed."""
        if self._client is None:
            return
        current_token = self._client.token
        current_refresh = self._client.refresh_token
        stored_token = self.entry.data.get(DATA_ACCESS_TOKEN)
        stored_refresh = self.entry.data.get(DATA_REFRESH_TOKEN)
        if current_token != stored_token or current_refresh != stored_refresh:
            self.hass.config_entries.async_update_entry(
                self.entry,
                data={
                    **self.entry.data,
                    DATA_ACCESS_TOKEN: current_token,
                    DATA_REFRESH_TOKEN: current_refresh,
                },
            )

    async def async_shutdown(self) -> None:
        """Clean up SDK client and timers on unload."""
        if self._unsub_heartbeat_check:
            self._unsub_heartbeat_check()
            self._unsub_heartbeat_check = None
        if self._unsub_wakeup_renewal:
            self._unsub_wakeup_renewal()
            self._unsub_wakeup_renewal = None
        if self._unsub_scheduler_tick:
            self._unsub_scheduler_tick()
            self._unsub_scheduler_tick = None
        if self._unsub_altitude_save:
            self._unsub_altitude_save()
            self._unsub_altitude_save = None
        if self._altitude_store is not None:
            try:
                await self._altitude_store.async_save()
            except Exception:  # noqa: BLE001
                pass
        if self._state_store is not None:
            # Final flush — the Store writes are atomic so even a crash
            # mid-shutdown leaves either the old or new file intact.
            try:
                await self._state_store.async_save()
            except Exception as err:
                _LOGGER.warning("[scheduler] final state save failed: %s", err)
            self._state_store = None
        if self._recorder is not None:
            try:
                await self.hass.async_add_executor_job(self._recorder.stop)
            except Exception as err:
                _LOGGER.warning("MQTT recorder stop failed: %s", err)
            self._recorder = None
        if self._client:
            await self.hass.async_add_executor_job(self._client.close)
            self._client = None

    # -----------------------------------------------------------------
    # Scheduler — public API used by entities and the per-minute tick.
    # -----------------------------------------------------------------

    @property
    def state_store(self) -> ScheduleStateStore | None:
        """Persistent runtime state. None until async_setup completes."""
        return self._state_store

    async def _async_migrate_to_subentries(self) -> None:
        """One-shot migration of options-stored schedules + zone rules.

        Older versions stored the lists at
        ``config_entry.options[CONF_SCHEDULES]`` and
        ``config_entry.options[CONF_ZONE_RULES]``. Move each to its
        own HA subentry so the user gets native add/edit/delete UI on
        the device card.

        Safe to re-run: each item's ``id`` becomes the subentry's
        ``unique_id``; we skip any item that already has a matching
        subentry. After successful migration we strip the obsolete
        keys from options so the UI stops showing them.
        """
        from homeassistant.config_entries import ConfigSubentry

        old_schedules = self.entry.options.get(CONF_SCHEDULES, []) or []
        old_rules = self.entry.options.get(CONF_ZONE_RULES, []) or []
        if not old_schedules and not old_rules:
            return

        existing_unique_ids = {
            (sub.subentry_type, sub.unique_id)
            for sub in self.entry.subentries.values()
        }
        added = 0
        for spec in old_schedules:
            if not isinstance(spec, dict):
                continue
            sid = spec.get("id")
            if not sid:
                continue
            if ("schedule", sid) in existing_unique_ids:
                continue
            sub = ConfigSubentry(
                subentry_type="schedule",
                data=spec,
                title=spec.get("plan_name") or "Schedule",
                unique_id=sid,
            )
            # NOTE: async_add_subentry is misleadingly named — it's
            # sync (despite the async_ prefix) and returns bool.
            self.hass.config_entries.async_add_subentry(self.entry, sub)
            added += 1
        for spec in old_rules:
            if not isinstance(spec, dict):
                continue
            rid = spec.get("id")
            if not rid:
                continue
            if ("zone_rule", rid) in existing_unique_ids:
                continue
            sub = ConfigSubentry(
                subentry_type="zone_rule",
                data=spec,
                title=spec.get("name") or "Zone rule",
                unique_id=rid,
            )
            self.hass.config_entries.async_add_subentry(self.entry, sub)
            added += 1

        # Clear the obsolete option keys so the UI stops surfacing
        # them and a future re-run is a no-op.
        new_options = {
            k: v
            for k, v in self.entry.options.items()
            if k not in (CONF_SCHEDULES, CONF_ZONE_RULES)
        }
        if new_options != dict(self.entry.options):
            self.hass.config_entries.async_update_entry(
                self.entry, options=new_options,
            )
        if added:
            _LOGGER.info(
                "[migration] moved %d schedule(s)+rule(s) from options to subentries",
                added,
            )

    def subentry_id_for(self, subentry_type: str, item_id: str) -> str | None:
        """Return HA's subentry_id for a schedule/rule with our internal id.

        Platforms call ``async_add_entities(..., config_subentry_id=X)``
        so HA can clean up the entities when a subentry is removed.
        Mapping is uniqueness-of-(subentry_type, our id stored in
        subentry.unique_id).
        """
        for sub in self.entry.subentries.values():
            if sub.subentry_type == subentry_type and sub.unique_id == item_id:
                return sub.subentry_id
        return None

    def _iter_subentry_data(self, subentry_type: str) -> list[dict]:
        """All config-subentry data dicts of a given type.

        Schedules and zone rules live as subentries (one HA card each)
        so users can manage them via the native subentry UI on the
        device card. Each subentry's ``data`` is the raw spec dict
        we'd previously stored in ``config_entry.options[CONF_*]``.
        """
        out: list[dict] = []
        for sub in self.entry.subentries.values():
            if sub.subentry_type == subentry_type:
                out.append(dict(sub.data))
        return out

    @property
    def schedules(self) -> list[ScheduleSpec]:
        """All configured schedules across all devices for this entry."""
        return [
            spec_with_defaults(s)
            for s in self._iter_subentry_data("schedule")
        ]

    def schedules_for(self, sn: str) -> list[ScheduleSpec]:
        """Configured schedules belonging to one device."""
        return [s for s in self.schedules if s.get("device_sn") == sn]

    async def async_set_global_enabled(self, sn: str, enabled: bool) -> None:
        """Pause / resume all schedules on one device."""
        if self._state_store is None:
            return
        self._state_store.set_global_enabled(sn, enabled)
        await self._state_store.async_save()
        if self.data is not None:
            self.async_set_updated_data(self.data)

    async def _async_set_manual_hold(self, sn: str, held: bool) -> None:
        """Hold / release the whole device after a manual Pause or stop.

        Called from the end-of-run detector (via hass.add_job, because that
        code runs on the MQTT worker thread) and from the Pause / Resume
        buttons. Idempotent: a no-op write is skipped so we do not churn the
        Store or push a pointless coordinator update.
        """
        if self._state_store is None:
            return
        if self._state_store.get_manual_hold(sn) == bool(held):
            return
        self._state_store.set_manual_hold(sn, held)
        await self._state_store.async_save()
        _LOGGER.info(
            "[scheduler] manual hold %s for %s — schedules %s",
            "SET" if held else "released", sn,
            "will not fire until Resume" if held else "may fire again",
        )
        if self.data is not None:
            self.async_set_updated_data(self.data)

    async def async_set_manual_hold(self, sn: str, held: bool) -> None:
        """Public wrapper for the Pause / Resume buttons."""
        await self._async_set_manual_hold(sn, held)

    async def async_set_schedule_enabled(
        self, sn: str, schedule_id: str, enabled: bool
    ) -> None:
        """Pause / resume a single schedule."""
        if self._state_store is None:
            return
        self._state_store.set_schedule_enabled(sn, schedule_id, enabled)
        await self._state_store.async_save()
        if self.data is not None:
            self.async_set_updated_data(self.data)

    async def async_set_skip_next(
        self, sn: str, schedule_id: str, skip: bool
    ) -> None:
        """Queue/cancel a one-shot skip for the next eligible run."""
        if self._state_store is None:
            return
        self._state_store.set_skip_next(sn, schedule_id, skip)
        await self._state_store.async_save()
        if self.data is not None:
            self.async_set_updated_data(self.data)

    def evaluate_schedule(self, spec: ScheduleSpec) -> Evaluation:
        """Return the current hold reason + next-eligible time for a schedule.

        Used by the status sensor and by the tick. Reads only in-memory
        state (coordinator.data + state store + HA states); does not
        mutate anything.
        """
        full = spec_with_defaults(spec)
        gates = self._build_gate_inputs(full)
        return evaluate(gates)

    # ---- Plan starter (shared by button + scheduler) ----

    async def async_start_plan(
        self,
        sn: str,
        plan_id: int,
        *,
        percent: int | None = None,
        triggered_by: str = "unknown",
    ) -> None:
        """Start a plan with the standard preflight checks.

        Used by both ``YarboStartPlanButton.async_press`` and the
        scheduler tick. Centralizing here means the two code paths
        cannot drift on safety checks. Raises HomeAssistantError on
        any precondition failure.

        If the started plan corresponds to a configured schedule
        (matched by device_sn + plan_name), the schedule's last_run
        timestamp is stamped — so manual runs from the existing Start
        Plan button also satisfy the cooldown.
        """
        if self._client is None:
            raise HomeAssistantError("SDK client not initialized")
        device = next((d for d in self.devices if d.sn == sn), None)
        if device is None:
            raise HomeAssistantError(f"Device {sn} not managed")

        data = self.data.get(sn, {}) if self.data else {}
        if not data.get("__online__"):
            raise HomeAssistantError("Cannot start plan: device is offline")

        # Wired charging blocks — robot literally cannot move.
        recharge_state = (data.get("BodyMsg") or {}).get("rechargeState")
        if isinstance(recharge_state, (int, float)) and recharge_state in (1, 3):
            raise HomeAssistantError(
                "Cannot start plan: device is wired charging"
            )

        # RTK signal must be Strong (4) or Medium (5). Anything else and
        # the robot can't navigate accurately.
        rtk_status = (data.get("RTKMSG") or {}).get("status")
        rtk_val = int(rtk_status) if rtk_status is not None else 0
        if rtk_val not in (4, 5):
            raise HomeAssistantError(
                "Cannot start plan: RTK/GPS signal is weak"
            )

        # Already cleaning. on_going_planning > 0 and != 5 = active.
        planning = (data.get("StateMSG") or {}).get("on_going_planning", 0)
        if isinstance(planning, (int, float)) and planning > 0 and planning != 5:
            raise HomeAssistantError(
                "Cannot start plan: a plan is already running"
            )

        # Returning to dock. on_going_recharging > 0 and != 4 = active.
        recharging = (data.get("StateMSG") or {}).get("on_going_recharging", 0)
        if (
            isinstance(recharging, (int, float))
            and recharging > 0
            and recharging != 4
        ):
            raise HomeAssistantError(
                "Cannot start plan: device is returning to charge"
            )

        payload: dict = {"id": plan_id}
        if percent is not None and percent > 0:
            payload["percent"] = int(percent)

        _LOGGER.info("Starting plan %s for %s: %s", plan_id, sn, payload)
        await self.hass.async_add_executor_job(
            self._client.mqtt_publish_command,
            sn,
            device.type_id,
            "start_plan",
            payload,
        )

        # Remember which plan we just kicked off so the end-of-run
        # detector in _on_device_status can attribute the completion
        # to the right schedule. Cleared on Cleaning → idle transition.
        # Stamping last_run is END-based: only happens on a successful
        # Completed (on_going_planning == 5) transition; cancellations
        # and errors leave the cooldown unchanged so a recovery run
        # can fire as soon as conditions allow.
        self._active_plan_id[sn] = plan_id

        # Bus event for HA automations.
        self.hass.bus.async_fire(
            EVENT_PLAN_STARTED,
            self._build_event_payload(
                sn=sn,
                plan_id=plan_id,
                extra={
                    "triggered_by": triggered_by,
                    "percent": int(percent) if percent else 0,
                },
            ),
        )

        # Send start notifications for any matching schedule.
        await self._async_notify_schedules_for_plan(
            sn, plan_id, kind="start",
            extra={"triggered_by": triggered_by, "percent": percent or 0},
        )

    async def async_start_plan_by_name(
        self,
        sn: str,
        plan_name: str,
        *,
        percent: int | None = None,
        triggered_by: str = "unknown",
    ) -> None:
        """Start a plan looked up by name in coordinator.plan_data."""
        plans = self._plan_data.get(sn, [])
        plan_id = next(
            (p["id"] for p in plans if p.get("name") == plan_name),
            None,
        )
        if plan_id is None:
            raise HomeAssistantError(
                f"Plan '{plan_name}' not found on device {sn}"
            )
        await self.async_start_plan(
            sn, plan_id, percent=percent, triggered_by=triggered_by,
        )

    # ---- Scheduler tick ----

    async def _async_scheduler_tick(self, now=None) -> None:
        """Per-minute evaluator. Fires plans whose every gate passes."""
        if self._state_store is None:
            return
        schedules = self._iter_subentry_data("schedule")

        # Always check quiet-hours violations, even when no schedules
        # are configured (defensive — if all schedules were just deleted
        # we still want to stop a running plan that's now out of bounds).
        # In practice if schedules is empty we have no quiet-hours rules
        # to enforce, but the helper handles that.
        await self._async_enforce_quiet_hours(schedules)

        if not schedules:
            return

        # Pre-fetch snow forecasts for every (weather_entity, hours)
        # pair used by an enabled schedule. Async — the evaluator
        # reads the cached value synchronously.
        await self._async_refresh_snow_forecasts(schedules)

        # Update post-hold tracking BEFORE evaluation. A schedule
        # transitions from "weather hold" → "clear" → arms a one-time
        # cleanup permission slip (when post_hold_run is enabled).
        await self._async_update_post_hold_tracking(schedules)

        # Zone rules — independent of plan schedules. Runs each tick
        # to integrate precipitation rate, engage/release zones.
        await self._async_run_zone_rules()

        managed_sns = {d.sn for d in self.devices}
        # One start per device per tick. We used to rely on the preflight's
        # `on_going_planning > 0` check to reject the second start, but that
        # value comes from the robot's MQTT StateMSG — cached telemetry that
        # cannot possibly refresh in the milliseconds between two firings in
        # the same loop. Every schedule therefore read the same stale "not
        # planning" snapshot and all of them fired (observed: three plans
        # started 7ms apart). Track what we have already started here so the
        # decision never depends on remote state catching up.
        fired_sns: set[str] = set()
        for spec_raw in schedules:
            if not isinstance(spec_raw, dict):
                continue
            sn = spec_raw.get("device_sn")
            sid = spec_raw.get("id")
            plan_name = spec_raw.get("plan_name")
            if not sn or not sid or not plan_name:
                continue
            if sn not in managed_sns:
                continue
            if sn in fired_sns:
                # A plan is already starting on this device this tick.
                # Leave the rest eligible — they get a fair look next tick,
                # by which point telemetry reflects the running plan.
                _LOGGER.debug(
                    "[scheduler] skipping '%s' on %s: another schedule "
                    "already fired for this device this tick",
                    plan_name, sn,
                )
                continue
            try:
                spec = spec_with_defaults(spec_raw)
                gates = self._build_gate_inputs(spec)
                result = evaluate(gates)
                if result.hold_reason != "eligible":
                    continue
                # Replay any saved resume_percent so we pick up where
                # the last attempt left off. None = fresh start (the
                # default after a successful Completed).
                resume_pct = (
                    self._state_store.get_schedule_state(sn, sid)["resume_percent"]
                    if self._state_store else 0
                )
                percent = resume_pct if resume_pct > 0 else None
                _LOGGER.info(
                    "[scheduler] firing '%s' on %s (schedule=%s, percent=%s)",
                    plan_name, sn, sid, percent,
                )
                await self.async_start_plan_by_name(
                    sn, plan_name, percent=percent,
                    triggered_by="scheduler",
                )
                # Claim the device only after the start actually succeeded.
                # A preflight rejection below means nothing was started, so
                # a later schedule in this tick may still legitimately try.
                fired_sns.add(sn)
                # last_run / resume_percent are managed by the end-of-run
                # detector in _on_device_status — nothing to do here.
            except HomeAssistantError as err:
                # Preflight failure — robot transient state. Skip this
                # tick; we'll try again next minute.
                _LOGGER.debug(
                    "[scheduler] preflight blocked '%s' on %s: %s",
                    plan_name, sn, err,
                )
            except Exception as err:
                # Anything else is a bug — log loudly and keep going so
                # one bad schedule can't break the whole tick.
                _LOGGER.exception(
                    "[scheduler] tick failed for schedule %s on %s: %s",
                    sid, sn, err,
                )

    # ---- Event helpers ----

    def _build_event_payload(
        self,
        *,
        sn: str,
        plan_id: int | None,
        extra: dict | None = None,
    ) -> dict:
        """Common fields for every yarbo_plan_* event.

        Resolves plan_name and matching schedule_id from current state
        so subscribers don't have to. Caller can override or add via
        ``extra``.
        """
        device = next((d for d in self.devices if d.sn == sn), None)
        plan_name = None
        if plan_id is not None:
            for p in self._plan_data.get(sn, []) or []:
                if p.get("id") == plan_id:
                    plan_name = p.get("name")
                    break
        schedule_id: str | None = None
        if plan_name:
            for spec in self._iter_subentry_data("schedule"):
                if (
                    isinstance(spec, dict)
                    and spec.get("device_sn") == sn
                    and spec.get("plan_name") == plan_name
                    and spec.get("id")
                ):
                    schedule_id = spec["id"]
                    break
        payload: dict = {
            "device_sn": sn,
            "device_name": device.name if device else None,
            "plan_id": plan_id,
            "plan_name": plan_name,
            "schedule_id": schedule_id,
        }
        if extra:
            payload.update(extra)
        return payload

    def _fire_finished_event(
        self,
        sn: str,
        plan_id: int | None,
        success: bool,
        reason: str,
        error_code: int,
        planning_code: int,
        recharging_code: int,
        resume_percent_saved: int,
    ) -> None:
        """Fire EVENT_PLAN_FINISHED on the event loop.

        Called via ``hass.loop.call_soon_threadsafe`` from the MQTT
        worker thread; this method itself is loop-only.
        """
        self.hass.bus.async_fire(
            EVENT_PLAN_FINISHED,
            self._build_event_payload(
                sn=sn,
                plan_id=plan_id,
                extra={
                    "success": success,
                    "reason": reason,
                    "error_code": error_code,
                    "planning_code": planning_code,
                    "recharging_code": recharging_code,
                    "resume_percent_saved": resume_percent_saved,
                },
            ),
        )
        # Send completion notifications for any matching schedule.
        # Done as a fire-and-forget task because we're already on the
        # loop and the notify services are async.
        self.hass.async_create_task(
            self._async_notify_schedules_for_plan(
                sn, plan_id, kind="complete",
                extra={
                    "success": success,
                    "reason": reason,
                    "error_code": error_code,
                    "resume_percent_saved": resume_percent_saved,
                },
            )
        )

    # ---- Notification dispatcher ----

    async def _async_notify_schedules_for_plan(
        self,
        sn: str,
        plan_id: int | None,
        *,
        kind: str,        # "start" | "complete"
        extra: dict,
    ) -> None:
        """Fire HA notify services configured on schedules matching the plan.

        Iterates schedules whose plan_name matches the just-(un)started
        plan, and for each, calls every entry in
        ``pre_run_notify_target`` (kind="start") or
        ``complete_notify_target`` (kind="complete"). Service names are
        the ``notify.<name>`` strings the user picked in the form.

        Failures are logged and swallowed — one bad service shouldn't
        block the others or the rest of the lifecycle.
        """
        if plan_id is None:
            return
        plan_name = next(
            (p["name"] for p in self._plan_data.get(sn, []) or []
             if p.get("id") == plan_id),
            None,
        )
        if not plan_name:
            return
        device = next((d for d in self.devices if d.sn == sn), None)
        device_name = device.name if device else sn
        # The ScheduleSpec stores the targets as lists in current data
        # but legacy storage may have a string — coerce on read.
        for spec in self._iter_subentry_data("schedule"):
            if spec.get("device_sn") != sn or spec.get("plan_name") != plan_name:
                continue
            field = (
                "pre_run_notify_target" if kind == "start"
                else "complete_notify_target"
            )
            raw_targets = spec.get(field) or []
            if isinstance(raw_targets, str):
                raw_targets = [raw_targets] if raw_targets else []
            targets = [t for t in raw_targets if t]
            if not targets:
                continue
            title, message = self._build_notify_text(
                kind=kind, plan_name=plan_name, device_name=device_name,
                extra=extra,
            )
            for target in targets:
                if not target.startswith("notify."):
                    _LOGGER.debug(
                        "[notify] skipping non-notify target %r", target,
                    )
                    continue
                service = target.split(".", 1)[1]
                try:
                    await self.hass.services.async_call(
                        "notify", service,
                        {"title": title, "message": message},
                        blocking=False,
                    )
                except Exception as err:
                    _LOGGER.warning(
                        "[notify] %s failed: %s", target, err,
                    )

    # Human-friendly mappings for programmatic strings that end up in
    # notification message text. The internal values stay in bus
    # events for automation filtering; only the user-facing message
    # gets the display version.
    _TRIGGERED_BY_LABELS = {
        "scheduler": "scheduler",
        "schedule_run_now_button": "Run Now",
        "start_plan_button": "the Start Plan button",
        "external": "the Yarbo app or another source",
        "unknown": "an unknown source",
    }
    _REASON_LABELS = {
        "completed": "finished cleanly",
        "user_stopped": "stopped manually",
        "error": "ended with an error",
        "quiet_hours_stop": "stopped because quiet hours started",
        "unknown": "ended for an unknown reason",
    }

    def _build_notify_text(
        self,
        *,
        kind: str,
        plan_name: str,
        device_name: str,
        extra: dict,
    ) -> tuple[str, str]:
        """Compose the (title, message) for a start or complete notification.

        Hand-tuned defaults — single line, includes the most useful
        context available. Users wanting custom message templates can
        listen for the ``yarbo_plan_started`` / ``yarbo_plan_finished``
        bus events and write their own automation.
        """
        if kind == "start":
            triggered_raw = extra.get("triggered_by") or "unknown"
            triggered = self._TRIGGERED_BY_LABELS.get(
                triggered_raw, triggered_raw,
            )
            percent = int(extra.get("percent") or 0)
            title = f"Yarbo: {plan_name} starting"
            parts = [f"{plan_name} starting on {device_name}"]
            parts.append(f"(triggered by {triggered})")
            if percent > 0:
                parts.append(f"— resuming from {percent}%")
            return title, " ".join(parts)
        # kind == "complete"
        success = bool(extra.get("success"))
        reason_raw = extra.get("reason") or "unknown"
        reason = self._REASON_LABELS.get(reason_raw, reason_raw)
        if success:
            title = f"Yarbo: {plan_name} finished ✓"
            return title, f"{plan_name} finished successfully on {device_name}."
        # non-success
        title = f"Yarbo: {plan_name} {reason}"
        bits = [f"{plan_name} {reason} on {device_name}."]
        ec = int(extra.get("error_code") or 0)
        if ec:
            bits.append(f"(error code {ec})")
        rps = int(extra.get("resume_percent_saved") or 0)
        if rps > 0:
            bits.append(f"Saved {rps}% for resume.")
        return title, " ".join(bits)

    # ---- Snow forecast ----

    # Weather conditions we sum precipitation under to estimate
    # snowfall. Other "snowy" labels emitted by various weather
    # providers should land here too — extend as needed.
    _SNOW_CONDITIONS = frozenset({
        "snowy", "snowy-rainy", "snow", "lightning-snowy",
    })

    async def _async_refresh_snow_forecasts(self, schedules: list) -> None:
        """Recompute the per-tick snow_estimate cache.

        Iterates unique (weather_entity, snow_forecast_hours) pairs,
        calls weather.get_forecasts for each, and sums precipitation
        in periods whose condition is one of _SNOW_CONDITIONS within
        the window. Result lives in self._snow_forecast_cache and is
        consumed by _build_gate_inputs.

        Failures are silently treated as "no estimate" — the gate then
        fails open (doesn't hold). Better than silently holding an
        active snowblower schedule because of a transient weather
        provider hiccup.
        """
        self._snow_forecast_cache = {}
        keys: set[tuple[str, int]] = set()
        for spec_raw in schedules:
            if not isinstance(spec_raw, dict):
                continue
            we = spec_raw.get("weather_entity") or ""
            try:
                hrs = int(spec_raw.get("snow_forecast_hours", 12) or 12)
            except (TypeError, ValueError):
                hrs = 12
            try:
                threshold = float(spec_raw.get("min_snow_accumulation", 0) or 0)
            except (TypeError, ValueError):
                threshold = 0.0
            if we and threshold > 0 and hrs > 0:
                keys.add((we, hrs))
        for (entity_id, hrs) in keys:
            estimate = await self._async_compute_snow_forecast(entity_id, hrs)
            self._snow_forecast_cache[(entity_id, hrs)] = estimate

    async def _async_compute_snow_forecast(
        self, weather_entity_id: str, hours: int,
    ) -> float | None:
        """Sum forecast precipitation in snowy periods over `hours`.

        Returns None on any failure. Uses HA's weather.get_forecasts
        service, which is the supported public API across all weather
        integrations as of 2024+.
        """
        try:
            response = await self.hass.services.async_call(
                "weather",
                "get_forecasts",
                {"type": "hourly", "entity_id": weather_entity_id},
                blocking=True,
                return_response=True,
            )
        except Exception as err:
            _LOGGER.debug(
                "[scheduler] snow forecast unavailable for %s: %s",
                weather_entity_id, err,
            )
            return None
        if not response:
            return None
        forecast = (response.get(weather_entity_id) or {}).get("forecast") or []
        if not forecast:
            return None
        cutoff = dt_util.utcnow() + timedelta(hours=hours)
        total = 0.0
        for period in forecast:
            try:
                ts = period.get("datetime")
                cond = (period.get("condition") or "").lower()
                precip = float(period.get("precipitation") or 0)
            except (TypeError, ValueError):
                continue
            # Filter to upcoming periods only — forecast may include
            # past entries for some integrations.
            if ts:
                try:
                    period_dt = dt_util.parse_datetime(ts)
                    if period_dt is None:
                        continue
                    if period_dt > cutoff:
                        continue
                except Exception:
                    pass
            if cond in self._SNOW_CONDITIONS and precip > 0:
                total += precip
        return total

    # ---- Post-hold tracking ----

    async def _async_update_post_hold_tracking(self, schedules: list) -> None:
        """Detect weather-hold transitions and arm post_hold_armed.

        Per schedule with post_hold_run enabled, compute the current
        "weather is held" status (without the post_hold_armed bypass).
        Compare to the prior tick's state. On True→False transition,
        set post_hold_armed = True so the next eligible window fires
        a cleanup run regardless of cooldown / weather / snow gates.
        """
        if self._state_store is None:
            return
        any_change = False
        for spec_raw in schedules:
            if not isinstance(spec_raw, dict):
                continue
            sn = spec_raw.get("device_sn")
            sid = spec_raw.get("id")
            if not sn or not sid:
                continue
            if not bool(spec_raw.get("post_hold_run", False)):
                continue

            # Did the weather gate hold this tick? Mirrors the logic
            # in evaluate() (without bypass), but standalone so we
            # can read the state before evaluation.
            we = spec_raw.get("weather_entity") or ""
            hold_states = list(spec_raw.get("weather_hold_states", []) or [])
            ws = self.hass.states.get(we) if we else None
            current_state = (
                ws.state
                if ws and ws.state not in ("unknown", "unavailable")
                else None
            )
            currently_held = bool(
                current_state and current_state in hold_states
            )

            sched_state = self._state_store.get_schedule_state(sn, sid)
            was_held = sched_state["was_in_weather_hold"]

            if was_held and not currently_held:
                # Transition from holding → clear: arm post-hold.
                self._state_store.set_post_hold_armed(sn, sid, True)
                any_change = True
                _LOGGER.info(
                    "[scheduler] '%s' post-hold armed — weather cleared",
                    spec_raw.get("plan_name"),
                )
            if currently_held != was_held:
                self._state_store.set_was_in_weather_hold(
                    sn, sid, currently_held,
                )
                any_change = True

        if any_change:
            await self._state_store.async_save()

    async def _async_enforce_quiet_hours(
        self, schedules: list,
    ) -> None:
        """Stop a running plan if its matching schedule is in quiet hours.

        Quiet hours apply to ANY run that matches a schedule's plan_name,
        regardless of how it was started (manual button, Yarbo app, the
        scheduler tick itself). Scope is intentional: a plan that
        doesn't match any schedule is never auto-stopped.

        Local time. ``is_in_sleep_window`` already handles cross-midnight
        windows, zero-width "always awake", and bad input.
        """
        if not schedules or self._client is None:
            return
        # Index schedules by (sn, plan_name) for quick lookup, picking
        # the FIRST one when multiple schedules target the same plan
        # (rare; quiet hours are usually identical anyway).
        by_plan: dict[tuple[str, str], dict] = {}
        for spec_raw in schedules:
            if not isinstance(spec_raw, dict):
                continue
            key = (spec_raw.get("device_sn"), spec_raw.get("plan_name"))
            if key[0] and key[1] and key not in by_plan:
                by_plan[key] = spec_raw

        local_now = dt_util.now()
        # Sun-mode lookup is shared across all schedules — fetch once.
        sun_elev: float | None = None
        sun_state = self.hass.states.get("sun.sun")
        if sun_state is not None:
            try:
                sun_elev = float(sun_state.attributes.get("elevation", 90))
            except (TypeError, ValueError):
                sun_elev = None

        for device in self.devices:
            sn = device.sn
            data = self.data.get(sn, {}) if self.data else {}
            state_msg = data.get("StateMSG") or {}
            try:
                planning = int(state_msg.get("on_going_planning", 0) or 0)
            except (TypeError, ValueError):
                planning = 0
            # Plan is active when planning is positive and not 5
            # (Completed). Negative codes are errors/aborts; idle states
            # don't need stopping.
            if not (planning > 0 and planning != 5):
                continue

            running_plan_id = (self._plan_feedback.get(sn) or {}).get("planId")
            if running_plan_id is None:
                # Fall back so manual runs without recent plan_feedback
                # still get protected — match by selected plan instead.
                running_plan_id = self._selected_plan.get(sn)
            if running_plan_id is None:
                continue

            plans = self._plan_data.get(sn, [])
            running_plan_name = next(
                (p["name"] for p in plans if p.get("id") == running_plan_id),
                None,
            )
            if not running_plan_name:
                continue

            spec_raw = by_plan.get((sn, running_plan_name))
            if spec_raw is None:
                continue  # No matching schedule = no quiet hours apply.

            spec = spec_with_defaults(spec_raw)
            in_window = is_in_sleep_window(
                local_now.time(),
                spec["sleep_start"],
                spec["sleep_end"],
            )
            if (
                not in_window
                and spec["use_sun_for_sleep"]
                and sun_elev is not None
                and sun_elev < spec["sun_elevation_threshold"]
            ):
                in_window = True

            if not in_window:
                continue

            _LOGGER.warning(
                "[scheduler] '%s' is running on %s during quiet hours "
                "(%s\u2013%s%s) — sending stop_plan",
                running_plan_name, sn,
                spec["sleep_start"], spec["sleep_end"],
                " + sun" if spec["use_sun_for_sleep"] else "",
            )
            try:
                await self.hass.async_add_executor_job(
                    self._client.mqtt_publish_command,
                    sn, device.type_id, "stop_plan", {},
                )
            except Exception as exc:
                _LOGGER.warning(
                    "[scheduler] stop_plan during quiet-hours failed: %s",
                    exc,
                )
                continue
            # Mark so the resulting active→idle transition is reported
            # as reason="quiet_hours_stop" in the finished event.
            self._quiet_stop_pending[sn] = True
            self.hass.bus.async_fire(
                EVENT_QUIET_HOURS_STOP,
                self._build_event_payload(
                    sn=sn,
                    plan_id=running_plan_id,
                    extra={
                        "schedule_id": spec.get("id"),
                        "sleep_window": (
                            f'{spec["sleep_start"]}\u2013{spec["sleep_end"]}'
                        ),
                    },
                ),
            )

    # ---- Zone rules ----

    def zone_rules(self) -> list[dict]:
        return self._iter_subentry_data("zone_rule")

    def zone_rules_for(self, sn: str) -> list[dict]:
        return [r for r in self.zone_rules() if r.get("device_sn") == sn]

    def _zone_rule_state(self, rule_id: str) -> dict:
        """Read per-rule runtime state from the same Store the scheduler uses,
        under a distinct namespace so it doesn't collide with schedule state.
        """
        if self._state_store is None:
            return _zr.state_with_defaults(None)
        # Reach into the Store's data dict directly. The ScheduleStateStore
        # API was scheduler-shaped; expose zone state via the same file
        # under a separate top-level key.
        zr_states = self._state_store._data.setdefault("zone_rule_states", {})
        return _zr.state_with_defaults(zr_states.get(rule_id))

    def _set_zone_rule_state(self, rule_id: str, new_state: dict) -> None:
        if self._state_store is None:
            return
        zr_states = self._state_store._data.setdefault("zone_rule_states", {})
        zr_states[rule_id] = new_state

    def _live_zone_enable_for_rule(
        self, sn: str, zone_ids: list[int],
    ) -> dict[str, bool]:
        """Return current enable flag of each zone_id from the cached
        map data. Missing zones are absent from the dict (decider
        treats that as 'disappeared')."""
        raw = (self._map_raw.get(sn) or {})
        zones = raw.get("nogozones") or []
        index = {str(z.get("id")): bool(z.get("enable", True)) for z in zones}
        return {
            str(z): index[str(z)]
            for z in zone_ids
            if str(z) in index
        }

    async def _async_run_zone_rules(self) -> None:
        """Tick every configured zone rule. Apply zone toggles + fire events."""
        rules = self.zone_rules()
        if not rules or self._state_store is None:
            return
        managed_sns = {d.sn for d in self.devices}
        any_state_change = False
        now_ts = dt_util.utcnow().timestamp()
        for rule_raw in rules:
            if not isinstance(rule_raw, dict):
                continue
            sn = rule_raw.get("device_sn")
            rid = rule_raw.get("id")
            if not sn or not rid or sn not in managed_sns:
                continue
            spec = _zr.spec_with_defaults(rule_raw)
            state = self._zone_rule_state(rid)
            # Sample the rate (precipitation trigger only — presence
            # ignores it).
            rate, available = self._read_rate(spec.get("rate_entity") or "")
            presence_home = False
            if spec.get("trigger_type") == "presence":
                presence_home = self._any_entity_home(
                    spec.get("presence_entities") or [],
                )
            inputs = _zr.TickInputs(
                spec=spec,
                state=state,
                now_ts=now_ts,
                rate=rate,
                rate_available=available,
                live_zone_enable=self._live_zone_enable_for_rule(
                    sn, spec.get("zone_ids", []),
                ),
                presence_home=presence_home,
            )
            try:
                result = _zr.tick(inputs)
            except Exception as err:
                _LOGGER.exception(
                    "[zone_rule] tick failed for %s on %s: %s",
                    rid, sn, err,
                )
                continue

            # Apply zone toggles. Any failure here logs but doesn't stop
            # the loop — better to keep other rules functioning.
            for z in result.action.enable_zones:
                await self._async_zone_set(sn, z, True)
            for z in result.action.disable_zones:
                await self._async_zone_set(sn, z, False)

            # Fire events.
            if result.action.fire_engaged_event:
                self.hass.bus.async_fire(
                    EVENT_ZONE_RULE_ENGAGED,
                    self._build_zone_event_payload(spec, state),
                )
            if result.action.fire_threshold_event:
                self.hass.bus.async_fire(
                    EVENT_ZONE_RULE_THRESHOLD,
                    self._build_zone_event_payload(spec, state),
                )
            if result.action.fire_released_event:
                self.hass.bus.async_fire(
                    EVENT_ZONE_RULE_RELEASED,
                    self._build_zone_event_payload(spec, state),
                )

            # Persist new state.
            if result.new_state != state:
                self._set_zone_rule_state(rid, result.new_state)
                any_state_change = True

        if any_state_change:
            await self._state_store.async_save()
            if self.data is not None:
                self.async_set_updated_data(self.data)

    def _read_rate(self, entity_id: str) -> tuple[float, bool]:
        """Read a numeric rate from a HA entity. Returns (value, available)."""
        if not entity_id:
            return (0.0, False)
        st = self.hass.states.get(entity_id)
        if st is None or st.state in ("unknown", "unavailable", None, ""):
            return (0.0, False)
        try:
            return (float(st.state), True)
        except (TypeError, ValueError):
            return (0.0, False)

    def _any_entity_home(self, entity_ids: list[str]) -> bool:
        """True if any listed person/device_tracker/zone is in 'home' state."""
        for ent in entity_ids or []:
            st = self.hass.states.get(ent)
            if st is not None and st.state == "home":
                return True
        return False

    async def _async_zone_set(self, sn: str, zone_id: int, enabled: bool) -> None:
        """Send the no-go zone enable toggle. Logs on failure."""
        device = next((d for d in self.devices if d.sn == sn), None)
        if device is None:
            return
        try:
            await self.async_set_nogozone_enabled(
                sn, device.type_id, zone_id, enabled,
            )
        except Exception as err:
            _LOGGER.warning(
                "[zone_rule] zone toggle failed sn=%s zone=%s enabled=%s: %s",
                sn, zone_id, enabled, err,
            )

    def _build_zone_event_payload(self, spec: dict, state: dict) -> dict:
        device = next(
            (d for d in self.devices if d.sn == spec.get("device_sn")), None,
        )
        return {
            "rule_id": spec.get("id"),
            "rule_name": spec.get("name"),
            "device_sn": spec.get("device_sn"),
            "device_name": device.name if device else None,
            "zone_ids": list(spec.get("zone_ids", []) or []),
            "rate_entity": spec.get("rate_entity"),
            "event_threshold": float(spec.get("event_threshold", 0)),
            "duration_hours": float(spec.get("duration_hours", 0)),
            "accumulator": float(state.get("accumulator", 0) or 0),
            "expires_at": state.get("expires_at"),
        }

    # ---- Helpers ----

    async def _async_stamp_run_for_plan(self, sn: str, plan_id: int) -> None:
        """Update last_run for any schedule whose plan_name resolves to plan_id."""
        if self._state_store is None:
            return
        plans = self._plan_data.get(sn, [])
        plan_name = next(
            (p["name"] for p in plans if p.get("id") == plan_id),
            None,
        )
        if not plan_name:
            return
        now = dt_util.utcnow()
        changed = False
        for spec in self._iter_subentry_data("schedule"):
            if (
                isinstance(spec, dict)
                and spec.get("device_sn") == sn
                and spec.get("plan_name") == plan_name
                and spec.get("id")
            ):
                self._state_store.record_run(sn, spec["id"], now)
                changed = True
        if changed:
            await self._state_store.async_save()
            if self.data is not None:
                self.async_set_updated_data(self.data)

    def _compute_progress_percent(self, sn: str) -> int:
        """Estimate %completion of the plan that's about to end.

        Reads from the most recent ``plan_feedback`` payload (held in
        ``coordinator._plan_feedback``). Uses ``finishCleanArea /
        totalCleanArea`` because that's the area Yarbo considers
        "definitively cleaned" — passing this as ``percent`` to
        start_plan tells the firmware to skip ahead by that fraction
        when resuming.

        Returns 0 when the data isn't present or the math doesn't make
        sense (no totalCleanArea, finish exceeds total, etc.). The
        caller should treat 0 as "no resume info, start fresh".
        """
        pf = self._plan_feedback.get(sn) or {}
        try:
            total = float(pf.get("totalCleanArea") or 0)
            finish = float(pf.get("finishCleanArea") or 0)
        except (TypeError, ValueError):
            return 0
        if total <= 0 or finish <= 0:
            return 0
        ratio = finish / total
        if ratio <= 0 or ratio >= 1:
            return 0
        # Clamp to [1, 99]: 0 is "no resume", 100 is "complete (use
        # record_run instead)".
        return max(1, min(99, int(round(ratio * 100))))

    async def _async_save_resume_percent(
        self, sn: str, plan_id: int, percent: int,
    ) -> None:
        """Stash a resume-from percent for every matching schedule."""
        if self._state_store is None:
            return
        plans = self._plan_data.get(sn, [])
        plan_name = next(
            (p["name"] for p in plans if p.get("id") == plan_id),
            None,
        )
        if not plan_name:
            return
        changed = False
        for spec in self._iter_subentry_data("schedule"):
            if (
                isinstance(spec, dict)
                and spec.get("device_sn") == sn
                and spec.get("plan_name") == plan_name
                and spec.get("id")
            ):
                self._state_store.set_resume_percent(sn, spec["id"], percent)
                changed = True
        if changed:
            _LOGGER.info(
                "[scheduler] '%s' on %s ended at %d%%; saved for resume",
                plan_name, sn, percent,
            )
            await self._state_store.async_save()
            if self.data is not None:
                self.async_set_updated_data(self.data)

    def _derive_robot_snapshot(self, sn: str) -> RobotSnapshot:
        """Pull online + error + busy flags out of coordinator.data.

        The integration's existing entities already extract these from
        the same MQTT payloads; the SDK exposes the values via
        StateMSG. Keeping the extraction here means the scheduler
        evaluates against the same source of truth as the sensors.
        """
        data = self.data.get(sn, {}) if self.data else {}
        online = bool(data.get("__online__"))
        state_msg = data.get("StateMSG") or {}

        try:
            error_code = int(state_msg.get("error_code", 0) or 0)
        except (TypeError, ValueError):
            error_code = 0

        # Busy = "the robot can't accept a new start_plan right now".
        #  - planning > 0 (and != 5 = Completed) → actively running a plan
        #  - recharging in {1,2,3,99} (Returning / Repositioning /
        #    Verifying) → transitioning to/from the dock
        #  - recharging == 4 (Charging) → docked, normally idle BUT we
        #    must also treat this as busy when a plan is mid-flight
        #    (firmware reports <99% coverage), otherwise the scheduler
        #    would launch a different plan while the current one is
        #    paused for a refuel.
        try:
            planning = int(state_msg.get("on_going_planning", 0) or 0)
        except (TypeError, ValueError):
            planning = 0
        try:
            recharging = int(state_msg.get("on_going_recharging", 0) or 0)
        except (TypeError, ValueError):
            recharging = 0
        progress_pct = self._compute_progress_percent(sn)
        mid_plan_charging = recharging == 4 and 0 < progress_pct < 99
        is_busy = (
            (planning > 0 and planning != 5)
            or (recharging > 0 and recharging != 4)
            or mid_plan_charging
        )
        return RobotSnapshot(
            online=online, error_code=error_code, is_busy=is_busy,
        )

    def _build_gate_inputs(self, spec: ScheduleSpec) -> GateInputs:
        """Resolve every HA-side input the evaluator needs.

        Pre-fetches weather state, presence states, sun elevation, and
        battery so the evaluator stays a pure function. Anything
        missing (entity not configured, unavailable state) is treated
        as "gate passes" — better to fire than to silently hold for an
        un-debuggable reason.
        """
        sn = spec["device_sn"]
        sid = spec["id"]
        store = self._state_store
        if store is None:
            paused = True
            skipped = False
            last_run = None
            manual_hold = False
        else:
            sched_state = store.get_schedule_state(sn, sid)
            paused = (
                not store.get_global_enabled(sn)
                or not sched_state["enabled"]
            )
            skipped = sched_state["skip_next"]
            last_run = store.get_last_run(sn, sid)
            manual_hold = store.get_manual_hold(sn)

        # Battery — pull from the same field the battery sensor uses.
        data = self.data.get(sn, {}) if self.data else {}
        battery_pct = 100
        battery_msg = data.get("BatteryMSG") or {}
        for key in ("battery", "soc", "level"):
            raw = battery_msg.get(key)
            if raw is not None:
                try:
                    battery_pct = int(float(raw))
                except (TypeError, ValueError):
                    pass
                break

        # Weather (only when configured).
        weather_state: str | None = None
        if spec.get("weather_entity"):
            ws = self.hass.states.get(spec["weather_entity"])
            if ws and ws.state not in ("unknown", "unavailable"):
                weather_state = ws.state

        # Presence — first 'home' wins.
        presence_at_home = False
        for ent in spec.get("presence_entities", []) or []:
            ps = self.hass.states.get(ent)
            if ps is not None and ps.state == "home":
                presence_at_home = True
                break

        # Sun elevation (only when sun-mode is enabled).
        sun_elevation: float | None = None
        if spec.get("use_sun_for_sleep"):
            sun = self.hass.states.get("sun.sun")
            if sun is not None:
                try:
                    sun_elevation = float(
                        sun.attributes.get("elevation", 90)
                    )
                except (TypeError, ValueError):
                    sun_elevation = None

        # Required head type — read the head_type sensor's current
        # state. None when the sensor isn't found; the evaluator
        # treats that as "wrong head" (fail closed) when a requirement
        # is set.
        head_type: str | None = None
        head_id = self._head_type_entity_id(sn)
        if head_id:
            ht = self.hass.states.get(head_id)
            if ht and ht.state not in ("unknown", "unavailable"):
                head_type = ht.state

        # Snow forecast — pulled from the per-tick cache populated in
        # _async_scheduler_tick. Cache key is (weather_entity,
        # snow_forecast_hours) so multiple schedules pointing at the
        # same weather entity share one fetch per tick.
        snow_estimate: float | None = None
        we = spec.get("weather_entity") or ""
        sfh = int(spec.get("snow_forecast_hours", 12) or 12)
        if we and float(spec.get("min_snow_accumulation", 0) or 0) > 0:
            snow_estimate = self._snow_forecast_cache.get((we, sfh))

        # Rain rate gate (numeric). Reuses _read_rate; None when entity
        # is missing/unavailable (skipped by evaluator).
        rain_rate: float | None = None
        rre = spec.get("rain_rate_entity") or ""
        if rre and float(spec.get("rain_rate_max", 0) or 0) > 0:
            val, available = self._read_rate(rre)
            if available:
                rain_rate = val

        # Post-hold permission slip from store.
        post_hold_armed = False
        if store is not None:
            post_hold_armed = store.get_schedule_state(sn, sid).get(
                "post_hold_armed", False,
            )

        return GateInputs(
            paused=paused,
            skipped=skipped,
            manual_hold=manual_hold,
            last_run=last_run,
            interval_days=float(spec.get("interval_days", 3.0)),
            weather_state=weather_state,
            weather_hold_states=list(spec.get("weather_hold_states", [])),
            sleep_start=spec.get("sleep_start", "22:00"),
            sleep_end=spec.get("sleep_end", "06:00"),
            use_sun_for_sleep=bool(spec.get("use_sun_for_sleep", False)),
            sun_elevation_threshold=float(
                spec.get("sun_elevation_threshold", -6.0)
            ),
            sun_elevation=sun_elevation,
            battery_pct=battery_pct,
            battery_min_pct=int(spec.get("battery_min_pct", 30)),
            presence_at_home=presence_at_home,
            robot=self._derive_robot_snapshot(sn),
            head_type=head_type,
            required_head_type=spec.get("required_head_type", "") or "",
            snow_estimate=snow_estimate,
            min_snow_accumulation=float(
                spec.get("min_snow_accumulation", 0.0) or 0.0
            ),
            rain_rate=rain_rate,
            rain_rate_max=float(spec.get("rain_rate_max", 0.0) or 0.0),
            post_hold_armed=post_hold_armed,
            # MUST be local time. The evaluator's sleep-window check
            # extracts `.time()` and compares to the user's HH:MM input,
            # which is always specified in the dashboard's local timezone.
            # Using UTC here let a 20:00–10:00 EDT quiet window fire at
            # 06:00 EDT (= 10:00 UTC, which falls outside [20:00,10:00)
            # under the half-open overnight-window check). The cooldown
            # math elsewhere is timestamp-based and timezone-agnostic, so
            # it doesn't care which we pass.
            now=dt_util.now(),
        )

    def _head_type_entity_id(self, sn: str) -> str | None:
        """Find sensor.<...>_head_type for this device.

        Uses HA's util.slugify (folds accents, etc.) to construct the
        expected entity_id. Falls back to a unique_id scan if the
        convention misses (renamed device, multiple devices, etc.).
        """
        device = next((d for d in self.devices if d.sn == sn), None)
        if device is None:
            return None
        from homeassistant.util import slugify
        candidate = f"sensor.{slugify(device.name)}_head_type"
        if self.hass.states.get(candidate) is not None:
            return candidate
        from homeassistant.helpers import entity_registry as er
        ent_reg = er.async_get(self.hass)
        for ent in ent_reg.entities.values():
            if not ent.entity_id.endswith("_head_type"):
                continue
            if not ent.entity_id.startswith("sensor."):
                continue
            if ent.unique_id and sn in ent.unique_id:
                return ent.entity_id
        return None
