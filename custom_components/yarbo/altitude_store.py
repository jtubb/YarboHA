"""Per-area altitude sample buffer for downstream mesh-building.

Records ``[lat, lon, z_msl, ts]`` tuples grouped by device SN and
mowing area_id (from plan_feedback's ``cleanAreaId``). Spatially
de-duped: a new sample within ``DEDUP_RADIUS_M`` of an existing one
in the same area replaces the existing one's altitude/timestamp
rather than appending. This caps total samples by spatial coverage
density rather than time spent.

Stored at ``yarbo.altitude.<entry_id>`` via HA's Store helper. Hard
upper bound of ``PER_AREA_MAX_SAMPLES`` per area (oldest dropped) as
a safety net if dedup falls behind for any reason.

GPS-only persistence: portable to any GIS tool, survives gps_ref
shifts (mower re-init, dock moved). Distance dedup uses a small
flat-earth approximation since DEDUP_RADIUS_M is sub-meter.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

# Bumped from 1 → 2: schema changed from [x, y, z, lat, lon, ts] to
# [lat, lon, z, ts]. v1 data is dropped on load (mesh-building hadn't
# started; a re-record over a few runs gives the same coverage).
STORAGE_VERSION = 2
DEDUP_RADIUS_M = 0.5
PER_AREA_MAX_SAMPLES = 5000

# Meters-per-degree at the equator. Latitude is uniform; longitude
# scales by cos(lat) which we apply per-comparison.
_M_PER_DEG_LAT = 111_320.0


class _AltitudeBackingStore(Store):
    """HA Store subclass with a v1→v2 migrator (drops old data)."""

    async def _async_migrate_func(
        self, old_major_version: int, old_minor_version: int, old_data: Any,
    ) -> dict:
        # v1 used a 6-tuple [x, y, z, lat, lon, ts] (local coords);
        # v2 is [lat, lon, z, ts] (GPS-only, portable). Re-recording
        # over a few runs reproduces the same spatial coverage, so a
        # wipe is acceptable.
        _LOGGER.info(
            "[altitude] discarding v%d.%d data (schema changed to v%d)",
            old_major_version, old_minor_version, STORAGE_VERSION,
        )
        return {}


class AltitudeStore:
    """Persistent per-area altitude sample buffer.

    Schema: ``{sn: {area_id_str: [[lat, lon, z_msl, ts], ...]}}``
    """

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._hass = hass
        self._store: Store = _AltitudeBackingStore(
            hass, STORAGE_VERSION, f"yarbo.altitude.{entry_id}",
        )
        self._data: dict[str, dict[str, list[list[float]]]] = {}
        self._dirty = False

    async def async_load(self) -> None:
        loaded = await self._store.async_load()
        if isinstance(loaded, dict):
            self._data = loaded

    async def async_save(self) -> None:
        if not self._dirty:
            return
        await self._store.async_save(self._data)
        self._dirty = False

    @property
    def dirty(self) -> bool:
        return self._dirty

    def maybe_record(
        self,
        sn: str,
        area_id: int | str,
        lat: float,
        lon: float,
        z_msl: float,
        ts: float,
    ) -> bool:
        """Add a sample if not within DEDUP_RADIUS_M of an existing one
        in the same area; otherwise refresh the existing sample's
        altitude + timestamp. Returns True if a *new* point was added.

        Distance is computed in meters using a flat-earth projection
        around the candidate sample's latitude — accurate enough for
        sub-meter dedup at any latitude away from the poles.
        """
        key = str(area_id)
        sn_data = self._data.setdefault(sn, {})
        samples = sn_data.setdefault(key, [])
        r2 = DEDUP_RADIUS_M * DEDUP_RADIUS_M
        m_per_deg_lon = _M_PER_DEG_LAT * math.cos(math.radians(lat))
        for s in samples:
            dlat_m = (s[0] - lat) * _M_PER_DEG_LAT
            dlon_m = (s[1] - lon) * m_per_deg_lon
            if dlat_m * dlat_m + dlon_m * dlon_m < r2:
                # Already covered. Update z + ts to latest reading so
                # the mesh reflects the most recent fix at this spot.
                s[2] = round(float(z_msl), 3)
                s[3] = round(float(ts), 1)
                self._dirty = True
                return False
        samples.append([
            round(float(lat), 7),
            round(float(lon), 7),
            round(float(z_msl), 3),
            round(float(ts), 1),
        ])
        if len(samples) > PER_AREA_MAX_SAMPLES:
            del samples[0]
        self._dirty = True
        return True

    def samples_for(
        self, sn: str, area_id: int | str | None = None,
    ) -> Any:
        """Return all samples for an SN (dict by area), or one area's list."""
        sn_data = self._data.get(sn, {})
        if area_id is None:
            return sn_data
        return sn_data.get(str(area_id), [])

    def all_data(self) -> dict[str, dict[str, list[list[float]]]]:
        return self._data

    def clear(
        self,
        sn: str | None = None,
        area_id: int | str | None = None,
    ) -> None:
        if sn is None:
            self._data = {}
        elif area_id is None:
            self._data.pop(sn, None)
        else:
            self._data.get(sn, {}).pop(str(area_id), None)
        self._dirty = True

    def stats(self) -> dict[str, dict[str, int]]:
        """Per-SN per-area sample count for diagnostics."""
        return {
            sn: {a: len(pts) for a, pts in areas.items()}
            for sn, areas in self._data.items()
        }
