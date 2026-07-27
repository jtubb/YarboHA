"""Unit tests for custom_components.yarbo.zone_rules.

Pure-logic tests; no HA imports.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "custom_components" / "yarbo"))

from zone_rules import (  # type: ignore[import-not-found]
    TickInputs,
    spec_with_defaults,
    state_with_defaults,
    tick,
)


def _spec(**overrides):
    base = {
        "id": "rule1",
        "name": "Wet ground",
        "device_sn": "SN1",
        "zone_ids": [1277, 1278],
        "rate_entity": "sensor.rain",
        "event_threshold": 0.5,
        "duration_hours": 48.0,
        "dry_reset_hours": 6.0,
    }
    base.update(overrides)
    return base


def _state(**overrides):
    s = state_with_defaults(None)
    s.update(overrides)
    return s


def _inputs(now_ts=1000.0, rate=0.0, **kwargs):
    return TickInputs(
        spec=kwargs.pop("spec", _spec()),
        state=kwargs.pop("state", _state()),
        now_ts=now_ts,
        rate=rate,
        rate_available=kwargs.pop("rate_available", True),
        live_zone_enable=kwargs.pop("live_zone_enable", {}),
        presence_home=kwargs.pop("presence_home", False),
    )


class FirstSampleTests(unittest.TestCase):
    def test_first_sample_initializes_without_accumulating(self):
        # First sample sets last_sample_at but doesn't add to accumulator
        # (we have no delta to attribute). Even though it IS raining,
        # accumulator stays 0 → status is idle, not awaiting.
        r = tick(_inputs(now_ts=1000.0, rate=2.0))
        self.assertEqual(r.new_state["accumulator"], 0.0)
        self.assertEqual(r.new_state["last_sample_at"], 1000.0)
        self.assertEqual(r.new_state["last_precip_at"], 1000.0)
        self.assertEqual(r.status, "idle")

    def test_first_sample_with_zero_rate_is_idle(self):
        r = tick(_inputs(rate=0.0))
        self.assertEqual(r.status, "idle")
        self.assertEqual(r.new_state["accumulator"], 0.0)


class AccumulationTests(unittest.TestCase):
    def test_steady_rain_accumulates_over_two_ticks(self):
        # Tick 1 — first sample.
        r1 = tick(_inputs(now_ts=1000.0, rate=1.0))
        # Tick 2 — 60s later, same rate.
        r2 = tick(_inputs(
            now_ts=1060.0,
            rate=1.0,
            state=r1.new_state,
        ))
        # 1.0 in/h × 60s = 1/60 in
        self.assertAlmostEqual(r2.new_state["accumulator"], 1 / 60, places=4)
        self.assertEqual(r2.status, "awaiting")

    def test_threshold_crossing_engages(self):
        # Set accumulator just below threshold, then push it over.
        s = _state(
            accumulator=0.49,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
        )
        r = tick(_inputs(now_ts=1060.0, rate=2.0, state=s))
        self.assertGreaterEqual(r.new_state["accumulator"], 0.5)
        self.assertEqual(r.status, "engaged")
        self.assertEqual(r.action.enable_zones, [1277, 1278])
        self.assertTrue(r.action.fire_engaged_event)
        self.assertTrue(r.action.fire_threshold_event)
        # expires_at = now + 48h
        self.assertEqual(r.new_state["expires_at"], 1060.0 + 48 * 3600)

    def test_already_engaged_extends_timer_no_new_event(self):
        # Already-engaged state.
        s = _state(
            accumulator=0.6,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
            expires_at=1000.0 + 24 * 3600,
            auto_owned_zones=[1277, 1278],
            last_owned_state={"1277": True, "1278": True},
        )
        live = {"1277": True, "1278": True}
        r = tick(_inputs(now_ts=1060.0, rate=2.0, state=s, live_zone_enable=live))
        self.assertEqual(r.status, "engaged")
        # Timer should be extended to now + 48h (rolling).
        self.assertEqual(r.new_state["expires_at"], 1060.0 + 48 * 3600)
        # No new enable events — already engaged.
        self.assertEqual(r.action.enable_zones, [])
        self.assertFalse(r.action.fire_engaged_event)


class ReleaseTests(unittest.TestCase):
    def test_release_when_timer_expired(self):
        s = _state(
            accumulator=0.6,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
            expires_at=1000.0 - 60,   # already in the past
            auto_owned_zones=[1277, 1278],
            last_owned_state={"1277": True, "1278": True},
        )
        live = {"1277": True, "1278": True}
        r = tick(_inputs(now_ts=1060.0, rate=0.0, state=s, live_zone_enable=live))
        self.assertEqual(r.status, "released")
        self.assertEqual(sorted(r.action.disable_zones), [1277, 1278])
        self.assertTrue(r.action.fire_released_event)
        # State cleared.
        self.assertEqual(r.new_state["auto_owned_zones"], [])
        self.assertEqual(r.new_state["accumulator"], 0.0)
        self.assertIsNone(r.new_state["expires_at"])

    def test_engaged_below_threshold_stays_until_expiry(self):
        # Accumulator dropped below threshold (e.g., reset by a short
        # dry spell elsewhere) but expires_at hasn't passed → stay engaged.
        s = _state(
            accumulator=0.0,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
            expires_at=1000.0 + 3600,
            auto_owned_zones=[1277],
            last_owned_state={"1277": True},
        )
        live = {"1277": True}
        r = tick(_inputs(now_ts=1060.0, rate=0.0, state=s, live_zone_enable=live))
        self.assertEqual(r.status, "engaged")
        self.assertEqual(r.action.disable_zones, [])


class ManualOverrideTests(unittest.TestCase):
    def test_user_disables_zone_drops_ownership(self):
        s = _state(
            accumulator=0.6,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
            expires_at=1000.0 + 3600,
            auto_owned_zones=[1277, 1278],
            last_owned_state={"1277": True, "1278": True},
        )
        # User disabled 1278 manually since we set it.
        live = {"1277": True, "1278": False}
        r = tick(_inputs(now_ts=1060.0, rate=0.0, state=s, live_zone_enable=live))
        self.assertEqual(r.new_state["auto_owned_zones"], [1277])
        # On release: only disable the one we still own.
        s2 = r.new_state
        s2["expires_at"] = 1060.0 - 60   # force expiry
        r2 = tick(_inputs(now_ts=1120.0, rate=0.0, state=s2, live_zone_enable={"1277": True}))
        self.assertEqual(r2.action.disable_zones, [1277])

    def test_zone_disappears_dropped_silently(self):
        s = _state(
            accumulator=0.6,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
            expires_at=1000.0 + 3600,
            auto_owned_zones=[1277, 9999],
            last_owned_state={"1277": True, "9999": True},
        )
        live = {"1277": True}  # 9999 missing
        r = tick(_inputs(now_ts=1060.0, rate=0.0, state=s, live_zone_enable=live))
        self.assertEqual(r.new_state["auto_owned_zones"], [1277])


class DryResetTests(unittest.TestCase):
    def test_accumulator_resets_after_dry_period(self):
        s = _state(
            accumulator=0.3,  # below threshold
            last_sample_at=1000.0,
            last_precip_at=1000.0,
        )
        # 7 hours later (> dry_reset_hours=6)
        r = tick(_inputs(now_ts=1000.0 + 7 * 3600, rate=0.0, state=s))
        self.assertEqual(r.new_state["accumulator"], 0.0)
        self.assertEqual(r.status, "idle")

    def test_engaged_does_not_dry_reset(self):
        s = _state(
            accumulator=0.6,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
            expires_at=1000.0 + 24 * 3600,
            auto_owned_zones=[1277],
            last_owned_state={"1277": True},
        )
        live = {"1277": True}
        r = tick(_inputs(
            now_ts=1000.0 + 12 * 3600,  # 12h later, well past dry_reset
            rate=0.0,
            state=s,
            live_zone_enable=live,
        ))
        # Still engaged, accumulator preserved.
        self.assertEqual(r.status, "engaged")
        self.assertEqual(r.new_state["accumulator"], 0.6)


class PauseTests(unittest.TestCase):
    def test_paused_short_circuits(self):
        s = _state(
            accumulator=0.9,
            enabled=False,
        )
        r = tick(_inputs(now_ts=1000.0, rate=5.0, state=s))
        self.assertEqual(r.status, "paused")
        self.assertEqual(r.action.enable_zones, [])


class RestartGapTests(unittest.TestCase):
    def test_long_gap_clamped_to_max_delta(self):
        # last_sample_at way in the past; should not over-accumulate.
        s = _state(
            accumulator=0.0,
            last_sample_at=1000.0,
            last_precip_at=1000.0,
        )
        r = tick(_inputs(now_ts=1000.0 + 24 * 3600, rate=10.0, state=s))
        # If we hadn't clamped: 10 in/h × 24h = 240 in → false trigger.
        # With clamp at 120s: 10 × 120/3600 = 0.33 in.
        self.assertLess(r.new_state["accumulator"], 1.0)


class PresenceTriggerTests(unittest.TestCase):
    def _presence_spec(self, **overrides):
        return _spec(
            trigger_type="presence",
            presence_entities=["person.alice"],
            duration_hours=1.0,
            **overrides,
        )

    def test_idle_when_no_one_home(self):
        r = tick(_inputs(
            spec=self._presence_spec(),
            presence_home=False,
        ))
        self.assertEqual(r.status, "idle")
        self.assertEqual(r.action.enable_zones, [])

    def test_engages_when_home(self):
        r = tick(_inputs(
            spec=self._presence_spec(),
            presence_home=True,
        ))
        self.assertEqual(r.status, "engaged")
        self.assertEqual(set(r.action.enable_zones), {1277, 1278})
        self.assertTrue(r.action.fire_engaged_event)
        self.assertEqual(r.new_state["auto_owned_zones"], [1277, 1278])

    def test_stays_engaged_while_home_with_rolling_timer(self):
        s = _state(
            auto_owned_zones=[1277, 1278],
            last_owned_state={"1277": True, "1278": True},
            expires_at=1000.0 + 3600.0,  # 1h from t0
        )
        # 30 minutes later, still home
        r = tick(_inputs(
            now_ts=1000.0 + 1800.0,
            spec=self._presence_spec(),
            state=s,
            presence_home=True,
            live_zone_enable={"1277": True, "1278": True},
        ))
        self.assertEqual(r.status, "engaged")
        # Timer rolled forward to now + 1h
        self.assertEqual(r.new_state["expires_at"], 1000.0 + 1800.0 + 3600.0)

    def test_releases_after_grace_when_left(self):
        # Engaged, expires in 1h, person left → timer no longer rolls.
        s = _state(
            auto_owned_zones=[1277, 1278],
            last_owned_state={"1277": True, "1278": True},
            expires_at=1000.0 + 3600.0,
        )
        # 2h later — grace expired
        r = tick(_inputs(
            now_ts=1000.0 + 2 * 3600.0,
            spec=self._presence_spec(),
            state=s,
            presence_home=False,
            live_zone_enable={"1277": True, "1278": True},
        ))
        self.assertEqual(r.status, "released")
        self.assertEqual(set(r.action.disable_zones), {1277, 1278})
        self.assertEqual(r.new_state["auto_owned_zones"], [])

    def test_holds_during_grace_when_left(self):
        # Just left, timer hasn't expired.
        s = _state(
            auto_owned_zones=[1277, 1278],
            last_owned_state={"1277": True, "1278": True},
            expires_at=1000.0 + 3600.0,
        )
        r = tick(_inputs(
            now_ts=1000.0 + 1800.0,  # 30m, half the grace
            spec=self._presence_spec(),
            state=s,
            presence_home=False,
            live_zone_enable={"1277": True, "1278": True},
        ))
        self.assertEqual(r.status, "engaged")
        # Timer was NOT extended (presence_home=False).
        self.assertEqual(r.new_state["expires_at"], 1000.0 + 3600.0)

    def test_presence_ignores_rate_sensor(self):
        # Heavy rain shouldn't trigger a presence rule.
        r = tick(_inputs(
            spec=self._presence_spec(),
            rate=50.0,
            presence_home=False,
        ))
        self.assertEqual(r.status, "idle")
        self.assertEqual(r.action.enable_zones, [])


if __name__ == "__main__":
    unittest.main()
