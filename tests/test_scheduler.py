"""Unit tests for custom_components.yarbo.scheduler.

Pure-logic tests — does NOT import Home Assistant, so it runs with just
``python -m unittest tests.test_scheduler`` from the repo root. (No
pytest dependency required.)

The Store wrapper in scheduler_state.py is exercised at integration
test time via HA itself; these tests cover the deterministic gate
logic, which is where any subtle bug would silently let the mower run
in the rain at midnight.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

# Allow running the file directly without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "custom_components" / "yarbo"))

from scheduler import (  # type: ignore[import-not-found]
    DEFAULT_WEATHER_HOLD_STATES,
    HOLD_LABELS,
    Evaluation,
    GateInputs,
    RobotSnapshot,
    evaluate,
    is_in_sleep_window,
    next_eligible_at,
    schedule_unique_id,
    slugify,
    spec_with_defaults,
)


# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------


class SlugifyTests(unittest.TestCase):
    def test_lowercases(self):
        self.assertEqual(slugify("Front Yard"), "front_yard")

    def test_collapses_runs_of_non_alnum(self):
        self.assertEqual(slugify("Lot #3 — North!!"), "lot_3_north")

    def test_trims_underscores(self):
        self.assertEqual(slugify("  ___Side___  "), "side")

    def test_unicode_is_stripped(self):
        # Non-ASCII letters are non-alphanumeric under [a-z0-9], so they
        # collapse to underscores. Matches the TS implementation.
        self.assertEqual(slugify("Jardín Frente"), "jard_n_frente")

    def test_empty_inputs(self):
        self.assertEqual(slugify(""), "")
        self.assertEqual(slugify("!!!"), "")


# ---------------------------------------------------------------------------
# is_in_sleep_window
# ---------------------------------------------------------------------------


class SleepWindowTests(unittest.TestCase):
    # Same-day window 08:00–20:00
    def test_same_day_middle_is_in(self):
        self.assertTrue(is_in_sleep_window(time(10, 0), "08:00", "20:00"))

    def test_same_day_at_end_is_out_half_open(self):
        self.assertFalse(is_in_sleep_window(time(20, 0), "08:00", "20:00"))

    def test_same_day_before_start_is_out(self):
        self.assertFalse(is_in_sleep_window(time(7, 59), "08:00", "20:00"))

    # Overnight window 22:00–06:00
    def test_overnight_after_start_is_in(self):
        self.assertTrue(is_in_sleep_window(time(23, 30), "22:00", "06:00"))

    def test_overnight_before_end_is_in(self):
        self.assertTrue(is_in_sleep_window(time(3, 0), "22:00", "06:00"))

    def test_overnight_at_end_is_out_half_open(self):
        self.assertFalse(is_in_sleep_window(time(6, 0), "22:00", "06:00"))

    def test_overnight_midday_is_out(self):
        self.assertFalse(is_in_sleep_window(time(12, 0), "22:00", "06:00"))

    # Edge cases
    def test_zero_width_window_is_always_out(self):
        for h in (0, 12, 23):
            self.assertFalse(
                is_in_sleep_window(time(h, 0), "08:00", "08:00"),
            )

    def test_seconds_format_accepted(self):
        self.assertTrue(
            is_in_sleep_window(time(23, 30), "22:00:00", "06:00:00"),
        )

    def test_invalid_input_falls_open(self):
        self.assertFalse(is_in_sleep_window(time(2, 0), "lol", "06:00"))
        self.assertFalse(is_in_sleep_window(time(2, 0), "25:00", "06:00"))


# ---------------------------------------------------------------------------
# next_eligible_at
# ---------------------------------------------------------------------------


class NextEligibleAtTests(unittest.TestCase):
    def test_no_last_run_returns_none(self):
        self.assertIsNone(
            next_eligible_at(None, 3.0, datetime(2026, 5, 4, 12, 0)),
        )

    def test_zero_or_negative_interval_returns_none(self):
        now = datetime(2026, 5, 4, 12, 0)
        self.assertIsNone(next_eligible_at(now - timedelta(days=1), 0, now))
        self.assertIsNone(next_eligible_at(now - timedelta(days=1), -1, now))

    def test_cooldown_elapsed_returns_none(self):
        now = datetime(2026, 5, 4, 12, 0)
        self.assertIsNone(
            next_eligible_at(now - timedelta(days=4), 3.0, now),
        )

    def test_cooldown_remaining_returns_future(self):
        now = datetime(2026, 5, 4, 12, 0)
        last = now - timedelta(days=1)
        result = next_eligible_at(last, 3.0, now)
        self.assertIsNotNone(result)
        self.assertEqual(result, last + timedelta(days=3))


# ---------------------------------------------------------------------------
# evaluate — priority order is the contract
# ---------------------------------------------------------------------------


def _base() -> GateInputs:
    """Default inputs: every gate passes, robot is idle, cooldown clear."""
    return GateInputs(
        paused=False,
        skipped=False,
        last_run=None,
        interval_days=3.0,
        weather_state=None,
        weather_hold_states=list(DEFAULT_WEATHER_HOLD_STATES),
        sleep_start="22:00",
        sleep_end="06:00",
        use_sun_for_sleep=False,
        sun_elevation_threshold=-6.0,
        sun_elevation=20.0,
        battery_pct=80,
        battery_min_pct=30,
        presence_at_home=False,
        robot=RobotSnapshot(online=True, error_code=0, is_busy=False),
        # 12:00 — well outside the default 22-06 quiet window
        now=datetime(2026, 5, 4, 12, 0),
    )


class EvaluatePriorityTests(unittest.TestCase):
    def test_default_inputs_are_eligible(self):
        self.assertEqual(evaluate(_base()).hold_reason, "eligible")

    def test_paused_beats_everything(self):
        g = _base()
        g.paused = True
        g.skipped = True
        g.robot = RobotSnapshot(False, 17, True)
        g.battery_pct = 0
        g.presence_at_home = True
        g.weather_state = "rainy"
        self.assertEqual(evaluate(g).hold_reason, "paused")

    def test_skipped_beats_robot_battery_presence_sleep_weather(self):
        g = _base()
        g.skipped = True
        g.robot = RobotSnapshot(False, 0, False)
        g.battery_pct = 0
        g.presence_at_home = True
        g.weather_state = "rainy"
        self.assertEqual(evaluate(g).hold_reason, "skipped")

    def test_robot_offline_beats_battery_and_below(self):
        g = _base()
        g.robot = RobotSnapshot(online=False, error_code=0, is_busy=False)
        g.battery_pct = 0
        g.presence_at_home = True
        g.weather_state = "rainy"
        self.assertEqual(evaluate(g).hold_reason, "robot-offline")

    def test_error_code_nonzero_is_robot_busy(self):
        g = _base()
        g.robot = RobotSnapshot(True, 17, False)
        self.assertEqual(evaluate(g).hold_reason, "robot-busy")

    def test_busy_flag_is_robot_busy(self):
        g = _base()
        g.robot = RobotSnapshot(True, 0, True)
        self.assertEqual(evaluate(g).hold_reason, "robot-busy")

    def test_idle_robot_passes(self):
        g = _base()
        g.robot = RobotSnapshot(True, 0, False)
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_battery_below_threshold(self):
        g = _base()
        g.battery_pct = 25
        g.battery_min_pct = 30
        self.assertEqual(evaluate(g).hold_reason, "battery")

    def test_battery_above_threshold_is_eligible(self):
        g = _base()
        g.battery_pct = 30
        g.battery_min_pct = 30
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_presence_beats_sleep_and_weather(self):
        g = _base()
        g.presence_at_home = True
        g.weather_state = "rainy"
        # Force into the sleep window too, to confirm presence wins.
        g.now = datetime(2026, 5, 4, 23, 30)
        self.assertEqual(evaluate(g).hold_reason, "presence")

    def test_sleep_window_beats_weather(self):
        g = _base()
        g.now = datetime(2026, 5, 4, 23, 30)  # in window 22-06
        g.weather_state = "rainy"
        self.assertEqual(evaluate(g).hold_reason, "sleep")

    def test_sun_below_threshold_when_sun_mode_on(self):
        g = _base()
        g.use_sun_for_sleep = True
        g.sun_elevation = -10.0
        g.sun_elevation_threshold = -6.0
        # Outside the explicit time window — only sun-mode triggers.
        g.now = datetime(2026, 5, 4, 12, 0)
        self.assertEqual(evaluate(g).hold_reason, "sleep")

    def test_sun_below_threshold_ignored_when_sun_mode_off(self):
        g = _base()
        g.use_sun_for_sleep = False
        g.sun_elevation = -20.0
        g.now = datetime(2026, 5, 4, 12, 0)
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_weather_hold_when_state_matches(self):
        g = _base()
        g.weather_state = "pouring"
        self.assertEqual(evaluate(g).hold_reason, "weather")

    def test_weather_sunny_is_eligible(self):
        g = _base()
        g.weather_state = "sunny"
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_no_weather_entity_skips_gate(self):
        g = _base()
        g.weather_state = None
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_cooldown_is_lowest_priority(self):
        g = _base()
        g.last_run = datetime(2026, 5, 4, 12, 0) - timedelta(days=1)
        g.interval_days = 3.0
        result = evaluate(g)
        self.assertEqual(result.hold_reason, "cooldown")
        self.assertIsNotNone(result.next_eligible_at)

    def test_cooldown_elapsed_is_eligible(self):
        g = _base()
        g.last_run = datetime(2026, 5, 4, 12, 0) - timedelta(days=4)
        g.interval_days = 3.0
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_next_eligible_at_populated_even_when_held_for_other_reason(self):
        # The card surfaces "next in X" even during a weather hold; the
        # number is meaningful as soon as the weather clears.
        g = _base()
        g.weather_state = "rainy"
        g.last_run = datetime(2026, 5, 4, 12, 0) - timedelta(hours=12)
        g.interval_days = 1.0
        result = evaluate(g)
        self.assertEqual(result.hold_reason, "weather")
        self.assertIsNotNone(result.next_eligible_at)


# ---------------------------------------------------------------------------
# spec_with_defaults
# ---------------------------------------------------------------------------


class SpecDefaultsTests(unittest.TestCase):
    def test_minimal_spec_gets_filled(self):
        full = spec_with_defaults({"id": "abc", "device_sn": "SN1", "plan_name": "Front"})
        self.assertEqual(full["interval_days"], 3.0)
        self.assertEqual(full["sleep_start"], "22:00")
        self.assertEqual(full["sleep_end"], "06:00")
        self.assertEqual(full["battery_min_pct"], 30)
        self.assertEqual(full["weather_hold_states"], list(DEFAULT_WEATHER_HOLD_STATES))
        self.assertFalse(full["use_sun_for_sleep"])
        self.assertEqual(full["pre_run_notify_minutes"], 5)

    def test_provided_values_are_preserved(self):
        spec = {
            "id": "abc",
            "device_sn": "SN1",
            "plan_name": "Back",
            "interval_days": 7.5,
            "sleep_start": "20:00",
            "sleep_end": "08:00",
            "battery_min_pct": 50,
        }
        full = spec_with_defaults(spec)
        self.assertEqual(full["interval_days"], 7.5)
        self.assertEqual(full["battery_min_pct"], 50)
        self.assertEqual(full["sleep_start"], "20:00")


# ---------------------------------------------------------------------------
# schedule_unique_id
# ---------------------------------------------------------------------------


class UniqueIdTests(unittest.TestCase):
    def test_format_is_stable_across_calls(self):
        a = schedule_unique_id("SN123", "uuid-abc", "run_now")
        b = schedule_unique_id("SN123", "uuid-abc", "run_now")
        self.assertEqual(a, b)
        self.assertEqual(a, "SN123_schedule_uuid-abc_run_now")

    def test_different_suffix_yields_different_id(self):
        run = schedule_unique_id("SN123", "uuid-abc", "run_now")
        skip = schedule_unique_id("SN123", "uuid-abc", "skip_next")
        self.assertNotEqual(run, skip)


# ---------------------------------------------------------------------------
# HOLD_LABELS coverage
# ---------------------------------------------------------------------------


class HoldLabelsTests(unittest.TestCase):
    def test_every_reason_has_a_label(self):
        reasons = [
            "eligible",
            "cooldown",
            "paused",
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
        for r in reasons:
            self.assertIn(r, HOLD_LABELS)
            self.assertTrue(HOLD_LABELS[r])


# ---------------------------------------------------------------------------
# Snow-aware gates
# ---------------------------------------------------------------------------


class WrongHeadGateTests(unittest.TestCase):
    def test_required_head_matches_substring(self):
        g = _base()
        g.required_head_type = "snow blower"
        g.head_type = "Snow Blower"
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_required_head_substring_partial(self):
        g = _base()
        g.required_head_type = "mower"
        g.head_type = "Mower Pro"
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_wrong_head_holds(self):
        g = _base()
        g.required_head_type = "snow blower"
        g.head_type = "Mower Pro"
        self.assertEqual(evaluate(g).hold_reason, "wrong-head")

    def test_missing_head_with_requirement_holds(self):
        g = _base()
        g.required_head_type = "snow blower"
        g.head_type = None
        # Defensive: missing sensor reads as hold rather than allowing
        # an unknown attachment to run.
        self.assertEqual(evaluate(g).hold_reason, "wrong-head")

    def test_no_requirement_passes_any_head(self):
        g = _base()
        g.required_head_type = ""
        g.head_type = "anything"
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_wrong_head_beats_battery_and_below(self):
        g = _base()
        g.required_head_type = "snow blower"
        g.head_type = "Mower Pro"
        g.battery_pct = 0
        g.weather_state = "rainy"
        self.assertEqual(evaluate(g).hold_reason, "wrong-head")


class SnowInsufficientGateTests(unittest.TestCase):
    def test_below_threshold_holds(self):
        g = _base()
        g.min_snow_accumulation = 2.0
        g.snow_estimate = 0.5
        self.assertEqual(evaluate(g).hold_reason, "snow-insufficient")

    def test_above_threshold_passes(self):
        g = _base()
        g.min_snow_accumulation = 2.0
        g.snow_estimate = 3.0
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_zero_threshold_disables_gate(self):
        g = _base()
        g.min_snow_accumulation = 0.0
        g.snow_estimate = 0.0  # would be insufficient if gate enabled
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_no_estimate_skips_gate(self):
        # If we couldn't compute an estimate (sensor unavailable), don't
        # silently hold forever — fail open here. Different from
        # wrong-head which fails closed because mishaps are physical.
        g = _base()
        g.min_snow_accumulation = 2.0
        g.snow_estimate = None
        self.assertEqual(evaluate(g).hold_reason, "eligible")


class PostHoldArmedTests(unittest.TestCase):
    def test_post_hold_bypasses_cooldown(self):
        g = _base()
        g.post_hold_armed = True
        g.last_run = datetime(2026, 5, 4, 12, 0) - timedelta(hours=12)
        g.interval_days = 3.0
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_post_hold_bypasses_weather(self):
        g = _base()
        g.post_hold_armed = True
        g.weather_state = "rainy"
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_post_hold_bypasses_snow_insufficient(self):
        g = _base()
        g.post_hold_armed = True
        g.min_snow_accumulation = 2.0
        g.snow_estimate = 0.0
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_post_hold_does_not_bypass_sleep(self):
        g = _base()
        g.post_hold_armed = True
        g.now = datetime(2026, 5, 4, 23, 30)  # in default 22-06 window
        self.assertEqual(evaluate(g).hold_reason, "sleep")

    def test_post_hold_does_not_bypass_wrong_head(self):
        g = _base()
        g.post_hold_armed = True
        g.required_head_type = "snow blower"
        g.head_type = "Mower Pro"
        self.assertEqual(evaluate(g).hold_reason, "wrong-head")

    def test_post_hold_does_not_bypass_pause(self):
        g = _base()
        g.post_hold_armed = True
        g.paused = True
        self.assertEqual(evaluate(g).hold_reason, "paused")

    def test_post_hold_does_not_bypass_battery(self):
        g = _base()
        g.post_hold_armed = True
        g.battery_pct = 10
        g.battery_min_pct = 30
        self.assertEqual(evaluate(g).hold_reason, "battery")


class RainRateGateTests(unittest.TestCase):
    def test_above_threshold_holds_with_weather_reason(self):
        g = _base()
        g.rain_rate_max = 0.1
        g.rain_rate = 0.5
        self.assertEqual(evaluate(g).hold_reason, "weather")

    def test_at_threshold_holds(self):
        g = _base()
        g.rain_rate_max = 0.5
        g.rain_rate = 0.5
        self.assertEqual(evaluate(g).hold_reason, "weather")

    def test_below_threshold_passes(self):
        g = _base()
        g.rain_rate_max = 0.5
        g.rain_rate = 0.1
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_zero_max_disables_gate(self):
        g = _base()
        g.rain_rate_max = 0.0
        g.rain_rate = 99.0
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_no_reading_skips_gate(self):
        g = _base()
        g.rain_rate_max = 0.5
        g.rain_rate = None
        # Fail open — sensor unavailable shouldn't silently hold forever.
        self.assertEqual(evaluate(g).hold_reason, "eligible")

    def test_post_hold_armed_bypasses_rain_rate(self):
        g = _base()
        g.post_hold_armed = True
        g.rain_rate_max = 0.1
        g.rain_rate = 5.0
        self.assertEqual(evaluate(g).hold_reason, "eligible")


if __name__ == "__main__":
    unittest.main()
