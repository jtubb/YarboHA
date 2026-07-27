"""Constants for the Yarbo integration."""

DOMAIN = "yarbo"
PLATFORMS = ["sensor", "binary_sensor", "select", "device_tracker", "button", "switch", "number"]

# Config flow
CONF_EMAIL = "email"
CONF_PASSWORD = "password"
# Config entry data keys
DATA_ACCESS_TOKEN = "access_token"
DATA_REFRESH_TOKEN = "refresh_token"

# Options
CONF_SELECTED_DEVICES = "selected_devices"

# Keep-awake policy: how aggressively HA renews the device wake-up command.
CONF_KEEP_AWAKE_MODE = "keep_awake_mode"
KEEP_AWAKE_ALWAYS = "always"
KEEP_AWAKE_DOCKED = "docked"
KEEP_AWAKE_OFF = "off"
KEEP_AWAKE_MODES = {
    KEEP_AWAKE_ALWAYS: "Always (device never sleeps)",
    KEEP_AWAKE_DOCKED: "Only while charging on the dock",
    KEEP_AWAKE_OFF: "Never (device sleeps on its own schedule)",
}

# Scheduler options. Stored as a list of dicts under this key in
# config_entry.options. See scheduler.ScheduleSpec for the expected
# shape. Editing this list triggers a full integration reload, which
# re-creates per-schedule entities.
CONF_SCHEDULES = "schedules"

# Per-minute scheduler tick. Cheap because the work inside is just
# evaluating in-memory state and (rarely) firing a plan.
SCHEDULER_TICK_SECONDS = 60

# HA bus events fired by the scheduler. All include device_sn,
# device_name, plan_id, plan_name, schedule_id (None when no matching
# schedule is configured).
EVENT_PLAN_STARTED = "yarbo_plan_started"           # also: triggered_by, percent
EVENT_PLAN_FINISHED = "yarbo_plan_finished"         # also: success, reason, error_code, planning_code, recharging_code, resume_percent_saved
EVENT_QUIET_HOURS_STOP = "yarbo_plan_quiet_hours_stop"  # also: sleep_window

# Zone rule options. Stored as a list of dicts under this key in
# config_entry.options. See zone_rules.ZoneRuleSpec for the shape.
CONF_ZONE_RULES = "zone_rules"

# Zone rule lifecycle events.
EVENT_ZONE_RULE_THRESHOLD = "yarbo_zone_rule_threshold_crossed"  # accumulator first crossed threshold this engagement
EVENT_ZONE_RULE_ENGAGED = "yarbo_zone_rule_engaged"              # zones just enabled by this rule
EVENT_ZONE_RULE_RELEASED = "yarbo_zone_rule_released"            # release timer expired, zones disabled
