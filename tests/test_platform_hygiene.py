"""Static guards against HA-integration mistakes that fail silently.

Pure source inspection — does NOT import Home Assistant, so it runs with
``python -m unittest tests.test_platform_hygiene`` from the repo root.

Everything here encodes a bug that actually happened and that produced
no error at runtime. Each one cost a long debugging session precisely
because HA logged a warning at most, so a cheap static check is worth
more than it looks.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

COMPONENT = Path(__file__).resolve().parent.parent / "custom_components" / "yarbo"

# Platforms that attach entities to the shared per-mower device.
PLATFORM_FILES = [
    "binary_sensor.py",
    "button.py",
    "device_tracker.py",
    "number.py",
    "select.py",
    "sensor.py",
    "switch.py",
]


class SubentryDeviceTests(unittest.TestCase):
    """A device may belong to at most one config subentry (HA >= 2026.8).

    Every schedule is its own subentry, and they all attach to the one
    mower device, so passing config_subentry_id made each add move that
    device. Under 2026.8 the move detached it from the main config
    entry and HA purged 213 of 217 entities -- the device page went
    empty with no error logged anywhere.
    """

    def test_no_config_subentry_id_in_entity_adds(self) -> None:
        offenders = []
        for name in PLATFORM_FILES:
            path = COMPONENT / name
            if not path.exists():
                continue
            for i, line in enumerate(path.read_text().splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                if re.search(r"config_subentry_id\s*=", line):
                    offenders.append(f"{name}:{i}: {line.strip()}")
        self.assertEqual(
            offenders,
            [],
            "async_add_entities must not pass config_subentry_id while "
            "entities share one mower device; this silently purges every "
            "main-entry entity on reload:\n" + "\n".join(offenders),
        )


class SdkInternalsTests(unittest.TestCase):
    """The SDK has no ``_mqtt``; it keeps _legacy_mqtt/_new_mqtt.

    Broker choice is per device via _ensure_mqtt_for(sn). Reaching for a
    single global _mqtt raised AttributeError on the attribute form and
    silently evaluated to None on the getattr form -- which made
    goto_waypoints never publish and no-go toggles always fail.
    """

    def test_no_direct_mqtt_attribute_access(self) -> None:
        offenders = []
        for path in sorted(COMPONENT.glob("*.py")):
            for i, line in enumerate(path.read_text().splitlines(), 1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                if re.search(r"_client\._mqtt\b", line) or re.search(
                    r"getattr\(\s*[\w.]*_client\s*,\s*[\"']_mqtt[\"']", line
                ):
                    offenders.append(f"{path.name}:{i}: {line.strip()}")
        self.assertEqual(
            offenders,
            [],
            "use _publish_raw_topic / _subscribe_raw_topic instead of a "
            "global _mqtt:\n" + "\n".join(offenders),
        )


class PayloadEncodingTests(unittest.TestCase):
    """Wire format depends on device firmware, not on the SDK.

    mqtt_publish_command sends zlib only for firmware >= 3.9.0 and
    plaintext JSON below it. Encoding unconditionally handed a zlib blob
    to firmware 3.2.21, which dropped it without any data_feedback
    reply. _publish_raw_topic owns this decision now, so no caller
    should be encoding payloads itself.
    """

    def test_encode_only_paired_with_should_compress(self) -> None:
        text = (COMPONENT / "coordinator.py").read_text()
        encode_lines = [
            (i, line)
            for i, line in enumerate(text.splitlines(), 1)
            if "encode_mqtt_payload(" in line
            and not line.lstrip().startswith("#")
        ]
        for i, line in encode_lines:
            window = "\n".join(text.splitlines()[max(0, i - 12):i + 2])
            self.assertIn(
                "should_compress",
                window,
                f"coordinator.py:{i} encodes without a firmware check: "
                f"{line.strip()}",
            )


class DeprecatedImportTests(unittest.TestCase):
    """Deprecated HA aliases become hard failures on a known release."""

    def test_tracker_entity_not_imported_from_config_entry(self) -> None:
        text = (COMPONENT / "device_tracker.py").read_text()
        self.assertNotIn(
            "device_tracker.config_entry import TrackerEntity",
            text,
            "TrackerEntity must come from homeassistant.components."
            "device_tracker; the config_entry alias is removed in HA "
            "Core 2027.6",
        )


if __name__ == "__main__":
    unittest.main()
