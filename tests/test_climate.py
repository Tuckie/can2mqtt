"""Climate entity: OD layout, MQTT discovery config, state / command conversions (run: pytest tests/)."""

import math
import os
import sys

import canopen
import pytest
from canopen.objectdictionary import ODRecord

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from can2mqtt import entities as e  # noqa: E402

PREFIX = "homeassistant"
ALL = 255


def make_climate(caps, entity_index=22):
    node = canopen.RemoteNode(0x21, canopen.ObjectDictionary())
    node.device_name = "node"
    node.sw_version = node.hw_version = ""
    node.availability_topic = f"{PREFIX}/can_021/availability"
    entity = e.EntityRegistry.create(
        e.Climate.TYPE_ID | (1 << 8) | (caps << 16), node, entity_index, PREFIX
    )
    base = 0x2000 + entity_index * 16
    node.object_dictionary.add_object(ODRecord("states", base + 1))
    node.object_dictionary.add_object(ODRecord("cmds", base + 2))
    entity.setup_object_dictionary(node, base)
    return entity, base


def state(entity, topic, raw):
    """MQTT payload of a raw state value, by its discovery topic key."""
    index = [t for t, *_ in entity.STATES].index(topic)
    return entity.get_mqtt_state(entity.state_map[index], raw)[1]


def command(entity, topic, payload):
    return entity.get_can_cmd(entity.get_mqtt_config()[topic], payload)


def test_single_point_with_action_and_current():
    entity, base = make_climate(caps=5)
    assert isinstance(entity, e.Climate)
    for key, value in ((1, b"Thermostat"), (7, 7.0), (8, 35.0), (9, 0b1001), (10, 0.5)):
        entity.set_metadata_property(key, value)
    cfg = entity.get_mqtt_config()
    assert cfg["modes"] == ["off", "heat"]
    assert (cfg["min_temp"], cfg["max_temp"], cfg["temp_step"]) == (7.0, 35.0, 0.5)
    assert cfg["temperature_unit"] == "C"
    assert [t for t, *_ in entity.STATES] == [
        "mode_state_topic",
        "temperature_state_topic",
        "action_topic",
        "current_temperature_topic",
    ]
    # state subindices 1..4, commands only for mode and target temperature
    assert entity.state_map == [((base + 1) << 16) | (sub << 8) for sub in (1, 2, 3, 4)]
    assert entity.command_map == [((base + 2) << 16) | (sub << 8) for sub in (1, 2)]
    assert "fan_mode_command_topic" not in cfg and "preset_modes" not in cfg

    assert state(entity, "mode_state_topic", 3) == "heat"
    assert (
        state(entity, "temperature_state_topic", e.temperature_to_wire(21.5)) == "21.50"
    )
    assert state(entity, "action_topic", 3) == "heating"
    assert state(entity, "current_temperature_topic", 65535) == "None"

    assert command(entity, "mode_command_topic", b"heat")[1] == 3
    key, wire = command(entity, "temperature_command_topic", b"21.5")
    assert key == entity.command_map[1] and wire == round(100 * (21.5 + 327.67))


def test_all_features():
    entity, base = make_climate(caps=ALL)
    for key, value in (
        (11, (1 << 2) | (1 << 3) | (1 << 5)),  # auto, low, high
        (12, 0b0011),  # off, both
        (13, 0b0111),  # none, home, away
        (14, b"Turbo\nWhisper"),
        (15, b"Vacation"),
        (16, 30.0),
        (17, 70.0),
    ):
        entity.set_metadata_property(key, value)
    cfg = entity.get_mqtt_config()
    assert cfg["fan_modes"] == ["auto", "low", "high", "Turbo", "Whisper"]
    assert cfg["swing_modes"] == ["off", "both"]
    assert cfg["preset_modes"] == ["home", "away", "Vacation"]
    assert (cfg["min_humidity"], cfg["max_humidity"]) == (30.0, 70.0)
    assert "presets" not in cfg

    assert [t for t, *_ in entity.STATES] == [
        "mode_state_topic",
        "temperature_low_state_topic",
        "temperature_high_state_topic",
        "fan_mode_state_topic",
        "swing_mode_state_topic",
        "preset_mode_state_topic",
        "target_humidity_state_topic",
        "action_topic",
        "current_temperature_topic",
        "current_humidity_topic",
    ]
    # 7 writable states, each with its command at the same subindex
    assert entity.command_map == [
        ((base + 2) << 16) | (sub << 8) for sub in range(1, 8)
    ]
    assert [t for t, *_ in entity.COMMANDS] == [
        "mode_command_topic",
        "temperature_low_command_topic",
        "temperature_high_command_topic",
        "fan_mode_command_topic",
        "swing_mode_command_topic",
        "preset_mode_command_topic",
        "target_humidity_command_topic",
    ]

    assert state(entity, "fan_mode_state_topic", 5) == "high"
    assert state(entity, "fan_mode_state_topic", 11) == "Whisper"
    assert state(entity, "fan_mode_state_topic", 255) == ""
    assert state(entity, "swing_mode_state_topic", 1) == "both"
    assert state(entity, "preset_mode_state_topic", 2) == "away"
    assert state(entity, "preset_mode_state_topic", 8) == "Vacation"
    assert state(entity, "preset_mode_state_topic", 255) == "none"
    assert state(entity, "target_humidity_state_topic", 45) == "45"
    assert state(entity, "current_humidity_topic", 255) == "None"

    assert command(entity, "fan_mode_command_topic", b"Turbo")[1] == 10
    assert command(entity, "fan_mode_command_topic", b"low")[1] == 3
    assert command(entity, "swing_mode_command_topic", b"both")[1] == 1
    assert command(entity, "preset_mode_command_topic", b"none")[1] == 0
    assert command(entity, "preset_mode_command_topic", b"Vacation")[1] == 8
    assert command(entity, "target_humidity_command_topic", b"55")[1] == 55
    with pytest.raises(ValueError):
        command(entity, "fan_mode_command_topic", b"Hurricane")


def test_mode_errors_and_defaults():
    entity, _ = make_climate(caps=0)
    entity.set_metadata_property(9, 0)  # no modes reported: offer all
    assert entity.get_mqtt_config()["modes"] == e.CLIMATE_MODES
    with pytest.raises(ValueError):
        e.climate_mode_to_wire(b"turbo")
    assert math.isclose(
        float(e.temperature_from_wire(e.temperature_to_wire(-12.34))), -12.34
    )
