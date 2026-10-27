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


def make_climate(caps, entity_index=22):
    node = canopen.RemoteNode(0x21, canopen.ObjectDictionary())
    node.device_name = "node"
    node.sw_version = node.hw_version = ""
    node.availability_topic = f"{PREFIX}/can_021/availability"
    entity = e.EntityRegistry.create(e.Climate.TYPE_ID | (1 << 8) | (caps << 16), node, entity_index, PREFIX)
    base = 0x2000 + entity_index * 16
    node.object_dictionary.add_object(ODRecord("states", base + 1))
    node.object_dictionary.add_object(ODRecord("cmds", base + 2))
    entity.setup_object_dictionary(node, base)
    return entity, base


def test_single_point_with_action_and_current():
    entity, base = make_climate(caps=5)
    assert isinstance(entity, e.Climate)
    for key, value in ((1, b"Thermostat"), (7, 7.0), (8, 35.0), (9, 0b1001), (10, 0.5)):
        entity.set_metadata_property(key, value)
    cfg = entity.get_mqtt_config()
    assert cfg["modes"] == ["off", "heat"]
    assert (cfg["min_temp"], cfg["max_temp"], cfg["temp_step"]) == (7.0, 35.0, 0.5)
    assert cfg["temperature_unit"] == "C"
    states = [topic for topic, *_ in entity.STATES]
    assert states == ["mode_state_topic", "temperature_state_topic", "action_topic", "current_temperature_topic"]
    # state subindices 1..4, commands only for mode and target temperature
    assert entity.state_map == [((base + 1) << 16) | (sub << 8) for sub in (1, 2, 3, 4)]
    assert entity.command_map == [((base + 2) << 16) | (sub << 8) for sub in (1, 2)]
    assert "temperature_low_command_topic" not in cfg

    assert entity.get_mqtt_state(entity.state_map[0], 3)[1] == "heat"
    assert entity.get_mqtt_state(entity.state_map[1], e.temperature_to_wire(21.5))[1] == "21.50"
    assert entity.get_mqtt_state(entity.state_map[2], 3)[1] == "heating"
    assert entity.get_mqtt_state(entity.state_map[3], 65535)[1] == "None"

    assert entity.get_can_cmd(cfg["mode_command_topic"], b"heat")[1] == 3
    key, wire = entity.get_can_cmd(cfg["temperature_command_topic"], b"21.5")
    assert key == entity.command_map[1] and wire == round(100 * (21.5 + 327.67))


def test_two_point():
    entity, _ = make_climate(caps=7)
    cfg = entity.get_mqtt_config()
    assert [topic for topic, *_ in entity.STATES][1:3] == [
        "temperature_low_state_topic",
        "temperature_high_state_topic",
    ]
    assert "temperature_state_topic" not in cfg
    assert entity.get_can_cmd(cfg["temperature_high_command_topic"], b"24")[1] == round(100 * (24 + 327.67))


def test_mode_errors_and_defaults():
    entity, _ = make_climate(caps=0)
    entity.set_metadata_property(9, 0)  # no modes reported: offer all
    assert entity.get_mqtt_config()["modes"] == e.CLIMATE_MODES
    with pytest.raises(ValueError):
        e.climate_mode_to_wire(b"turbo")
    assert math.isclose(float(e.temperature_from_wire(e.temperature_to_wire(-12.34))), -12.34)
