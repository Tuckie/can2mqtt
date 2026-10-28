from canopen.objectdictionary import datatypes
from collections import defaultdict
from functools import cached_property
import logging
import json
import math
from canopen.objectdictionary import ODVariable, ODRecord
from canopen.node import RemoteNode


class OctetString(ODVariable):
    def __init__(self, name, index, subindex):
        super().__init__(name, index, subindex)
        self.data_type = datatypes.OCTET_STRING


logger = logging.getLogger(__name__)


def bool2onoff(value):
    return b"ON" if value else b"OFF"


def onoff2bool(value):
    return value == b"ON"


def scale_to_wire(
    value: float | str, min_val: float, max_val: float, max_int: int
) -> int:
    value = float(value)
    if math.isnan(value):
        return max_int
    result = round((max_int - 1) * (value - min_val) / (max_val - min_val))
    return max(0, min(max_int - 1, int(result)))


def scale_from_wire(value: int, min_val: float, max_val: float, max_int: int) -> float:
    if value == max_int:
        return math.nan
    return value * (max_val - min_val) / (max_int - 1) + min_val


def percentage_to_wire(value: float | str):
    return scale_to_wire(value, 0.0, 100.0, 255)


def percentage_from_wire(value: int):
    return scale_from_wire(value, 0.0, 100.0, 255)


def color_temp_to_wire(value: float | str) -> int:
    value = float(value)
    return scale_to_wire(value, 100.0, 1000.0, 255)


def color_temp_from_wire(value: int) -> float:
    # round to int, floats are not expected on mqtt state topic
    return int(scale_from_wire(value, 100.0, 1000.0, 255))


def brightness_to_wire(brightness: float | str):
    return scale_to_wire(brightness, 0, 254, 255)


def brightness_from_wire(value):
    return scale_from_wire(value, 0, 254, 255)


class StateMixin:
    _node_state_key_2_entity = defaultdict(dict)

    def states(self):
        yield "state_topic", str, datatypes.UNSIGNED8

    @cached_property
    def STATES(self):
        return list(self.states())

    state_map = None

    @classmethod
    def get_entity_by_node_state_key(cls, node_id, state_key):
        return cls._node_state_key_2_entity[node_id].get(state_key)

    def setup_state_topics(self, state_map):
        self.state_map = state_map
        logger.debug("setup state topics for %s, %s", self, state_map)
        for state_key in self.state_map:
            self._node_state_key_2_entity[self.node.id][state_key] = self

    def get_mqtt_config(self):
        config = super(StateMixin, self).get_mqtt_config()
        assert len(self.STATES) == len(self.state_map)
        for (topic, *_), state_key in zip(self.STATES, self.state_map):
            config[topic] = self.get_mqtt_state_topic(state_key)
        return config

    def get_mqtt_state_topic(self, state_key):
        return f"{self.mqtt_topic_prefix}/can_state_{self.node.id:03x}_{state_key:08x}"

    def get_mqtt_state(self, state_key, value):
        index = self.state_map.index(state_key)
        return self.get_mqtt_state_topic(state_key), self.STATES[index][1](value)

    def setup_object_dictionary(self, node, base_index):
        super().setup_object_dictionary(node, base_index)
        state_map = []
        index = base_index + 1
        for sub, (_, _, _type) in enumerate(self.STATES, 1):
            v = ODVariable("state", index, sub)
            v.data_type = _type
            node.object_dictionary[index].add_member(v)
            state_map.append((index << 16) | (sub << 8))
        self.setup_state_topics(state_map)

    async def mqtt_initial_publish(self, mqtt_client):
        for state_key in self.state_map:
            value = await self.node.sdo[state_key >> 16][
                (state_key >> 8) & 0xFF
            ].aget_raw()
            topic, mqtt_value = self.get_mqtt_state(state_key, value)
            await mqtt_client.publish(topic, mqtt_value, retain=False)


class CommandMixin:
    _mqtt_cmd_topic2entity = dict()

    def commands(self):
        yield "command_topic", int, datatypes.UNSIGNED8

    @cached_property
    def COMMANDS(self):
        return list(self.commands())

    command_map = None
    _topic2cmdkey = None

    @classmethod
    def get_entity_by_cmd_topic(cls, cmd_topic):
        return cls._mqtt_cmd_topic2entity.get(cmd_topic)

    def setup_command_topics(self, command_map):
        self.command_map = command_map
        self._topic2cmdkey = {}
        for cmd_key in self.command_map:
            topic = self.get_mqtt_command_topic(cmd_key)
            self._mqtt_cmd_topic2entity[topic] = self
            self._topic2cmdkey[topic] = cmd_key

    def get_mqtt_config(self):
        config = super(CommandMixin, self).get_mqtt_config()
        assert len(self.COMMANDS) == len(self.command_map)
        for (topic, *_), cmd_key in zip(self.COMMANDS, self.command_map):
            config[topic] = self.get_mqtt_command_topic(cmd_key)
        return config

    def get_mqtt_command_topic(self, cmd_key):
        return f"{self.mqtt_topic_prefix}/can_cmd_{self.node.id:03x}_{cmd_key:08x}"

    def get_can_cmd(self, topic, value):
        cmd_key = self._topic2cmdkey and self._topic2cmdkey.get(topic)
        if not cmd_key:
            raise ValueError(f"topic {topic} is not recognized")
        index = self.command_map.index(cmd_key)
        return cmd_key, self.COMMANDS[index][1](value)

    def setup_object_dictionary(self, node, base_index):
        super().setup_object_dictionary(node, base_index)
        cmd_map = []
        index = base_index + 2
        for sub, (_, _, _type) in enumerate(self.COMMANDS, 1):
            v = ODVariable("cmd", index, sub)
            v.data_type = _type
            node.object_dictionary[index].add_member(v)
            cmd_map.append((index << 16) | (sub << 8))

        self.setup_command_topics(cmd_map)


class Entity:
    _entities = {}
    NAME_PROP = 1
    TYPE_ID = None
    VERSION = 0
    PROPS = {}

    @cached_property
    def METADATA_PROPERTIES(self):
        return dict(self.canopen_metadata_properties())

    def canopen_metadata_properties(self):
        yield 1, "name"
        yield 2, "device_class"

    def __init__(self, node, entity_index, mqtt_topic_prefix, caps):
        self.node = node
        self.entity_index = entity_index
        self.mqtt_topic_prefix = mqtt_topic_prefix
        self.caps = caps

        # TODO: add some canbus id part to allow for many can busses
        self.unique_id = f"can_{self.node.id:03x}_{self.entity_index:02x}"
        self.props = {}
        self._entities[self.unique_id] = self

    @classmethod
    def entities(cls):
        return list(cls._entities.values())

    @classmethod
    def remove_entity(cls, unique_id):
        cls._entities.pop(unique_id, None)

    async def publish_config(self, mqtt_client):
        config_topic = self.get_mqtt_config_topic()
        config_payload = self.get_mqtt_config()
        logger.debug("mqtt config_topic: %r, payload: %r", config_topic, config_payload)
        await mqtt_client.publish(
            config_topic, payload=json.dumps(config_payload), retain=False
        )

    async def delete_config(self, mqtt_client):
        config_topic = self.get_mqtt_config_topic()
        logger.debug("delete mqtt config_topic: %r", config_topic)
        await mqtt_client.publish(config_topic, payload=None, retain=False)

    async def remove_config(self, mqtt_client):
        config_topic = self.get_mqtt_config_topic()
        await mqtt_client.publish(config_topic, payload=b"", retain=False)

    def set_property(self, key, value):
        self.props[key] = value

    def get_mqtt_config_topic(self):
        return f"{self.mqtt_topic_prefix}/{self.TYPE_NAME}/{self.unique_id}/config"

    def get_mqtt_config(self):
        cfg = {
            # "object_id": self.unique_id,
            "unique_id": self.unique_id,
            "availability": [
                {
                    "topic": self.node.availability_topic,
                },
                {
                    "topic": f"{self.mqtt_topic_prefix}/can2mqtt/status",
                },
            ],
            "availability_mode": "all",
            "device": {
                "identifiers": [f"canopen_node_{self.node.id}"],
                "name": self.node.device_name,
                "sw_version": self.node.sw_version or "",
                "hw_version": self.node.hw_version or "",
                "manufacturer": "mrk",
                "model": "esphome-canopen",
            },
        }
        cfg.update(self.PROPS)
        cfg.update(self.props)
        return cfg

    def __str__(self):
        return f"{self.__class__.__name__}(node={self.node.id}, entity_index={self.entity_index})"

    def __repr__(self):
        args = []
        args.append(f"node_id=0x{self.node.id:02x}")
        args.append(f"entity_index={self.entity_index}")
        args.append(f"props={self.props}")
        args_str = ", ".join(args)
        return f"{self.__class__.__name__}({args_str})"

    def setup_object_dictionary(self, node: RemoteNode, base_index):
        node.object_dictionary.add_object(
            ODRecord(f"node {node.id:02x} metadata", base_index)
        )
        node.object_dictionary[base_index].add_member(
            OctetString("name", base_index, 1)
        )
        node.object_dictionary[base_index].add_member(
            OctetString("device_class", base_index, 2)
        )

    def set_metadata_property(self, key, value):
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        name = self.METADATA_PROPERTIES.get(key)
        if name:
            logger.debug("\t%s %s: %s", name, key, value)
            self.set_property(name, value)
        else:
            logger.warning("\tunknown metadata property %s: %s", key, value)

    async def mqtt_initial_publish(self, _mqtt_client):
        pass


class EntityRegistry:
    _by_type = {}

    @classmethod
    def register(cls, entity_class):
        cls._by_type[(entity_class.TYPE_ID, entity_class.VERSION)] = entity_class
        return entity_class

    @classmethod
    def create(cls, type_id, node, entity_index, mqtt_topic_prefix):
        version = (type_id >> 8) & 0xFF
        caps = (type_id >> 16) & 0xFFFF
        type_id = type_id & 0xFF
        logger.info("type_id: %s, version: %s, caps: %s", type_id, version, caps)

        return cls._by_type[(type_id, version)](
            node, entity_index, mqtt_topic_prefix, caps
        )


@EntityRegistry.register
class Update(Entity):
    TYPE_ID = 255
    TYPE_NAME = "update"
    STATES = [
        ("state_topic", str, str),
    ]

    disable_upload = False
    flags = 0

    def get_state_topic(self):
        return f"{self.mqtt_topic_prefix}/node_state_{self.node.id:03x}/update"

    def get_json_attributes_topic(self):
        return f"{self.mqtt_topic_prefix}/node_json_attr_{self.node.id:03x}/update"

    def get_command_topic(self):
        return (
            f"{self.mqtt_topic_prefix}/node_cmd_{self.node.id:03x}/update/{self.flags}"
        )

    def get_mqtt_config(self):
        config = super().get_mqtt_config()
        config["state_topic"] = self.get_state_topic()
        config["json_attributes_topic"] = self.get_json_attributes_topic()

        config["device_class"] = "firmware"
        config["name"] = "Update"
        config["icon"] = "mdi:file-download-outline"
        if not self.disable_upload:
            config["command_topic"] = self.get_command_topic()
        config["payload_install"] = "install"
        return config

    async def publish_version(self, mqtt_client, ver):
        state_topic = self.get_state_topic()
        json_attr_topic = self.get_json_attributes_topic()
        payload = json.dumps(
            {
                "installed_version": self.node.sw_version,
                "latest_version": ver or self.node.sw_version,
            }
        )
        logger.info("publish_version: %s", payload)
        await mqtt_client.publish(state_topic, payload=payload, retain=False)
        await mqtt_client.publish(json_attr_topic, payload=payload, retain=False)

    async def mqtt_initial_publish(self, mqtt_client):
        await self.publish_version(mqtt_client, None)

    async def publish_progress(self, mqtt_client, pos, size):
        if not size:
            return
        json_attr_topic = self.get_json_attributes_topic()
        payload = json.dumps(
            {
                "in_progress": True,
                "update_percentage": pos * 100 // size,
            }
        )
        await mqtt_client.publish(json_attr_topic, payload=payload, retain=False)


@EntityRegistry.register
class NMTStateSensor(Entity):
    TYPE_ID = 0
    TYPE_NAME = "sensor"
    STATES = [
        ("state_topic", str, datatypes.UNSIGNED8),
    ]

    def get_state_topic(self):
        return f"{self.mqtt_topic_prefix}/can_state_{self.node.id:03x}_nmt_state"

    def get_mqtt_config(self):
        config = super().get_mqtt_config()
        config["state_topic"] = self.get_state_topic()
        return config


def float_to_str(value):
    return not math.isnan(value) and str(value) or ""


@EntityRegistry.register
class Sensor(StateMixin, Entity):
    TYPE_ID = 1
    TYPE_NAME = "sensor"

    def states(self):
        yield "state_topic", float_to_str, datatypes.REAL32

    def canopen_metadata_properties(self):
        yield from super().canopen_metadata_properties()
        yield 3, "unit_of_measurement"
        yield 4, "state_class"

    def setup_object_dictionary(self, node: RemoteNode, base_index):
        super().setup_object_dictionary(node, base_index)
        logger.info("sensor, setup od")
        node.object_dictionary[base_index].add_member(
            OctetString("unit_of_measurement", base_index, 3)
        )
        node.object_dictionary[base_index].add_member(
            OctetString("state_class", base_index, 4)
        )


class MinMaxValueMixin:
    def canopen_metadata_properties(self):
        yield from super().canopen_metadata_properties()
        yield 7, "min_value"
        yield 8, "max_value"

    def setup_object_dictionary(self, node, base_index):
        super().setup_object_dictionary(node, base_index)
        logger.info("min max, setup od")
        v = ODVariable("meta_min_value", base_index, 7)
        v.data_type = datatypes.REAL32
        node.object_dictionary[base_index].add_member(v)
        v = ODVariable("meta_min_value", base_index, 8)
        v.data_type = datatypes.REAL32
        node.object_dictionary[base_index].add_member(v)

    def get_mqtt_state(self, state_key, value):
        if value == self.N_LEVELS:
            value2 = math.nan
        else:
            min_val = self.props.get("min_value", 0)
            max_val = self.props.get("max_value", self.N_LEVELS - 1)
            value2 = scale_from_wire(value, min_val, max_val, self.N_LEVELS)
        return super().get_mqtt_state(state_key, value2)

    # TODO: add scaling for commands in get_can_cmd


@EntityRegistry.register
class Sensor8(MinMaxValueMixin, Sensor):
    TYPE_ID = 6
    TYPE_NAME = "sensor"
    N_LEVELS = 255

    def states(self):
        yield "state_topic", float_to_str, datatypes.UNSIGNED8


@EntityRegistry.register
class Sensor16(MinMaxValueMixin, Sensor):
    TYPE_ID = 7
    TYPE_NAME = "sensor"
    N_LEVELS = 65535

    def states(self):
        yield "state_topic", float_to_str, datatypes.UNSIGNED16


@EntityRegistry.register
class BinarySensor(StateMixin, Entity):
    TYPE_ID = 2
    TYPE_NAME = "binary_sensor"

    def states(self):
        yield "state_topic", bool2onoff, datatypes.UNSIGNED8


@EntityRegistry.register
class Switch(StateMixin, CommandMixin, Entity):
    TYPE_ID = 3
    TYPE_NAME = "switch"
    PROPS = {"assumed_state": False}

    def states(self):
        yield "state_topic", bool2onoff, datatypes.UNSIGNED8

    def commands(self):
        yield "command_topic", onoff2bool, datatypes.UNSIGNED8


@EntityRegistry.register
class Light(StateMixin, CommandMixin, Entity):
    TYPE_ID = 5
    TYPE_NAME = "light"

    PROPS = {"assumed_state": False, "supported_color_modes": ["color_temp"]}

    def states(self):
        yield "state_topic", bool2onoff, datatypes.UNSIGNED8
        yield "brightness_state_topic", brightness_from_wire, datatypes.UNSIGNED8
        yield "color_temp_state_topic", color_temp_from_wire, datatypes.UNSIGNED8

    def commands(self):
        yield "command_topic", onoff2bool, datatypes.UNSIGNED8
        yield "brightness_command_topic", brightness_to_wire, datatypes.UNSIGNED8
        yield "color_temp_command_topic", color_temp_to_wire, datatypes.UNSIGNED8

    def canopen_metadata_properties(self):
        yield from super().canopen_metadata_properties()
        yield 7, "min_mireds"
        yield 8, "max_mireds"

    def setup_object_dictionary(self, node, base_index):
        super().setup_object_dictionary(node, base_index)
        # let's reuse 7 and 8 indices for min/max mireds
        v = ODVariable("min_mireds", base_index, 7)
        v.data_type = datatypes.REAL32
        node.object_dictionary[base_index].add_member(v)
        v = ODVariable("max_mireds", base_index, 8)
        v.data_type = datatypes.REAL32
        node.object_dictionary[base_index].add_member(v)


@EntityRegistry.register
class LightV1(StateMixin, CommandMixin, Entity):
    TYPE_ID = 5
    VERSION = 1
    TYPE_NAME = "light"

    def states(self):
        yield ("state_topic", bool2onoff, datatypes.UNSIGNED8)
        if self.supports_brightness():
            yield ("brightness_state_topic", brightness_from_wire, datatypes.UNSIGNED8)
        if self.supports_color_temp():
            yield ("color_temp_state_topic", color_temp_from_wire, datatypes.UNSIGNED8)

    def commands(self):
        yield ("command_topic", onoff2bool, datatypes.UNSIGNED8)
        if self.supports_brightness():
            yield ("brightness_command_topic", brightness_to_wire, datatypes.UNSIGNED8)
        if self.supports_color_temp():
            yield ("color_temp_command_topic", color_temp_to_wire, datatypes.UNSIGNED8)

    def canopen_metadata_properties(self):
        yield from super().canopen_metadata_properties()
        if self.supports_color_temp():
            yield 7, "min_mireds"
            yield 8, "max_mireds"

    def get_props(self):
        color_modes = {
            1: "onoff",
            2: "brightness",
            4: "color_temp",
        }

        supported_color_modes = [v for k, v in color_modes.items() if self.caps & k]

        yield "supported_color_modes", supported_color_modes

    @cached_property
    def PROPS(self):
        return dict(self.get_props())

    def supports_brightness(self):
        return self.caps & (4 | 2)

    def supports_color_temp(self):
        return self.caps & 4

    def setup_object_dictionary(self, node, base_index):
        super().setup_object_dictionary(node, base_index)
        # let's reuse 7 and 8 indices for min/max mireds
        v = ODVariable("min_mireds", base_index, 7)
        v.data_type = datatypes.REAL32
        node.object_dictionary[base_index].add_member(v)
        v = ODVariable("max_mireds", base_index, 8)
        v.data_type = datatypes.REAL32
        node.object_dictionary[base_index].add_member(v)


@EntityRegistry.register
class Cover(StateMixin, CommandMixin, Entity):
    TYPE_ID = 4
    TYPE_NAME = "cover"

    PROPS = {
        "position_closed": 0,
        "position_open": 100,
    }

    STATES_DICT = {
        0: "open",
        1: "opening",
        2: "closed",
        3: "closing",
    }

    CMDS = {
        b"STOP": 0,
        b"OPEN": 1,
        b"CLOSE": 2,
    }

    def states(self):
        yield "state_topic", self.STATES_DICT.get, datatypes.UNSIGNED8
        yield "position_topic", percentage_from_wire, datatypes.UNSIGNED8

    def commands(self):
        yield "command_topic", self.CMDS.get, datatypes.UNSIGNED8
        yield "set_position_topic", percentage_to_wire, datatypes.UNSIGNED8


@EntityRegistry.register
class CoverV1(StateMixin, CommandMixin, Entity):
    TYPE_ID = 4
    VERSION = 1
    TYPE_NAME = "cover"

    def get_props(self):
        if self.caps & 1:
            yield "position_closed", 0
            yield "position_open", 100

    @cached_property
    def PROPS(self):
        return dict(self.get_props())

    STATES_DICT = {
        0: "open",
        1: "opening",
        2: "closed",
        3: "closing",
    }

    CMDS = {
        b"STOP": 0,
        b"OPEN": 1,
        b"CLOSE": 2,
    }

    def states(self):
        yield ("state_topic", self.STATES_DICT.get, datatypes.UNSIGNED8)
        if self.caps & 1:
            yield ("position_topic", percentage_from_wire, datatypes.UNSIGNED8)
        if self.caps & 2:
            yield ("tilt_status_topic", percentage_from_wire, datatypes.UNSIGNED8)

    def commands(self):
        yield ("command_topic", self.CMDS.get, datatypes.UNSIGNED8)
        if self.caps & 1:
            yield ("set_position_topic", percentage_to_wire, datatypes.UNSIGNED8)
        if self.caps & 2:
            yield ("tilt_command_topic", percentage_to_wire, datatypes.UNSIGNED8)


@EntityRegistry.register
class Number(StateMixin, CommandMixin, Entity):
    TYPE_ID = 8
    TYPE_NAME = "number"

    def states(self):
        yield "state_topic", float_to_str, datatypes.REAL32

    def commands(self):
        yield "command_topic", float, datatypes.REAL32


@EntityRegistry.register
class Number8(MinMaxValueMixin, StateMixin, CommandMixin, Entity):
    TYPE_ID = 9
    TYPE_NAME = "number"
    N_LEVELS = 255

    def states(self):
        yield "state_topic", float_to_str, datatypes.UNSIGNED8

    def commands(self):
        yield "command_topic", int, datatypes.UNSIGNED8


@EntityRegistry.register
class Number16(MinMaxValueMixin, StateMixin, CommandMixin, Entity):
    TYPE_ID = 10
    TYPE_NAME = "number"
    N_LEVELS = 65535

    def states(self):
        yield "state_topic", float_to_str, datatypes.UNSIGNED16

    def commands(self):
        yield "command_topic", int, datatypes.UNSIGNED16


ALARM_COMMANDS = {
    b"DISARM": 0,
    b"ARM_AWAY": 1,
    b"ARM_HOME": 2,
    b"ARM_NIGHT": 3,
    b"ARM_VACATION": 4,
    b"ARM_CUSTOM_BYPASS": 5,
    b"TRIGGER": 127,
}

ALARM_STATES = {
    0: b"disarmed",
    1: b"armed_home",
    2: b"armed_away",
    3: b"armed_night",
    4: b"armed_vacation",
    5: b"armed_custom_bypass",
    6: b"pending",
    7: b"arming",
    8: b"disarming",
    9: b"triggered",
}


@EntityRegistry.register
class Alarm(StateMixin, CommandMixin, Entity):
    TYPE_ID = 16
    TYPE_NAME = "alarm_control_panel"

    def states(self):
        yield "state_topic", ALARM_STATES.get, datatypes.UNSIGNED8

    def commands(self):
        yield "command_topic", ALARM_COMMANDS.get, datatypes.UNSIGNED8

    PROPS = {
        "assumed_state": False,
        "code_arm_required": False,
        "code_disarm_requried": False,
        "code_trigger_required": False,
    }


# Climate, see "Climate" in esphome-canopen OBJECT_DICTIONARY.md
CLIMATE_MODES = ["off", "heat_cool", "cool", "heat", "fan_only", "dry", "auto"]
CLIMATE_ACTIONS = {
    0: "off",
    2: "cooling",
    3: "heating",
    4: "idle",
    5: "drying",
    6: "fan",
    7: "defrosting",
}
CLIMATE_FAN_MODES = [
    "on",
    "off",
    "auto",
    "low",
    "medium",
    "high",
    "middle",
    "focus",
    "diffuse",
    "quiet",
]
CLIMATE_SWING_MODES = ["off", "both", "vertical", "horizontal"]
CLIMATE_PRESETS = [
    "none",
    "home",
    "away",
    "boost",
    "comfort",
    "eco",
    "sleep",
    "activity",
]
# codes of custom fan modes / presets start here (after the standard ones)
CLIMATE_CUSTOM_FAN_MODE_BASE = 10
CLIMATE_CUSTOM_PRESET_BASE = 8
CLIMATE_CODE_NONE = 255

# temperatures: UINT16 scaled to -327.67..327.67 degC (0.01 degC steps), 65535 unknown
TEMPERATURE_MIN = -327.67
TEMPERATURE_MAX = 327.67

CLIMATE_CAP_CURRENT_TEMPERATURE = 1
CLIMATE_CAP_TWO_POINT = 2
CLIMATE_CAP_ACTION = 4
CLIMATE_CAP_FAN_MODE = 8
CLIMATE_CAP_SWING_MODE = 16
CLIMATE_CAP_PRESET = 32
CLIMATE_CAP_CURRENT_HUMIDITY = 64
CLIMATE_CAP_TARGET_HUMIDITY = 128

# metadata subindices
CLIMATE_META_MODES = 9
CLIMATE_META_TEMP_STEP = 10
CLIMATE_META_FAN_MODES = 11
CLIMATE_META_SWING_MODES = 12
CLIMATE_META_PRESETS = 13
CLIMATE_META_CUSTOM_FAN_MODES = 14
CLIMATE_META_CUSTOM_PRESETS = 15
CLIMATE_META_MIN_HUMIDITY = 16
CLIMATE_META_MAX_HUMIDITY = 17


def _payload_str(value: float | bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def temperature_to_wire(value: float | bytes | str) -> int:
    return scale_to_wire(_payload_str(value), TEMPERATURE_MIN, TEMPERATURE_MAX, 65535)


def temperature_from_wire(value: int) -> str:
    value = scale_from_wire(value, TEMPERATURE_MIN, TEMPERATURE_MAX, 65535)
    # "None" clears the value in Home Assistant's MQTT climate
    return "None" if math.isnan(value) else f"{value:.2f}"


def humidity_to_wire(value: float | bytes | str) -> int:
    return scale_to_wire(_payload_str(value), 0, 254, 255)


def humidity_from_wire(value: int) -> str:
    value = scale_from_wire(value, 0, 254, 255)
    return "None" if math.isnan(value) else f"{value:.0f}"


def climate_mode_from_wire(value: int) -> str:
    return CLIMATE_MODES[value] if value < len(CLIMATE_MODES) else "off"


def _name_to_code(payload, names, custom=(), custom_base=None):
    name = _payload_str(payload).strip()
    if name.lower() in names:
        return names.index(name.lower())
    if name in custom:
        return custom_base + custom.index(name)
    raise ValueError(
        f"unknown value {name!r}, valid: {', '.join(list(names) + list(custom))}"
    )


def climate_mode_to_wire(payload: bytes) -> int:
    return _name_to_code(payload, CLIMATE_MODES)


def _mask_names(mask, names):
    return [name for code, name in enumerate(names) if mask & (1 << code)]


def _split_names(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return [name for name in (value or "").split("\n") if name]


@EntityRegistry.register
class Climate(StateMixin, CommandMixin, Entity):
    TYPE_ID = 11
    VERSION = 1
    TYPE_NAME = "climate"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_fan_modes = []
        self.custom_presets = []

    @cached_property
    def PROPS(self):
        return {
            "temperature_unit": "C",
            "precision": 0.1,
            "modes": CLIMATE_MODES,
        }

    def has(self, cap):
        return bool(self.caps & cap)

    # wire <-> MQTT payloads of fan modes / presets, custom names from the node metadata

    def fan_mode_from_wire(self, value):
        if value < len(CLIMATE_FAN_MODES):
            return CLIMATE_FAN_MODES[value]
        pos = value - CLIMATE_CUSTOM_FAN_MODE_BASE
        return (
            self.custom_fan_modes[pos] if 0 <= pos < len(self.custom_fan_modes) else ""
        )

    def fan_mode_to_wire(self, payload):
        return _name_to_code(
            payload,
            CLIMATE_FAN_MODES,
            self.custom_fan_modes,
            CLIMATE_CUSTOM_FAN_MODE_BASE,
        )

    def preset_from_wire(self, value):
        if value < len(CLIMATE_PRESETS):
            return CLIMATE_PRESETS[value]
        pos = value - CLIMATE_CUSTOM_PRESET_BASE
        # 255 (no preset) as "none", which Home Assistant shows as no preset
        return (
            self.custom_presets[pos] if 0 <= pos < len(self.custom_presets) else "none"
        )

    def preset_to_wire(self, payload):
        return _name_to_code(
            payload, CLIMATE_PRESETS, self.custom_presets, CLIMATE_CUSTOM_PRESET_BASE
        )

    @staticmethod
    def swing_mode_from_wire(value):
        return CLIMATE_SWING_MODES[value] if value < len(CLIMATE_SWING_MODES) else ""

    @staticmethod
    def swing_mode_to_wire(payload):
        return _name_to_code(payload, CLIMATE_SWING_MODES)

    def writable(self):
        """(state topic, command topic, from wire, to wire, data type) of the writable states,
        in the node's state / command order."""
        yield (
            "mode_state_topic",
            "mode_command_topic",
            climate_mode_from_wire,
            climate_mode_to_wire,
            datatypes.UNSIGNED8,
        )
        if self.has(CLIMATE_CAP_TWO_POINT):
            yield (
                "temperature_low_state_topic",
                "temperature_low_command_topic",
                temperature_from_wire,
                temperature_to_wire,
                datatypes.UNSIGNED16,
            )
            yield (
                "temperature_high_state_topic",
                "temperature_high_command_topic",
                temperature_from_wire,
                temperature_to_wire,
                datatypes.UNSIGNED16,
            )
        else:
            yield (
                "temperature_state_topic",
                "temperature_command_topic",
                temperature_from_wire,
                temperature_to_wire,
                datatypes.UNSIGNED16,
            )
        if self.has(CLIMATE_CAP_FAN_MODE):
            yield (
                "fan_mode_state_topic",
                "fan_mode_command_topic",
                self.fan_mode_from_wire,
                self.fan_mode_to_wire,
                datatypes.UNSIGNED8,
            )
        if self.has(CLIMATE_CAP_SWING_MODE):
            yield (
                "swing_mode_state_topic",
                "swing_mode_command_topic",
                self.swing_mode_from_wire,
                self.swing_mode_to_wire,
                datatypes.UNSIGNED8,
            )
        if self.has(CLIMATE_CAP_PRESET):
            yield (
                "preset_mode_state_topic",
                "preset_mode_command_topic",
                self.preset_from_wire,
                self.preset_to_wire,
                datatypes.UNSIGNED8,
            )
        if self.has(CLIMATE_CAP_TARGET_HUMIDITY):
            yield (
                "target_humidity_state_topic",
                "target_humidity_command_topic",
                humidity_from_wire,
                humidity_to_wire,
                datatypes.UNSIGNED8,
            )

    def states(self):
        # order of the node's OD states (layout.py entity_state_kinds())
        for state_topic, _, from_wire, _, data_type in self.writable():
            yield (state_topic, from_wire, data_type)
        if self.has(CLIMATE_CAP_ACTION):
            yield (
                "action_topic",
                lambda v: CLIMATE_ACTIONS.get(v, "off"),
                datatypes.UNSIGNED8,
            )
        if self.has(CLIMATE_CAP_CURRENT_TEMPERATURE):
            yield (
                "current_temperature_topic",
                temperature_from_wire,
                datatypes.UNSIGNED16,
            )
        if self.has(CLIMATE_CAP_CURRENT_HUMIDITY):
            yield ("current_humidity_topic", humidity_from_wire, datatypes.UNSIGNED8)

    def commands(self):
        # only the writable states have commands, in state order
        for _, command_topic, _, to_wire, data_type in self.writable():
            yield (command_topic, to_wire, data_type)

    def get_mqtt_config(self):
        config = super().get_mqtt_config()
        presets = config.pop("presets", [])  # not a Home Assistant option
        if self.has(CLIMATE_CAP_FAN_MODE):
            config["fan_modes"] = config.get("fan_modes", []) + self.custom_fan_modes
        if self.has(CLIMATE_CAP_SWING_MODE):
            config.setdefault("swing_modes", [])
        if self.has(CLIMATE_CAP_PRESET):
            # Home Assistant adds "none" (no preset) itself
            config["preset_modes"] = [
                p for p in presets if p != "none"
            ] + self.custom_presets
        return config

    METADATA = {
        1: ("name", datatypes.VISIBLE_STRING),
        7: ("min_temp", datatypes.REAL32),
        8: ("max_temp", datatypes.REAL32),
        CLIMATE_META_MODES: ("modes", datatypes.UNSIGNED32),
        CLIMATE_META_TEMP_STEP: ("temp_step", datatypes.REAL32),
        CLIMATE_META_FAN_MODES: ("fan_modes", datatypes.UNSIGNED32),
        CLIMATE_META_SWING_MODES: ("swing_modes", datatypes.UNSIGNED32),
        CLIMATE_META_PRESETS: ("presets", datatypes.UNSIGNED32),
        CLIMATE_META_CUSTOM_FAN_MODES: ("custom_fan_modes", datatypes.OCTET_STRING),
        CLIMATE_META_CUSTOM_PRESETS: ("custom_presets", datatypes.OCTET_STRING),
        CLIMATE_META_MIN_HUMIDITY: ("min_humidity", datatypes.REAL32),
        CLIMATE_META_MAX_HUMIDITY: ("max_humidity", datatypes.REAL32),
    }

    def canopen_metadata_properties(self):
        for sub, (name, _) in self.METADATA.items():
            yield sub, name

    def set_metadata_property(self, key, value):
        masks = {
            CLIMATE_META_MODES: ("modes", CLIMATE_MODES),
            CLIMATE_META_FAN_MODES: ("fan_modes", CLIMATE_FAN_MODES),
            CLIMATE_META_SWING_MODES: ("swing_modes", CLIMATE_SWING_MODES),
            CLIMATE_META_PRESETS: ("presets", CLIMATE_PRESETS),
        }
        if key in masks:
            prop, names = masks[key]
            selected = _mask_names(value, names)
            if prop == "modes" and not selected:
                selected = CLIMATE_MODES  # nothing reported: offer all
            logger.debug("\t%s %s: %s", prop, key, selected)
            self.set_property(prop, selected)
            return
        if key == CLIMATE_META_CUSTOM_FAN_MODES:
            self.custom_fan_modes = _split_names(value)
            return
        if key == CLIMATE_META_CUSTOM_PRESETS:
            self.custom_presets = _split_names(value)
            return
        if isinstance(value, float) and math.isnan(value):
            return
        super().set_metadata_property(key, value)

    def setup_object_dictionary(self, node, base_index):
        super().setup_object_dictionary(node, base_index)
        for sub, (name, data_type) in self.METADATA.items():
            if sub in (1, 2):  # added by Entity
                continue
            if data_type == datatypes.OCTET_STRING:
                node.object_dictionary[base_index].add_member(
                    OctetString(name, base_index, sub)
                )
                continue
            v = ODVariable(name, base_index, sub)
            v.data_type = data_type
            node.object_dictionary[base_index].add_member(v)
