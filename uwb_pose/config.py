import os
import re
from typing import Dict, Tuple

from env_loader import load_repo_env


load_repo_env()


# Return an environment variable or fall back to the provided default string.
def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


# Parse an integer environment variable and fall back when it is missing or invalid.
def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


# Parse a float environment variable and fall back when it is missing or invalid.
def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default


# Parse a boolean environment variable and fall back when it is missing.
def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Parse a `key:value` float mapping from an environment variable.
def parse_float_mapping(value: str, default: Dict[str, float]) -> Dict[str, float]:
    parsed = {str(key): float(number) for key, number in default.items()}
    if value in (None, ""):
        return parsed

    for part in re.split(r"[;,]", value):
        item = part.strip()
        if not item:
            continue
        key, sep, raw_number = item.partition(":")
        if not sep:
            continue
        key = key.strip()
        if not key:
            continue
        try:
            parsed[key] = float(raw_number.strip())
        except ValueError:
            continue

    return parsed


# Derive a stable slot index from the node id so nodes can stagger their startup timing.
def infer_node_slot_index(node_id: str) -> int:
    match = re.search(r"(\d+)$", node_id)
    if match is not None:
        try:
            return max(int(match.group(1)) - 1, 0)
        except ValueError:
            pass
    if node_id in ("", "local"):
        return 0
    slot = 0
    for char in node_id:
        slot = ((slot * 131) + ord(char)) % 32
    return slot


# Build the default UWB init command from a comma-separated peer address list.
def build_init_command(peer_addrs_csv: str) -> bytes:
    peer_addrs = [part.strip() for part in peer_addrs_csv.split(",") if part.strip()]
    if not peer_addrs:
        peer_addrs = ["1", "2", "3", "4", "5"]
    return f"initf -multi -paddr=[{','.join(peer_addrs)}]\r\n".encode("ascii")


# Convert a plain-text serial command into the CRLF-terminated bytes sent to the device.
def build_command_bytes(command_text: str) -> bytes:
    stripped = command_text.strip()
    if not stripped:
        return b""
    return f"{stripped}\r\n".encode("ascii")


# Parse a semicolon-separated command list into serial-ready command bytes.
def parse_command_list(commands_text: str, default_commands) -> Tuple[bytes, ...]:
    if commands_text in (None, ""):
        commands = list(default_commands)
    else:
        commands = [part.strip() for part in commands_text.split(";")]
    built = [build_command_bytes(command) for command in commands if command.strip()]
    return tuple(command for command in built if command)


# Add a filename prefix while preserving the original directory path.
def prefixed_path(base_path: str, prefix: str) -> str:
    if not prefix:
        return base_path
    directory, filename = os.path.split(base_path)
    if not filename:
        return base_path
    prefixed_name = f"{prefix}{filename}"
    return os.path.join(directory, prefixed_name) if directory else prefixed_name


MQTT_HOST = env_str("MQTT_HOST", "127.0.0.1")
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_TOPIC = env_str("MQTT_TOPIC", "uwb/raw/+")
MQTT_USERNAME = env_str("MQTT_USERNAME", "")
MQTT_PASSWORD = env_str("MQTT_PASSWORD", "")
MQTT_QOS = env_int("MQTT_QOS", 0)
AUTOCAM_POSE_TOPIC_BASE = env_str("AUTOCAM_POSE_TOPIC_BASE", "autocam/pose")

NODE_ID = env_str("NODE_ID", "local")
OUTPUT_PREFIX = env_str("OUTPUT_PREFIX", "" if NODE_ID == "local" else f"{NODE_ID}_")
PORT = env_str("UWB_PORT", "/dev/ttyACM0")
BAUD = env_int("UWB_BAUD", 115200)
SERIAL_TIMEOUT_S = env_float("SERIAL_TIMEOUT_S", 1.0)
SERIAL_RECONNECT_DELAY_S = env_float("SERIAL_RECONNECT_DELAY_S", 2.0)
SERIAL_READY_DELAY_S = env_float("SERIAL_READY_DELAY_S", 2.0)
NODE_SLOT_INDEX = env_int("NODE_SLOT_INDEX", infer_node_slot_index(NODE_ID))
NODE_STAGGER_STEP_S = env_float("NODE_STAGGER_STEP_S", 0.35)
NODE_STAGGER_MAX_S = env_float("NODE_STAGGER_MAX_S", 2.5)
DEFAULT_INIT_DELAY_S = min(
    max(NODE_SLOT_INDEX, 0) * NODE_STAGGER_STEP_S,
    NODE_STAGGER_MAX_S,
)
INIT_DELAY_S = env_float("INIT_DELAY_S", DEFAULT_INIT_DELAY_S)
PEER_ADDRS_CSV = env_str("UWB_PEER_ADDRS", "1,2,3,4,5")
INIT_COMMAND = env_str("UWB_INIT_COMMAND", "").encode("ascii") or build_init_command(
    PEER_ADDRS_CSV
)
PRE_INIT_COMMANDS = parse_command_list(
    env_str("UWB_PRE_INIT_COMMANDS", ""),
    default_commands=("stop",),
)
PRE_INIT_COMMAND_DELAY_S = env_float("UWB_PRE_INIT_COMMAND_DELAY_S", 0.20)
SHUTDOWN_COMMANDS = parse_command_list(
    env_str("UWB_SHUTDOWN_COMMANDS", ""),
    default_commands=("stop",),
)
SHUTDOWN_COMMAND_DELAY_S = env_float("UWB_SHUTDOWN_COMMAND_DELAY_S", 0.10)

HISTORY_PATH = env_str("CENTRAL_HISTORY_PATH", "central_history.json")
MAX_HISTORY = 1000
ANCHORS_PATH = env_str("ANCHORS_PATH", "anchors.json")
RAW_SERIAL_LOG_PATH = env_str("RAW_SERIAL_LOG_PATH", "central_raw_input_log.txt")
EVENT_LOG_PATH = env_str(
    "CENTRAL_EVENT_LOG_PATH",
    "central_pose_logger_output.txt",
)
CENTRAL_LOGGER_CONSOLE = env_bool("CENTRAL_LOGGER_CONSOLE", True)
SERIAL_DIAGNOSTIC_INTERVAL_S = env_float("SERIAL_DIAGNOSTIC_INTERVAL_S", 10.0)
PIPELINE_DIAGNOSTIC_INTERVAL_S = env_float("PIPELINE_DIAGNOSTIC_INTERVAL_S", 10.0)

DEFAULT_PROFILE_NAME = "BED"
DEFAULT_ANCHOR_PROFILES = {
    "BED": {
        "1": {"x": -0.9144, "y": -0.47625},
        "2": {"x": 0.9144, "y": -0.47625},
        "3": {"x": -0.9144, "y": 0.47625},
        "4": {"x": 0.9144, "y": 0.47625},
        "5": {"x": 0.0, "y": 0.47625},
    },
    "ECELAB": {
        "1": {"x": -2.5654, "y": -2.5908},
        "2": {"x": 3.0226, "y": -2.5908},
        "3": {"x": -2.5654, "y": 2.5654},
        "4": {"x": 3.0226, "y": 2.5654},
        "5": {"x": 0.0, "y": 2.5654},
    },
}

DEFAULT_NODE_HEIGHTS_M = {
    "rpi1": 0.0,
    "rpi2": 0.0,
    "rpi3": 0.0,
    "rpi4": 0.0,
}
NODE_HEIGHT_DEFAULT_M = env_float("NODE_HEIGHT_DEFAULT_M", 0.0)
NODE_HEIGHTS_M = parse_float_mapping(
    env_str("NODE_HEIGHTS_M", ""),
    DEFAULT_NODE_HEIGHTS_M,
)


# Return the configured wearable height for a node relative to the shared anchor plane.
def node_height_m(node_id: str) -> float:
    return float(NODE_HEIGHTS_M.get(str(node_id), NODE_HEIGHT_DEFAULT_M))

RANGE_MEDIAN_WINDOW = 3
RANGE_ALPHA_SLOW = 0.35
RANGE_ALPHA_FAST = 0.60
RANGE_FAST_DELTA_M = 0.12
RANGE_STEP_BASE_M = 0.10
RANGE_MAX_RATE_MPS = 2.2
HOLD_LAST_RANGE_S = 0.20
STALE_WEIGHT_FLOOR = 0.10
INVALID_RANGE_HOLD_S = 0.30
INVALID_RANGE_WEIGHT_FLOOR = 0.22

GEOMETRY_BIAS_MIN = 0.72
GEOMETRY_BIAS_MAX = 5.588
GEOMETRY_HORIZONTAL_BIAS = 0.16
GEOMETRY_VERTICAL_BIAS = 0.18
GEOMETRY_PROXIMITY_BIAS = 0.0
GEOMETRY_DISTANCE_SCALE_M = 0.90
GEOMETRY_CENTER_RELAX_START = 0.10
GEOMETRY_CENTER_RELAX_END = 0.35
TOP_CENTER_ANCHOR_ID = "5"
TOP_CENTER_BIAS = 0.0

SOLVER_MAX_ITERS = 12
SOLVER_HUBER_DELTA_M = 0.08
SOLVER_DAMPING = 0.015
SOLVER_STEP_LIMIT = 1.25
OUTLIER_REJECT_RESIDUAL_M = 0.18
OUTLIER_REJECT_MARGIN_M = 0.06
OUTLIER_RETRY_IMPROVEMENT_M = 0.015
SOLVER_MAX_BASE_WEIGHT = 1.35

FILTER_ALPHA_MIN = 0.20
FILTER_ALPHA_MAX = 0.55
FILTER_BETA_MIN = 0.015
FILTER_BETA_MAX = 0.10
FILTER_VELOCITY_DAMPING = 0.88
FILTER_HOLD_VELOCITY_DAMPING = 0.10
MAX_TRACK_SPEED_MPS = 2.2
LOW_QUALITY_MAX_INNOVATION_M = 0.10
INNOVATION_SPEED_ALLOWANCE_MPS = 1.2
INNOVATION_DEADZONE_M = 0.012
LOW_QUALITY_INNOVATION_DEADZONE_BONUS_M = 0.008
FAST_MOTION_SCALE_M = 0.14
EDGE_SOFT_MARGIN_M = 0.06
OUTPUT_DEADBAND_M = 0.018
OUTPUT_LOW_QUALITY_DEADBAND_BONUS_M = 0.010
LOW_QUALITY_HOLD_SCORE = 0.35
LOW_QUALITY_BLEND_END_SCORE = 0.65
LOW_QUALITY_HOLD_BASE_M = 0.045
LOW_QUALITY_HOLD_SPEED_ALLOWANCE_MPS = 0.45
LOW_QUALITY_MEASUREMENT_GAIN_MIN = 0.22

STATIONARY_ENTRY_SPEED_MPS = 0.10
STATIONARY_EXIT_SPEED_MPS = 0.26
STATIONARY_ENTRY_INNOVATION_M = 0.035
STATIONARY_EXIT_INNOVATION_M = 0.095
STATIONARY_MIN_QUALITY = 0.35

STATIONARY_CONFIDENCE_RISE = 0.50
STATIONARY_CONFIDENCE_FALL = 0.10

STATIONARY_MEASUREMENT_GAIN_MIN = 0.010
STATIONARY_OUTPUT_DEADBAND_BONUS_M = 0.080
STATIONARY_VELOCITY_DAMPING = 0.015

MIN_DT_S = 0.02
MAX_DT_S = 0.25
DEFAULT_DT_S = 0.05
QUALITY_RMSE_SCALE_M = 0.08
QUALITY_MAX_RESID_SCALE_M = 0.18
MIN_VALID_DISTANCE_CM = 1
HISTORY_FLUSH_INTERVAL_S = env_float("HISTORY_FLUSH_INTERVAL_S", 0.08)
HISTORY_FLUSH_MAX_PENDING = env_int("HISTORY_FLUSH_MAX_PENDING", 1)

STATIONARY_ANALYSIS_START_CONFIDENCE = 0.90
STATIONARY_ANALYSIS_STOP_CONFIDENCE = 0.55
STATIONARY_ANALYSIS_MIN_QUALITY = 0.60

STATIONARY_ANALYSIS_WARMUP_SAMPLES = 5
STATIONARY_ANALYSIS_WINDOW = 25
STATIONARY_ANALYSIS_MIN_ANCHOR_SAMPLES = 10
STATIONARY_ANALYSIS_MAX_SPREAD_M = 0.10

MEAS_RE = re.compile(
    r'\[mac_address=0x([0-9a-fA-F]+), status="([^"]+)"(?:, distance\[cm\]=(-?\d+))?\]'
)
SEQ_RE = re.compile(r"sequence_number=(\d+)")
