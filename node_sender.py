# node_sender.py

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

try:
    import serial
except ImportError:
    serial = None

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

if serial is not None:
    SerialException = serial.SerialException
else:
    class SerialException(Exception):
        pass


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() not in ("0", "false", "no", "off")


def env_positive_int(name: str, default: int, minimum: int = 1) -> int:
    value = env_int(name, default)
    return value if value >= minimum else default


def build_init_command(
    peer_addrs_csv: str,
    include_timing_args: bool,
    slot_rstu: int,
    round_slots: int,
    block_ms: int,
) -> bytes:
    peer_addrs = [part.strip() for part in peer_addrs_csv.split(",") if part.strip()]
    if not peer_addrs:
        peer_addrs = ["1", "2", "3", "4", "5"]
    command_text = f"initf -multi -paddr=[{','.join(peer_addrs)}]"
    if include_timing_args:
        command_text += f" -slot={slot_rstu} -round={round_slots} -block={block_ms}"
    return build_command_bytes(command_text)


def build_command_bytes(command_text: str) -> bytes:
    stripped = command_text.strip()
    if not stripped:
        return b""
    return f"{stripped}\r\n".encode("ascii")


def parse_command_list(commands_text: str, default_commands) -> Tuple[bytes, ...]:
    if commands_text in (None, ""):
        commands = list(default_commands)
    else:
        commands = [part.strip() for part in commands_text.split(";")]
    built = [build_command_bytes(command) for command in commands if command.strip()]
    return tuple(command for command in built if command)


NODE_ID = env_str("NODE_ID", "rpi3")
PORT = env_str("UWB_PORT", "/dev/ttyACM0")
BAUD = env_int("UWB_BAUD", 115200)
SERIAL_TIMEOUT_S = env_float("SERIAL_TIMEOUT_S", 0.20)
SERIAL_RECONNECT_DELAY_S = env_float("SERIAL_RECONNECT_DELAY_S", 2.0)
SERIAL_READY_DELAY_S = env_float("SERIAL_READY_DELAY_S", 2.0)
SERIAL_DIAGNOSTIC_INTERVAL_S = env_float("SERIAL_DIAGNOSTIC_INTERVAL_S", 10.0)
MEASUREMENT_TIMEOUT_S = env_float("MEASUREMENT_TIMEOUT_S", 4.5)
POST_INIT_SETTLE_S = env_float("POST_INIT_SETTLE_S", 0.20)
MEASUREMENT_EARLY_ACCEPT_SUCCESS_COUNT = env_int(
    "MEASUREMENT_EARLY_ACCEPT_SUCCESS_COUNT",
    3,
)
CONTINUOUS_TIMEOUT_REINIT_THRESHOLD = env_int(
    "CONTINUOUS_TIMEOUT_REINIT_THRESHOLD",
    1,
)

PEER_ADDRS_CSV = env_str("UWB_PEER_ADDRS", "1,2,3,4,5")
UWB_INCLUDE_TIMING_ARGS = env_bool("UWB_INCLUDE_TIMING_ARGS", True)
UWB_FIRA_SLOT_RSTU = env_positive_int("UWB_FIRA_SLOT_RSTU", 2400, minimum=2400)
UWB_FIRA_ROUND_SLOTS = env_positive_int("UWB_FIRA_ROUND_SLOTS", 14)
UWB_FIRA_BLOCK_MS = env_positive_int("UWB_FIRA_BLOCK_MS", 50)
INIT_COMMAND = build_command_bytes(env_str("UWB_INIT_COMMAND", "")) or build_init_command(
    PEER_ADDRS_CSV,
    UWB_INCLUDE_TIMING_ARGS,
    UWB_FIRA_SLOT_RSTU,
    UWB_FIRA_ROUND_SLOTS,
    UWB_FIRA_BLOCK_MS,
)
PAUSE_AFTER_INIT = env_bool("UWB_PAUSE_AFTER_INIT", True)

ACTIVATE_COMMANDS = parse_command_list(
    env_str("UWB_ACTIVATE_COMMANDS", ""),
    default_commands=("activate",),
)
ACTIVATE_COMMAND_DELAY_S = env_float("UWB_ACTIVATE_COMMAND_DELAY_S", 0.05)

PAUSE_COMMANDS = parse_command_list(
    env_str("UWB_PAUSE_COMMANDS", ""),
    default_commands=("pause",),
)
PAUSE_COMMAND_DELAY_S = env_float("UWB_PAUSE_COMMAND_DELAY_S", 0.05)

PRE_INIT_COMMANDS = parse_command_list(
    env_str("UWB_PRE_INIT_COMMANDS", ""),
    default_commands=(),
)
PRE_INIT_COMMAND_DELAY_S = env_float("UWB_PRE_INIT_COMMAND_DELAY_S", 0.20)

SHUTDOWN_COMMANDS = parse_command_list(
    env_str("UWB_SHUTDOWN_COMMANDS", ""),
    default_commands=("pause",),
)
SHUTDOWN_COMMAND_DELAY_S = env_float("UWB_SHUTDOWN_COMMAND_DELAY_S", 0.10)

RAW_SERIAL_LOG_PATH = env_str("RAW_SERIAL_LOG_PATH", f"{NODE_ID}_raw_serial_output.txt")
EVENT_LOG_PATH = env_str("EVENT_LOG_PATH", f"{NODE_ID}_node_sender_log.txt")

MQTT_HOST = env_str("MQTT_HOST", "127.0.0.1")
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_RAW_TOPIC_BASE = env_str("MQTT_RAW_TOPIC_BASE", "uwb/raw")
MQTT_CMD_TOPIC_BASE = env_str("MQTT_CMD_TOPIC_BASE", "uwb/cmd")
MQTT_QOS = env_int("MQTT_QOS", 0)
MQTT_USERNAME = env_str("MQTT_USERNAME", "")
MQTT_PASSWORD = env_str("MQTT_PASSWORD", "")

ANCHOR_IDS = ["1", "2", "3", "4", "5"]
MIN_VALID_DISTANCE_CM = 1

MEAS_RE = re.compile(
    r'\[mac_address=0x([0-9a-fA-F]+), status="([^"]+)"(?:, distance\[cm\]=(-?\d+))?\]'
)
SEQ_RE = re.compile(r"sequence_number=(\d+)")


@dataclass
class TextLogWriter:
    path: str
    handle: Optional[object] = None

    def write_line(self, line: str) -> None:
        if self.handle is None:
            self.handle = open(self.path, "a", encoding="utf-8", buffering=1)
        self.handle.write(line + "\n")

    def close(self) -> None:
        if self.handle is None:
            return
        self.handle.close()
        self.handle = None


EVENT_LOGGER = TextLogWriter(EVENT_LOG_PATH)
RAW_SERIAL_LOGGER = TextLogWriter(RAW_SERIAL_LOG_PATH)


def format_log_timestamp(timestamp: Optional[float] = None) -> str:
    ts = time.time() if timestamp is None else timestamp
    whole = int(ts)
    millis = int((ts - whole) * 1000.0)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(whole)) + f".{millis:03d}"


def emit_log(message: str) -> None:
    print(message, flush=True)
    EVENT_LOGGER.write_line(f"{format_log_timestamp()} {message}")


def emit_raw_serial_line(line: str) -> None:
    RAW_SERIAL_LOGGER.write_line(f"{format_log_timestamp()} {line}")


def close_text_logs() -> None:
    EVENT_LOGGER.close()
    RAW_SERIAL_LOGGER.close()


def reset_text_logs() -> None:
    close_text_logs()
    for path in (EVENT_LOG_PATH, RAW_SERIAL_LOG_PATH):
        with open(path, "w", encoding="utf-8"):
            pass


def command_bytes_to_text(command: bytes) -> str:
    return command.decode("ascii", errors="replace").strip()


def send_serial_commands(
    ser_handle,
    commands: Tuple[bytes, ...],
    delay_s: float,
    phase: str,
) -> None:
    for command in commands:
        ser_handle.write(command)
        ser_handle.flush()
        emit_log(
            f"node={NODE_ID} serial={phase} command={command_bytes_to_text(command)}"
        )
        if delay_s > 0.0:
            time.sleep(delay_s)


def send_pause_commands(ser_handle, phase: str = "pause") -> None:
    if ser_handle is None or not PAUSE_COMMANDS:
        return
    send_serial_commands(
        ser_handle,
        PAUSE_COMMANDS,
        PAUSE_COMMAND_DELAY_S,
        phase=phase,
    )


def pause_active_session(ser_handle, phase: str = "pause") -> None:
    if PAUSE_COMMANDS:
        try:
            send_pause_commands(ser_handle, phase=phase)
            return
        except Exception as exc:
            emit_log(
                f"node={NODE_ID} serial={phase}_error "
                f"type={type(exc).__name__} detail={exc}"
            )
    safe_stop_session(ser_handle, phase=phase)


def safe_stop_session(ser_handle, phase: str = "stop") -> None:
    if ser_handle is None or not SHUTDOWN_COMMANDS:
        return
    try:
        send_serial_commands(
            ser_handle,
            SHUTDOWN_COMMANDS,
            SHUTDOWN_COMMAND_DELAY_S,
            phase=phase,
        )
        try:
            ser_handle.flush()
        except Exception:
            pass
        time.sleep(max(SHUTDOWN_COMMAND_DELAY_S, 0.20))
        try:
            ser_handle.reset_input_buffer()
        except Exception:
            pass
        try:
            ser_handle.reset_output_buffer()
        except Exception:
            pass
    except Exception as exc:
        emit_log(
            f"node={NODE_ID} serial={phase}_error "
            f"type={type(exc).__name__} detail={exc}"
        )


def sanitize_distance_cm(distance_cm: Optional[int]) -> Optional[int]:
    if distance_cm is None:
        return None
    if distance_cm < MIN_VALID_DISTANCE_CM:
        return None
    return distance_cm


def parse_block(block: str) -> Tuple[Optional[int], Dict[str, dict]]:
    seq_match = SEQ_RE.search(block)
    sequence_number = int(seq_match.group(1)) if seq_match else None

    anchors = {
        anchor_id: {"status": "MISSING", "distance_cm": None}
        for anchor_id in ANCHOR_IDS
    }

    for mac_hex, status, distance_cm in MEAS_RE.findall(block):
        anchor_id = str(int(mac_hex, 16))
        if anchor_id not in anchors:
            continue

        parsed_distance_cm = int(distance_cm) if distance_cm else None
        sanitized_distance_cm = sanitize_distance_cm(parsed_distance_cm)

        if status == "SUCCESS" and sanitized_distance_cm is None:
            status = "INVALID_RANGE"

        anchors[anchor_id] = {
            "status": status,
            "distance_cm": sanitized_distance_cm,
        }

    return sequence_number, anchors


def summarize_anchor_info(anchor_info: Dict[str, dict]) -> Tuple[int, str]:
    success_count = 0
    status_counts = {}
    for anchor_id in ANCHOR_IDS:
        status = str(anchor_info.get(anchor_id, {}).get("status", "MISSING"))
        if status == "SUCCESS":
            success_count += 1
        status_counts[status] = status_counts.get(status, 0) + 1
    status_summary = ",".join(
        f"{status}:{count}" for status, count in sorted(status_counts.items())
    )
    return success_count, status_summary


def read_one_session_block(
    ser_handle,
    timeout_s: float,
    stop_event: threading.Event,
) -> Optional[str]:
    current_lines = []
    start_monotonic = time.monotonic()
    last_serial_data_monotonic = start_monotonic
    last_diagnostic_monotonic = start_monotonic
    best_block: Optional[str] = None
    best_success_count = -1
    best_status_summary = ""
    total_lines = 0
    session_start_count = 0
    completed_block_count = 0
    non_session_line_count = 0
    partial_block_line_count = 0

    def consider_block(block_text: str) -> Optional[str]:
        nonlocal best_block, best_success_count, best_status_summary

        _, anchors = parse_block(block_text)
        success_count, status_summary = summarize_anchor_info(anchors)
        if success_count > best_success_count:
            best_block = block_text
            best_success_count = success_count
            best_status_summary = status_summary

        if success_count >= MEASUREMENT_EARLY_ACCEPT_SUCCESS_COUNT:
            emit_log(
                f"node={NODE_ID} measurement=accepted "
                f"success={success_count}/{len(ANCHOR_IDS)} "
                f"statuses={status_summary} "
                f"blocks={completed_block_count}"
            )
            return block_text

        return None

    while True:
        if stop_event.is_set():
            if best_block is not None:
                emit_log(
                    f"node={NODE_ID} measurement=stopped_best_effort "
                    f"success={max(best_success_count, 0)}/{len(ANCHOR_IDS)} "
                    f"statuses={best_status_summary or '-'} "
                    f"blocks={completed_block_count}"
                )
                return best_block
            return None

        if (time.monotonic() - start_monotonic) >= timeout_s:
            if best_block is not None:
                emit_log(
                    f"node={NODE_ID} measurement=best_effort "
                    f"success={max(best_success_count, 0)}/{len(ANCHOR_IDS)} "
                    f"statuses={best_status_summary or '-'} "
                    f"blocks={completed_block_count}"
                )
                return best_block
            emit_log(
                f"node={NODE_ID} measurement=timeout "
                f"lines={total_lines} "
                f"session_starts={session_start_count} "
                f"blocks={completed_block_count} "
                f"non_session={non_session_line_count} "
                f"partial_lines={partial_block_line_count}"
            )
            return None

        raw_line = ser_handle.readline()
        now = time.monotonic()

        if not raw_line:
            if (now - last_diagnostic_monotonic) >= SERIAL_DIAGNOSTIC_INTERVAL_S:
                emit_log(
                    f"node={NODE_ID} serial=waiting "
                    f"lines={total_lines} "
                    f"session_starts={session_start_count} "
                    f"blocks={completed_block_count} "
                    f"non_session={non_session_line_count} "
                    f"partial_lines={partial_block_line_count} "
                    f"idle_for={now - last_serial_data_monotonic:.1f}s"
                )
                last_diagnostic_monotonic = now
            continue

        last_serial_data_monotonic = now
        decoded_line = raw_line.decode(errors="replace").rstrip("\r\n")
        if decoded_line:
            emit_raw_serial_line(decoded_line)

        line = decoded_line.strip()
        if not line:
            continue

        total_lines += 1

        if line.startswith("SESSION_INFO_NTF:"):
            session_start_count += 1
            current_lines = [line]
            if line.endswith("}"):
                completed_block_count += 1
                accepted = consider_block(" ".join(current_lines))
                current_lines = []
                if accepted is not None:
                    return accepted
            continue

        if not current_lines:
            non_session_line_count += 1
            continue

        partial_block_line_count += 1
        current_lines.append(line)
        if line.endswith("}"):
            completed_block_count += 1
            accepted = consider_block(" ".join(current_lines))
            current_lines = []
            if accepted is not None:
                return accepted


def open_serial_idle():
    if serial is None:
        raise RuntimeError(
            "pyserial is not installed. Install it in your venv with 'python -m pip install pyserial'."
        )

    ser_handle = serial.Serial(PORT, BAUD, timeout=SERIAL_TIMEOUT_S)
    time.sleep(SERIAL_READY_DELAY_S)

    try:
        ser_handle.reset_input_buffer()
    except Exception:
        pass
    try:
        ser_handle.reset_output_buffer()
    except Exception:
        pass

    if PRE_INIT_COMMANDS:
        send_serial_commands(
            ser_handle,
            PRE_INIT_COMMANDS,
            PRE_INIT_COMMAND_DELAY_S,
            phase="pre_init",
        )
        try:
            ser_handle.reset_input_buffer()
        except Exception:
            pass

    emit_log(
        f"node={NODE_ID} serial=open_idle port={PORT} baud={BAUD} "
        f"ready_delay={SERIAL_READY_DELAY_S:.3f}s"
    )
    initialize_ranging_session(ser_handle)
    return ser_handle


def close_serial_handle(ser_handle) -> None:
    if ser_handle is None:
        return

    try:
        ser_handle.reset_input_buffer()
    except Exception:
        pass
    try:
        ser_handle.reset_output_buffer()
    except Exception:
        pass
    try:
        ser_handle.close()
    except Exception:
        pass


class CommandState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.start_event = threading.Event()
        self.stop_event = threading.Event()
        self.pending_start = False
        self.pending_token: Optional[str] = None
        self.continuous_mode = False
        self.active_continuous_mode = False
        self.running = False

    def request_start(self, token: Optional[str], continuous: bool = False) -> None:
        with self.lock:
            should_stop_current_cycle = self.running and self.active_continuous_mode and not continuous
            self.pending_start = True
            self.pending_token = token
            self.continuous_mode = continuous
            if should_stop_current_cycle:
                self.stop_event.set()
            else:
                self.stop_event.clear()
            self.start_event.set()

    def request_stop(self) -> None:
        with self.lock:
            self.continuous_mode = False
            self.pending_start = False
            self.pending_token = None
            self.start_event.clear()
            if self.running:
                self.stop_event.set()
            else:
                self.stop_event.clear()

    def begin_cycle(self) -> Tuple[Optional[str], bool]:
        with self.lock:
            if not self.pending_start:
                return None, False
            token = self.pending_token
            continuous_mode = self.continuous_mode
            self.pending_start = False
            self.pending_token = None
            self.running = True
            self.active_continuous_mode = continuous_mode
            self.start_event.clear()
            self.stop_event.clear()
            return token, continuous_mode

    def end_cycle(self) -> None:
        with self.lock:
            self.running = False
            self.active_continuous_mode = False
            self.stop_event.clear()

    def take_pending_token(self) -> Optional[str]:
        with self.lock:
            if not self.pending_start:
                return None
            token = self.pending_token
            self.pending_start = False
            self.pending_token = None
            self.start_event.clear()
            return token

    def is_running(self) -> bool:
        with self.lock:
            return self.running


class MqttNodeClient:
    def __init__(self, command_state: CommandState) -> None:
        if mqtt is None:
            raise RuntimeError(
                "paho-mqtt is not installed. Install it in your venv with 'python -m pip install paho-mqtt'."
            )

        self.command_state = command_state
        self.cmd_topic = f"{MQTT_CMD_TOPIC_BASE}/{NODE_ID}"
        self.raw_topic = f"{MQTT_RAW_TOPIC_BASE}/{NODE_ID}"

        try:
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"{NODE_ID}_tdma_node",
            )
        except AttributeError:
            self.client = mqtt.Client(client_id=f"{NODE_ID}_tdma_node")

        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)

        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        emit_log(
            f"node={NODE_ID} mqtt=connected host={MQTT_HOST} port={MQTT_PORT} rc={reason_code}"
        )
        client.subscribe(self.cmd_topic, qos=MQTT_QOS)
        emit_log(f"node={NODE_ID} mqtt=subscribed topic={self.cmd_topic}")

    def on_disconnect(self, client, userdata, disconnect_flags=None, reason_code=None, properties=None):
        emit_log(f"node={NODE_ID} mqtt=disconnected rc={reason_code}")

    def on_message(self, client, userdata, msg):
        try:
            text = msg.payload.decode("utf-8").strip()
            payload = json.loads(text) if text.startswith("{") else {"cmd": text}
            cmd = str(payload.get("cmd", "")).strip().lower()
            token = payload.get("token")
            continuous = bool(payload.get("continuous", False))
        except Exception as exc:
            emit_log(f"node={NODE_ID} mqtt=command_parse_error detail={exc}")
            return

        if cmd in ("activate", "start"):
            emit_log(
                f"node={NODE_ID} cmd=activate token={token} continuous={int(continuous)}"
            )
            self.command_state.request_start(
                str(token) if token is not None else None,
                continuous=continuous,
            )
        elif cmd in ("pause", "stop"):
            emit_log(f"node={NODE_ID} cmd=pause")
            self.command_state.request_stop()
        else:
            emit_log(f"node={NODE_ID} cmd=ignored value={cmd!r}")

    def publish_measurement(self, payload: dict) -> None:
        info = self.client.publish(
            self.raw_topic,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=MQTT_QOS,
            retain=False,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            emit_log(f"node={NODE_ID} mqtt=publish_error topic={self.raw_topic} rc={info.rc}")

    def close(self) -> None:
        try:
            self.client.loop_stop()
        finally:
            self.client.disconnect()


def perform_one_measurement_cycle(
    ser_handle,
    mqtt_client: MqttNodeClient,
    command_state: CommandState,
    slot_token: Optional[str],
) -> None:
    if ser_handle is None:
        raise RuntimeError("serial handle is not open")

    start_measurement_session(ser_handle, slot_token)
    block = read_one_session_block(
        ser_handle=ser_handle,
        timeout_s=MEASUREMENT_TIMEOUT_S,
        stop_event=command_state.stop_event,
    )
    pause_active_session(ser_handle, phase="post_measurement")
    publish_measurement_block(mqtt_client, block, slot_token)


def start_measurement_session(ser_handle, slot_token: Optional[str]) -> None:
    try:
        ser_handle.reset_input_buffer()
    except Exception:
        pass
    try:
        ser_handle.reset_output_buffer()
    except Exception:
        pass

    if ACTIVATE_COMMANDS:
        send_serial_commands(
            ser_handle,
            ACTIVATE_COMMANDS,
            ACTIVATE_COMMAND_DELAY_S,
            phase="activate",
        )
        emit_log(f"node={NODE_ID} serial=range_activate token={slot_token}")
        if POST_INIT_SETTLE_S > 0.0:
            time.sleep(POST_INIT_SETTLE_S)
        return

    emit_log(f"node={NODE_ID} serial=range_activate_skipped token={slot_token}")


def initialize_ranging_session(ser_handle) -> None:
    if ser_handle is None:
        raise RuntimeError("serial handle is not open")

    try:
        ser_handle.reset_input_buffer()
    except Exception:
        pass
    try:
        ser_handle.reset_output_buffer()
    except Exception:
        pass

    ser_handle.write(INIT_COMMAND)
    ser_handle.flush()
    emit_log(
        f"node={NODE_ID} serial=init command={INIT_COMMAND.decode().strip()}"
    )
    if POST_INIT_SETTLE_S > 0.0:
        time.sleep(POST_INIT_SETTLE_S)

    if PAUSE_AFTER_INIT:
        pause_active_session(ser_handle, phase="post_init_pause")


def publish_measurement_block(
    mqtt_client: MqttNodeClient,
    block: Optional[str],
    slot_token: Optional[str],
) -> None:
    if block is None:
        return

    sequence_number, anchors = parse_block(block)
    success_count, status_summary = summarize_anchor_info(anchors)
    payload = {
        "node_id": NODE_ID,
        "timestamp": time.time(),
        "sequence_number": sequence_number,
        "anchors": anchors,
        "slot_token": slot_token,
    }
    mqtt_client.publish_measurement(payload)
    emit_log(
        f"node={NODE_ID} mqtt=published seq={sequence_number} token={slot_token} "
        f"success={success_count}/{len(ANCHOR_IDS)} statuses={status_summary}"
    )


def perform_continuous_measurement_loop(
    ser_handle,
    mqtt_client: MqttNodeClient,
    command_state: CommandState,
    slot_token: Optional[str],
) -> None:
    if ser_handle is None:
        raise RuntimeError("serial handle is not open")

    publish_token = slot_token

    while not command_state.stop_event.is_set():
        start_measurement_session(ser_handle, publish_token)
        consecutive_timeouts = 0
        needs_reinitialize = False

        while not command_state.stop_event.is_set():
            block = read_one_session_block(
                ser_handle=ser_handle,
                timeout_s=MEASUREMENT_TIMEOUT_S,
                stop_event=command_state.stop_event,
            )
            if block is None:
                if command_state.stop_event.is_set():
                    break
                consecutive_timeouts += 1
                emit_log(
                    f"node={NODE_ID} measurement=continuous_timeout "
                    f"count={consecutive_timeouts}"
                )
                if consecutive_timeouts >= CONTINUOUS_TIMEOUT_REINIT_THRESHOLD:
                    emit_log(
                        f"node={NODE_ID} measurement=continuous_reinit "
                        f"count={consecutive_timeouts}"
                    )
                    needs_reinitialize = True
                    break
                continue

            consecutive_timeouts = 0
            publish_measurement_block(mqtt_client, block, publish_token)
            if publish_token is not None:
                publish_token = None
            if command_state.stop_event.is_set():
                break
            next_token = command_state.take_pending_token()
            if next_token is not None:
                publish_token = next_token

        pause_active_session(ser_handle, phase="continuous_pause")
        if not needs_reinitialize:
            break

        initialize_ranging_session(ser_handle)


def main() -> None:
    reset_text_logs()
    command_state = CommandState()
    mqtt_client = MqttNodeClient(command_state)
    ser_handle = None

    while True:
        try:
            if ser_handle is None:
                ser_handle = open_serial_idle()

            if not command_state.start_event.wait(timeout=0.10):
                continue

            slot_token, continuous_mode = command_state.begin_cycle()
            if not command_state.is_running():
                continue
            try:
                if continuous_mode:
                    perform_continuous_measurement_loop(
                        ser_handle=ser_handle,
                        mqtt_client=mqtt_client,
                        command_state=command_state,
                        slot_token=slot_token,
                    )
                else:
                    perform_one_measurement_cycle(
                        ser_handle=ser_handle,
                        mqtt_client=mqtt_client,
                        command_state=command_state,
                        slot_token=slot_token,
                    )
            finally:
                command_state.end_cycle()

        except KeyboardInterrupt:
            emit_log(f"node={NODE_ID} keyboard_interrupt=1")
            if ser_handle is not None:
                safe_stop_session(ser_handle, phase="shutdown")
                close_serial_handle(ser_handle)
                ser_handle = None
            mqtt_client.close()
            emit_log(f"node={NODE_ID} stopped")
            close_text_logs()
            return

        except SerialException as exc:
            if ser_handle is not None:
                safe_stop_session(ser_handle, phase="recover")
                close_serial_handle(ser_handle)
                ser_handle = None
            emit_log(
                f"node={NODE_ID} serial_error={exc} "
                f"reconnect_in={SERIAL_RECONNECT_DELAY_S:.1f}s"
            )
            time.sleep(SERIAL_RECONNECT_DELAY_S)

        except RuntimeError as exc:
            if ser_handle is not None:
                safe_stop_session(ser_handle, phase="shutdown")
                close_serial_handle(ser_handle)
                ser_handle = None
            mqtt_client.close()
            emit_log(f"node={NODE_ID} runtime_error={exc}")
            close_text_logs()
            return

        except Exception as exc:
            if ser_handle is not None:
                safe_stop_session(ser_handle, phase="recover")
                close_serial_handle(ser_handle)
                ser_handle = None
            emit_log(
                f"node={NODE_ID} unexpected_error={type(exc).__name__} detail={exc} "
                f"reconnect_in={SERIAL_RECONNECT_DELAY_S:.1f}s"
            )
            time.sleep(SERIAL_RECONNECT_DELAY_S)


if __name__ == "__main__":
    main()
