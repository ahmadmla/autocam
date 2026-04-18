import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Iterator, Optional, Tuple

from . import geometry as geom_state
from .config import (
    CENTRAL_LOGGER_CONSOLE,
    EVENT_LOG_PATH,
    HISTORY_FLUSH_INTERVAL_S,
    HISTORY_FLUSH_MAX_PENDING,
    HISTORY_PATH,
    MAX_HISTORY,
    MEAS_RE,
    NODE_ID,
    RAW_SERIAL_LOG_PATH,
    SEQ_RE,
    SERIAL_DIAGNOSTIC_INTERVAL_S,
    SHUTDOWN_COMMANDS,
    SHUTDOWN_COMMAND_DELAY_S,
)
from .solver import sanitize_distance_cm


# Parse one `SESSION_INFO_NTF` block into a sequence number and normalized anchor map.
def parse_block(block: str) -> Tuple[Optional[int], Dict[str, dict]]:
    seq_match = SEQ_RE.search(block)
    sequence_number = int(seq_match.group(1)) if seq_match else None

    anchors = {
        anchor_id: {"status": "MISSING", "distance_cm": None}
        for anchor_id in geom_state.ANCHOR_IDS
    }
    for mac_hex, status, distance_cm in MEAS_RE.findall(block):
        anchor_id = str(int(mac_hex, 16))
        if anchor_id not in geom_state.ANCHORS:
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


# Yield complete session blocks by stitching together serial lines until a block closes.
def iter_session_blocks(ser) -> Iterator[str]:
    current_lines = []
    last_serial_data_monotonic = time.monotonic()
    last_diagnostic_monotonic = last_serial_data_monotonic
    total_lines = 0
    session_start_count = 0
    completed_block_count = 0
    non_session_line_count = 0
    partial_block_line_count = 0

    while True:
        raw_line = ser.readline()
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
                yield " ".join(current_lines)
                current_lines = []
            continue

        if not current_lines:
            non_session_line_count += 1
            continue

        partial_block_line_count += 1
        current_lines.append(line)
        if line.endswith("}"):
            completed_block_count += 1
            yield " ".join(current_lines)
            current_lines = []


# Atomically rewrite the history JSON file so the dashboard never reads a partial write.
def write_history(history) -> None:
    temp_path = f"{HISTORY_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(history, handle, separators=(",", ":"), allow_nan=False)
    os.replace(temp_path, HISTORY_PATH)


@dataclass
class HistoryWriter:
    last_flush_monotonic: float = field(default_factory=time.monotonic)
    pending_entries: int = 0

    # Append one entry and flush when the pending count or flush interval is reached.
    def append(self, history, entry: dict) -> None:
        history.append(entry)
        self.pending_entries += 1
        history[:] = history[-MAX_HISTORY:]

        now = time.monotonic()
        if (
            self.pending_entries >= HISTORY_FLUSH_MAX_PENDING
            or (now - self.last_flush_monotonic) >= HISTORY_FLUSH_INTERVAL_S
        ):
            self.flush(history, now=now)

    # Force or opportunistically flush the in-memory history buffer to disk.
    def flush(self, history, force: bool = False, now: Optional[float] = None) -> None:
        if not force and self.pending_entries <= 0:
            return
        write_history(history)
        self.pending_entries = 0
        self.last_flush_monotonic = now if now is not None else time.monotonic()


@dataclass
class TextLogWriter:
    path: str
    handle: Optional[object] = None

    # Lazily open the log file and append a single line to it.
    def write_line(self, line: str) -> None:
        if self.handle is None:
            self.handle = open(self.path, "a", encoding="utf-8", buffering=1)
        self.handle.write(line + "\n")

    # Close the underlying log file handle if it was opened.
    def close(self) -> None:
        if self.handle is None:
            return
        self.handle.close()
        self.handle = None


# Format a wall-clock timestamp with millisecond precision for logs.
def format_log_timestamp(timestamp: Optional[float] = None) -> str:
    ts = time.time() if timestamp is None else timestamp
    whole = int(ts)
    millis = int((ts - whole) * 1000.0)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(whole)) + f".{millis:03d}"


EVENT_LOGGER = TextLogWriter(EVENT_LOG_PATH)
RAW_SERIAL_LOGGER = TextLogWriter(RAW_SERIAL_LOG_PATH)


# Print a runtime event and mirror it into the persistent event log.
def emit_log(message: str) -> None:
    if CENTRAL_LOGGER_CONSOLE:
        print(message, flush=True)
    EVENT_LOGGER.write_line(f"{format_log_timestamp()} {message}")


# Mirror a raw serial line into the serial capture log with a timestamp.
def emit_raw_serial_line(line: str) -> None:
    RAW_SERIAL_LOGGER.write_line(f"{format_log_timestamp()} {line}")


# Close both text log streams used by the central logger.
def close_text_logs() -> None:
    EVENT_LOGGER.close()
    RAW_SERIAL_LOGGER.close()


# Truncate the current log files so a new run starts with clean output.
def reset_text_logs() -> None:
    close_text_logs()
    for path in (EVENT_LOG_PATH, RAW_SERIAL_LOG_PATH):
        with open(path, "w", encoding="utf-8"):
            pass


# Render a raw command byte string as readable ASCII for diagnostics.
def command_bytes_to_text(command: bytes) -> str:
    return command.decode("ascii", errors="replace").strip()


# Send a sequence of serial commands with logging and an optional delay between them.
def send_serial_commands(
    ser,
    commands: Tuple[bytes, ...],
    delay_s: float,
    phase: str,
) -> None:
    for command in commands:
        ser.write(command)
        ser.flush()
        emit_log(
            f"node={NODE_ID} serial={phase} command={command_bytes_to_text(command)}"
        )
        if delay_s > 0.0:
            time.sleep(delay_s)


# Try to stop the current UWB session without letting shutdown errors crash the process.
def safe_stop_session(ser, phase: str = "stop") -> None:
    if not SHUTDOWN_COMMANDS:
        return
    try:
        send_serial_commands(
            ser,
            SHUTDOWN_COMMANDS,
            SHUTDOWN_COMMAND_DELAY_S,
            phase=phase,
        )
    except Exception as exc:
        emit_log(
            f"node={NODE_ID} serial={phase}_error "
            f"type={type(exc).__name__} detail={exc}"
        )
