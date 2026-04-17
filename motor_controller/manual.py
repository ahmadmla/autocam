"""Interactive manual jog and limit calibration mode for one motor axis."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

PACKAGE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_ROOT.parent

if __package__ in (None, ""):
    for candidate in (str(REPO_ROOT), str(PACKAGE_ROOT)):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)
    from config import env_bool, env_float, env_int, load_runtime_config
    from control import Bld305sMotorBus, MotorStatus
else:
    from .config import env_bool, env_float, env_int, load_runtime_config
    from .control import Bld305sMotorBus, MotorStatus

LOG = logging.getLogger("motor_manual")


@dataclass
class ManualAxisState:
    axis_name: str
    motor_id: int
    driver_sign: int
    units_per_raw_speed_s: float
    estimated_position: float
    unit_name: str
    configured_left_limit: Optional[float] = None
    configured_right_limit: Optional[float] = None
    center_mark: Optional[float] = None
    left_mark: Optional[float] = None
    right_mark: Optional[float] = None

    def apply_logical_pulse(self, logical_raw: int, duration_s: float) -> None:
        self.estimated_position += logical_raw * self.units_per_raw_speed_s * duration_s

    def conservative_left(self, margin: float) -> Optional[float]:
        if self.left_mark is None:
            return None
        return self.left_mark + abs(margin) if self.left_mark <= 0.0 else self.left_mark - abs(margin)

    def conservative_right(self, margin: float) -> Optional[float]:
        if self.right_mark is None:
            return None
        return self.right_mark - abs(margin) if self.right_mark >= 0.0 else self.right_mark + abs(margin)

    def bounds_midpoint(self) -> Optional[float]:
        if self.left_mark is None or self.right_mark is None:
            return None
        return (self.left_mark + self.right_mark) / 2.0


@dataclass(frozen=True)
class ManualSettings:
    jog_raw_speed: int
    jog_duration_s: float
    max_raw_speed: int
    max_duration_s: float
    settle_s: float
    status_poll_s: float = 0.02
    print_poll_samples: bool = False
    goto_max_duration_s: float = 0.10
    goto_tolerance: float = 0.01
    goto_max_steps: int = 50
    limit_margin: float = 0.05


class ManualMotorSession:
    def __init__(self, axis: ManualAxisState, settings: ManualSettings, *, live: bool, port: str, baudrate: int, timeout_s: float) -> None:
        self.axis = axis
        self.settings = settings
        self.motor_bus = Bld305sMotorBus(
            port=port,
            baudrate=baudrate,
            timeout_s=timeout_s,
            live=live,
        )
        self.live = live

    def connect(self) -> None:
        self.motor_bus.connect()
        self.motor_bus.clear_fault(self.axis.motor_id)
        self.motor_bus.configure_speed_mode(self.axis.motor_id)
        self.stop()

    def close(self) -> None:
        try:
            if self.motor_bus.connected:
                self.stop()
        except Exception:
            LOG.exception("manual_close_stop_failed axis=%s", self.axis.axis_name)
        finally:
            self.motor_bus.close()

    def stop(self) -> None:
        self.motor_bus.stop(self.axis.motor_id)

    def read_status(self) -> MotorStatus:
        return self.motor_bus.read_status(self.axis.motor_id)

    def check_fault(self) -> None:
        status = self.read_status()
        if status.has_fault:
            raise RuntimeError(f"Driver fault on motor {status.motor_id}: code={status.fault_code}")

    def _estimate_delta_from_actual_speed(self, logical_direction: int, duration_s: float) -> float:
        logical_sign = 1.0 if logical_direction >= 0 else -1.0
        if not self.live:
            logical_raw = int(logical_direction) * self.settings.jog_raw_speed
            return logical_raw * self.axis.units_per_raw_speed_s * duration_s

        end_t = time.monotonic() + max(0.0, duration_s)
        last_t = time.monotonic()
        delta = 0.0
        sample_index = 0
        while True:
            now = time.monotonic()
            remaining = end_t - now
            if remaining <= 0.0:
                break
            time.sleep(min(self.settings.status_poll_s, remaining))
            status = self.read_status()
            sample_t = time.monotonic()
            dt = max(0.0, sample_t - last_t)
            last_t = sample_t
            actual_raw = abs(int(status.actual_speed_raw or 0))
            sample_delta = logical_sign * actual_raw * self.axis.units_per_raw_speed_s * dt
            delta += sample_delta
            sample_index += 1
            if self.settings.print_poll_samples:
                print(
                    f"poll[{sample_index}] t={sample_t:.3f} dt={dt:.4f}s actual_speed_raw={actual_raw} "
                    f"sample_delta_{self.axis.unit_name}={sample_delta:.5f} cumulative_{self.axis.unit_name}={delta:.5f}",
                    flush=True,
                )
        return delta

    def pulse(
        self,
        logical_direction: int,
        *,
        count: int = 1,
        duration_s: Optional[float] = None,
        raw_speed: Optional[int] = None,
    ) -> None:
        count = max(1, int(count))
        pulse_duration_s = self.settings.jog_duration_s if duration_s is None else min(
            self.settings.max_duration_s,
            max(0.02, float(duration_s)),
        )
        pulse_raw_speed = self.settings.jog_raw_speed if raw_speed is None else min(
            self.settings.max_raw_speed,
            max(1, abs(int(raw_speed))),
        )
        logical_raw = int(logical_direction) * pulse_raw_speed
        driver_raw = logical_raw * self.axis.driver_sign
        for index in range(count):
            self.check_fault()
            self.motor_bus.set_speed(self.axis.motor_id, driver_raw)
            self.motor_bus.run(self.axis.motor_id, direction=1 if driver_raw > 0 else -1)
            LOG.info(
                "manual_pulse axis=%s step=%s/%s logical_raw=%s driver_raw=%s duration_s=%.3f",
                self.axis.axis_name,
                index + 1,
                count,
                logical_raw,
                driver_raw,
                pulse_duration_s,
            )
            delta = self._estimate_delta_from_actual_speed(int(logical_direction), pulse_duration_s)
            self.stop()
            self.axis.estimated_position += delta
            LOG.info(
                "manual_motion_estimate axis=%s step=%s/%s delta_%s=%.4f estimated_%s=%.4f",
                self.axis.axis_name,
                index + 1,
                count,
                self.axis.unit_name,
                delta,
                self.axis.unit_name,
                self.axis.estimated_position,
            )
            time.sleep(self.settings.settle_s)
            self.check_fault()

    def go_to_position(self, target_position: float, *, label: str) -> None:
        tolerance = max(0.0, self.settings.goto_tolerance)
        max_steps = max(1, self.settings.goto_max_steps)
        goto_max_duration_s = min(self.settings.max_duration_s, max(0.02, self.settings.goto_max_duration_s))
        max_raw_speed = min(self.settings.max_raw_speed, max(1, self.settings.jog_raw_speed))
        for step in range(1, max_steps + 1):
            error = target_position - self.axis.estimated_position
            if abs(error) <= tolerance:
                print(
                    f"Reached {label}: target_{self.axis.unit_name}={target_position:.4f} "
                    f"estimated_{self.axis.unit_name}={self.axis.estimated_position:.4f} "
                    f"error_{self.axis.unit_name}={error:.4f}",
                    flush=True,
                )
                return
            direction = 1 if error > 0.0 else -1
            raw_speed_from_error = int(round(abs(error) / max(self.axis.units_per_raw_speed_s * goto_max_duration_s, 1e-9)))
            pulse_raw_speed = min(max_raw_speed, max(1, raw_speed_from_error))
            nominal_rate = max(1e-9, pulse_raw_speed * self.axis.units_per_raw_speed_s)
            pulse_duration_s = min(
                goto_max_duration_s,
                max(0.02, abs(error) / nominal_rate),
            )
            print(
                f"go[{step}/{max_steps}] label={label} target_{self.axis.unit_name}={target_position:.4f} "
                f"estimated_{self.axis.unit_name}={self.axis.estimated_position:.4f} "
                f"error_{self.axis.unit_name}={error:.4f} raw_speed={pulse_raw_speed} duration_s={pulse_duration_s:.3f}",
                flush=True,
            )
            self.pulse(direction, count=1, duration_s=pulse_duration_s, raw_speed=pulse_raw_speed)
        raise RuntimeError(
            f"Could not reach {label} within {max_steps} steps. "
            f"estimated_{self.axis.unit_name}={self.axis.estimated_position:.4f} "
            f"target_{self.axis.unit_name}={target_position:.4f}"
        )

    def go_to_center(self) -> None:
        target = self.axis.center_mark
        label = "startup center mark"
        if target is None:
            target = self.axis.bounds_midpoint()
            label = "bounds midpoint"
        if target is None:
            raise RuntimeError("No center target available. Mark left/right bounds or set a center mark first.")
        self.go_to_position(target, label=label)

    def go_to_left(self) -> None:
        target = self.axis.conservative_left(self.settings.limit_margin)
        label = "safe left bound"
        if target is None:
            target = self.axis.left_mark
            label = "left mark"
        if target is None:
            target = self.axis.configured_left_limit
            label = "configured left limit"
        if target is None:
            raise RuntimeError("No left target available. Mark left first or configure a left limit.")
        self.go_to_position(target, label=label)

    def go_to_right(self) -> None:
        target = self.axis.conservative_right(self.settings.limit_margin)
        label = "safe right bound"
        if target is None:
            target = self.axis.right_mark
            label = "right mark"
        if target is None:
            target = self.axis.configured_right_limit
            label = "configured right limit"
        if target is None:
            raise RuntimeError("No right target available. Mark right first or configure a right limit.")
        self.go_to_position(target, label=label)

    def flip_sign(self) -> None:
        self.axis.driver_sign *= -1
        LOG.warning("manual_sign_flipped axis=%s new_sign=%s", self.axis.axis_name, self.axis.driver_sign)

    def print_status(self) -> None:
        status = self.read_status()
        print(
            f"axis={self.axis.axis_name} motor_id={self.axis.motor_id} live={int(self.live)} "
            f"driver_sign={self.axis.driver_sign} estimated_{self.axis.unit_name}={self.axis.estimated_position:.4f}",
            flush=True,
        )
        print(
            f"jog_raw_speed={self.settings.jog_raw_speed} jog_duration_s={self.settings.jog_duration_s:.3f} "
            f"settle_s={self.settings.settle_s:.3f} status_poll_s={self.settings.status_poll_s:.3f} "
            f"print_poll_samples={int(self.settings.print_poll_samples)} actual_speed_raw={status.actual_speed_raw} fault={status.fault_code}",
            flush=True,
        )
        if self.axis.left_mark is not None:
            print(
                f"marked_left_{self.axis.unit_name}={self.axis.left_mark:.4f} "
                f"conservative_left={self.axis.conservative_left(self.settings.limit_margin):.4f}",
                flush=True,
            )
        if self.axis.right_mark is not None:
            print(
                f"marked_right_{self.axis.unit_name}={self.axis.right_mark:.4f} "
                f"conservative_right={self.axis.conservative_right(self.settings.limit_margin):.4f}",
                flush=True,
            )
        if self.axis.center_mark is not None:
            print(f"center_mark_{self.axis.unit_name}={self.axis.center_mark:.4f}", flush=True)
        bounds_midpoint = self.axis.bounds_midpoint()
        if bounds_midpoint is not None:
            print(f"bounds_midpoint_{self.axis.unit_name}={bounds_midpoint:.4f}", flush=True)
        self.print_env_suggestions()

    def build_base_env_updates(self) -> dict[str, str]:
        startup_center = self.axis.center_mark
        if startup_center is None:
            startup_center = self.axis.estimated_position
        if self.axis.axis_name == "truck":
            return {
                "TRUCK_SIGN": str(self.axis.driver_sign),
                "CAMERA_START_X_M": f"{startup_center:.4f}",
            }
        return {
            "PAN_SIGN": str(self.axis.driver_sign),
            "CAMERA_START_PAN_DEG": f"{startup_center:.4f}",
        }

    def build_env_updates(self) -> dict[str, str]:
        updates = self.build_base_env_updates()
        if self.axis.axis_name == "truck":
            left_value = self.axis.conservative_left(self.settings.limit_margin)
            right_value = self.axis.conservative_right(self.settings.limit_margin)
            if left_value is not None:
                updates["TRUCK_MIN_X_M"] = f"{left_value:.4f}"
            if right_value is not None:
                updates["TRUCK_MAX_X_M"] = f"{right_value:.4f}"
            if left_value is not None and right_value is not None and left_value >= right_value:
                raise ValueError("Left/right truck limits overlap or are inverted. Re-zero and re-mark before saving.")
            return updates

        updates = self.build_base_env_updates()
        left_value = self.axis.conservative_left(self.settings.limit_margin)
        right_value = self.axis.conservative_right(self.settings.limit_margin)
        if left_value is not None:
            updates["PAN_MIN_DEG"] = f"{left_value:.4f}"
        if right_value is not None:
            updates["PAN_MAX_DEG"] = f"{right_value:.4f}"
        if left_value is not None and right_value is not None and left_value >= right_value:
            raise ValueError("Left/right pan limits overlap or are inverted. Re-zero and re-mark before saving.")
        return updates

    def print_env_suggestions(self) -> None:
        print("Suggested env values:", flush=True)
        try:
            updates = self.build_env_updates()
        except ValueError as exc:
            updates = self.build_base_env_updates()
            for key, value in updates.items():
                print(f"  {key}={value}", flush=True)
            print(f"  WARNING: {exc}", flush=True)
            return
        for key, value in updates.items():
            print(f"  {key}={value}", flush=True)


def build_axis_state(axis_name: str) -> ManualAxisState:
    config = load_runtime_config()
    if axis_name == "truck":
        return ManualAxisState(
            axis_name="truck",
            motor_id=config.motor.truck_motor_id,
            driver_sign=config.motor.truck_sign,
            units_per_raw_speed_s=config.motor.truck_m_per_raw_speed_s,
            estimated_position=config.camera_pose.start_x_m,
            unit_name="x_m",
            configured_left_limit=config.motor.truck_min_x_m,
            configured_right_limit=config.motor.truck_max_x_m,
        )
    if axis_name == "pan":
        return ManualAxisState(
            axis_name="pan",
            motor_id=config.motor.pan_motor_id,
            driver_sign=config.motor.pan_sign,
            units_per_raw_speed_s=config.motor.pan_deg_per_raw_speed_s,
            estimated_position=config.camera_pose.start_pan_deg,
            unit_name="deg",
            configured_left_limit=config.motor.pan_min_deg,
            configured_right_limit=config.motor.pan_max_deg,
        )
    raise ValueError(f"Unsupported axis: {axis_name}")


def load_manual_settings(default_speed: int) -> ManualSettings:
    max_raw_speed = max(1, env_int("MOTOR_MANUAL_MAX_RAW_SPEED", 20))
    requested_speed = abs(env_int("MOTOR_MANUAL_JOG_RAW_SPEED", default_speed))
    jog_raw_speed = min(max_raw_speed, max(1, requested_speed))
    max_duration_s = max(0.02, env_float("MOTOR_MANUAL_MAX_DURATION_S", 0.30))
    requested_duration = env_float("MOTOR_MANUAL_JOG_DURATION_S", 0.10)
    jog_duration_s = min(max_duration_s, max(0.02, requested_duration))
    goto_max_duration_s = min(max_duration_s, max(0.02, env_float("MOTOR_MANUAL_GOTO_MAX_DURATION_S", min(jog_duration_s, 0.10))))
    return ManualSettings(
        jog_raw_speed=jog_raw_speed,
        jog_duration_s=jog_duration_s,
        max_raw_speed=max_raw_speed,
        max_duration_s=max_duration_s,
        settle_s=max(0.0, env_float("MOTOR_MANUAL_SETTLE_S", 0.20)),
        status_poll_s=max(0.01, env_float("MOTOR_MANUAL_STATUS_POLL_S", 0.02)),
        print_poll_samples=env_bool("MOTOR_MANUAL_PRINT_POLL_SAMPLES", False),
        goto_max_duration_s=goto_max_duration_s,
        goto_tolerance=max(0.0, env_float("MOTOR_MANUAL_GOTO_TOLERANCE", 0.01)),
        goto_max_steps=max(1, env_int("MOTOR_MANUAL_GOTO_MAX_STEPS", 50)),
        limit_margin=max(0.0, env_float("MOTOR_MANUAL_LIMIT_MARGIN_M", 0.05)),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual jog and limit calibration for one motor axis.")
    parser.add_argument("--axis", choices=("truck", "pan"), default="truck", help="Axis to control manually.")
    return parser


def upsert_env_values(env_path: Path, updates: dict[str, str]) -> None:
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    for key, value in updates.items():
        replacement = f"{key}={value}"
        for index, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith(f"{key}=") or stripped.startswith(f"export {key}="):
                lines[index] = replacement
                break
        else:
            lines.append(replacement)
    env_path.write_text("\n".join(lines).rstrip("\n") + "\n", encoding="utf-8")


def print_help(axis_name: str) -> None:
    print(f"Manual {axis_name} commands:", flush=True)
    print("  p                jog in the positive logical direction once", flush=True)
    print("  n                jog in the negative logical direction once", flush=True)
    print("  p <count>        repeat positive jog count times", flush=True)
    print("  n <count>        repeat negative jog count times", flush=True)
    print("  speed <raw>      set jog raw speed (clamped to MOTOR_MANUAL_MAX_RAW_SPEED)", flush=True)
    print("  duration <sec>   set jog duration (clamped to MOTOR_MANUAL_MAX_DURATION_S)", flush=True)
    print("  flip-sign        invert runtime motor sign without editing .env", flush=True)
    print("  zero             define the current position as 0 and record it as the startup center", flush=True)
    print("  mark-center      record the current position as the startup center without renormalizing", flush=True)
    print("  mark-left        record the current position as the physical left/min limit", flush=True)
    print("  mark-right       record the current position as the physical right/max limit", flush=True)
    print("  set-pos <value>  manually set the estimated position for this session", flush=True)
    print("  go <value>       move to an absolute target position in the current axis units", flush=True)
    print("  go-left          move to the saved left bound using dedicated conservative goto speed/pulse settings", flush=True)
    print("  go-right         move to the saved right bound using dedicated conservative goto speed/pulse settings", flush=True)
    print("  go-center        move to the center mark when set, otherwise fall back to the midpoint of left/right marks", flush=True)
    print("  status           print current status and suggested env values", flush=True)
    print("  stop             send an immediate stop", flush=True)
    print("  save-env         confirm and write the current suggested values into ../.env", flush=True)
    print("  help             print this help", flush=True)
    print("  quit             stop and exit", flush=True)


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_runtime_config()
    axis = build_axis_state(args.axis)
    default_speed = config.motor.truck_max_raw_speed if args.axis == "truck" else config.motor.pan_max_raw_speed
    settings = load_manual_settings(default_speed=min(default_speed, 10))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    session = ManualMotorSession(
        axis,
        settings,
        live=config.motor.enable_live,
        port=config.motor.port,
        baudrate=config.motor.baudrate,
        timeout_s=config.motor.timeout_s,
    )

    print(
        f"Starting manual mode for axis={axis.axis_name} motor_id={axis.motor_id} live={int(config.motor.enable_live)} ",
        flush=True,
    )
    if not config.motor.enable_live:
        print("MOTOR_ENABLE_LIVE=0, so this session is dry-run only and will not physically move the motor.", flush=True)
    print(
        "Use short pulses only, keep a hand near power/stop, and leave a safety margin before the hard end of travel.",
        flush=True,
    )
    print_help(axis.axis_name)

    try:
        session.connect()
        session.print_status()
        while True:
            try:
                raw_line = input("manual> ").strip()
            except EOFError:
                raw_line = "quit"
            if not raw_line:
                continue
            parts = raw_line.split()
            command = parts[0].lower()
            try:
                if command == "help":
                    print_help(axis.axis_name)
                elif command in ("p", "pos", "+"):
                    count = int(parts[1]) if len(parts) > 1 else 1
                    session.pulse(+1, count=count)
                    session.print_status()
                elif command in ("n", "neg", "-"):
                    count = int(parts[1]) if len(parts) > 1 else 1
                    session.pulse(-1, count=count)
                    session.print_status()
                elif command == "speed":
                    requested = abs(int(parts[1]))
                    session.settings = ManualSettings(
                        jog_raw_speed=min(session.settings.max_raw_speed, max(1, requested)),
                        jog_duration_s=session.settings.jog_duration_s,
                        max_raw_speed=session.settings.max_raw_speed,
                        max_duration_s=session.settings.max_duration_s,
                        settle_s=session.settings.settle_s,
                        status_poll_s=session.settings.status_poll_s,
                        print_poll_samples=session.settings.print_poll_samples,
                        goto_max_duration_s=session.settings.goto_max_duration_s,
                        goto_tolerance=session.settings.goto_tolerance,
                        goto_max_steps=session.settings.goto_max_steps,
                        limit_margin=session.settings.limit_margin,
                    )
                    session.print_status()
                elif command == "duration":
                    requested = float(parts[1])
                    session.settings = ManualSettings(
                        jog_raw_speed=session.settings.jog_raw_speed,
                        jog_duration_s=min(session.settings.max_duration_s, max(0.02, requested)),
                        max_raw_speed=session.settings.max_raw_speed,
                        max_duration_s=session.settings.max_duration_s,
                        settle_s=session.settings.settle_s,
                        status_poll_s=session.settings.status_poll_s,
                        print_poll_samples=session.settings.print_poll_samples,
                        goto_max_duration_s=session.settings.goto_max_duration_s,
                        goto_tolerance=session.settings.goto_tolerance,
                        goto_max_steps=session.settings.goto_max_steps,
                        limit_margin=session.settings.limit_margin,
                    )
                    session.print_status()
                elif command == "flip-sign":
                    session.flip_sign()
                    session.print_status()
                elif command == "zero":
                    session.axis.estimated_position = 0.0
                    session.axis.center_mark = 0.0
                    session.print_status()
                elif command == "mark-center":
                    session.axis.center_mark = session.axis.estimated_position
                    session.print_status()
                elif command == "mark-left":
                    session.axis.left_mark = session.axis.estimated_position
                    session.print_status()
                elif command == "mark-right":
                    session.axis.right_mark = session.axis.estimated_position
                    session.print_status()
                elif command == "set-pos":
                    session.axis.estimated_position = float(parts[1])
                    session.print_status()
                elif command == "go":
                    target_value = float(parts[1])
                    session.go_to_position(
                        target_value,
                        label=f"requested {session.axis.unit_name} position",
                    )
                    session.print_status()
                elif command == "go-left":
                    session.go_to_left()
                    session.print_status()
                elif command == "go-right":
                    session.go_to_right()
                    session.print_status()
                elif command == "go-center":
                    session.go_to_center()
                    session.print_status()
                elif command == "status":
                    session.print_status()
                elif command == "stop":
                    session.stop()
                    session.print_status()
                elif command == "save-env":
                    try:
                        updates = session.build_env_updates()
                    except ValueError as exc:
                        print(str(exc), flush=True)
                        continue
                    env_path = REPO_ROOT / ".env"
                    print(f"About to write these values into {env_path}:", flush=True)
                    for key, value in updates.items():
                        print(f"  {key}={value}", flush=True)
                    confirm = input("Type YES to save these values: ").strip()
                    if confirm == "YES":
                        upsert_env_values(env_path, updates)
                        print(f"Saved {len(updates)} values to {env_path}", flush=True)
                    else:
                        print("Cancelled .env update.", flush=True)
                elif command in ("quit", "exit"):
                    break
                else:
                    print(f"Unknown command: {command}", flush=True)
                    print_help(axis.axis_name)
            except (IndexError, ValueError):
                print("Invalid command arguments. Try `help`.", flush=True)
    except KeyboardInterrupt:
        print("Interrupted, stopping motor.", flush=True)
    finally:
        session.close()
        print("Manual mode stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

