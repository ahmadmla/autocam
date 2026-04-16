"""Interactive manual jog and limit calibration mode for one motor axis."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from typing import Optional

from .config import env_float, env_int, load_runtime_config
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
    left_mark: Optional[float] = None
    right_mark: Optional[float] = None

    def apply_logical_pulse(self, logical_raw: int, duration_s: float) -> None:
        self.estimated_position += logical_raw * self.units_per_raw_speed_s * duration_s

    def conservative_left(self, margin: float) -> Optional[float]:
        if self.left_mark is None:
            return None
        return self.left_mark + abs(margin)

    def conservative_right(self, margin: float) -> Optional[float]:
        if self.right_mark is None:
            return None
        return self.right_mark - abs(margin)


@dataclass(frozen=True)
class ManualSettings:
    jog_raw_speed: int
    jog_duration_s: float
    max_raw_speed: int
    max_duration_s: float
    settle_s: float
    limit_margin: float


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
            self.stop()
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

    def pulse(self, logical_direction: int, *, count: int = 1) -> None:
        count = max(1, int(count))
        logical_raw = int(logical_direction) * self.settings.jog_raw_speed
        driver_raw = logical_raw * self.axis.driver_sign
        for index in range(count):
            self.check_fault()
            self.motor_bus.set_speed(self.axis.motor_id, driver_raw)
            self.motor_bus.run(self.axis.motor_id)
            LOG.info(
                "manual_pulse axis=%s step=%s/%s logical_raw=%s driver_raw=%s duration_s=%.3f",
                self.axis.axis_name,
                index + 1,
                count,
                logical_raw,
                driver_raw,
                self.settings.jog_duration_s,
            )
            time.sleep(self.settings.jog_duration_s)
            self.stop()
            self.axis.apply_logical_pulse(logical_raw, self.settings.jog_duration_s)
            time.sleep(self.settings.settle_s)
            self.check_fault()

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
            f"settle_s={self.settings.settle_s:.3f} actual_speed_raw={status.actual_speed_raw} fault={status.fault_code}",
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
        self.print_env_suggestions()

    def print_env_suggestions(self) -> None:
        if self.axis.axis_name == "truck":
            print("Suggested env values:", flush=True)
            print(f"  TRUCK_SIGN={self.axis.driver_sign}", flush=True)
            print(f"  CAMERA_START_X_M={self.axis.estimated_position:.4f}    # if you zeroed at your desired startup center", flush=True)
            if self.axis.left_mark is not None:
                print(f"  TRUCK_MIN_X_M={self.axis.conservative_left(self.settings.limit_margin):.4f}", flush=True)
            if self.axis.right_mark is not None:
                print(f"  TRUCK_MAX_X_M={self.axis.conservative_right(self.settings.limit_margin):.4f}", flush=True)
        else:
            print("Suggested env values:", flush=True)
            print(f"  PAN_SIGN={self.axis.driver_sign}", flush=True)
            print(f"  CAMERA_START_PAN_DEG={self.axis.estimated_position:.4f}    # if you zeroed at your desired startup center", flush=True)
            if self.axis.left_mark is not None:
                print(f"  PAN_MIN_DEG={self.axis.conservative_left(self.settings.limit_margin):.4f}", flush=True)
            if self.axis.right_mark is not None:
                print(f"  PAN_MAX_DEG={self.axis.conservative_right(self.settings.limit_margin):.4f}", flush=True)


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
        )
    if axis_name == "pan":
        return ManualAxisState(
            axis_name="pan",
            motor_id=config.motor.pan_motor_id,
            driver_sign=config.motor.pan_sign,
            units_per_raw_speed_s=config.motor.pan_deg_per_raw_speed_s,
            estimated_position=config.camera_pose.start_pan_deg,
            unit_name="deg",
        )
    raise ValueError(f"Unsupported axis: {axis_name}")


def load_manual_settings(default_speed: int) -> ManualSettings:
    max_raw_speed = max(1, env_int("MOTOR_MANUAL_MAX_RAW_SPEED", 20))
    requested_speed = abs(env_int("MOTOR_MANUAL_JOG_RAW_SPEED", default_speed))
    jog_raw_speed = min(max_raw_speed, max(1, requested_speed))
    max_duration_s = max(0.02, env_float("MOTOR_MANUAL_MAX_DURATION_S", 0.30))
    requested_duration = env_float("MOTOR_MANUAL_JOG_DURATION_S", 0.10)
    jog_duration_s = min(max_duration_s, max(0.02, requested_duration))
    return ManualSettings(
        jog_raw_speed=jog_raw_speed,
        jog_duration_s=jog_duration_s,
        max_raw_speed=max_raw_speed,
        max_duration_s=max_duration_s,
        settle_s=max(0.0, env_float("MOTOR_MANUAL_SETTLE_S", 0.20)),
        limit_margin=max(0.0, env_float("MOTOR_MANUAL_LIMIT_MARGIN_M", 0.05)),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual jog and limit calibration for one motor axis.")
    parser.add_argument("--axis", choices=("truck", "pan"), default="truck", help="Axis to control manually.")
    return parser


def print_help(axis_name: str) -> None:
    print(f"Manual {axis_name} commands:", flush=True)
    print("  p                jog in the positive logical direction once", flush=True)
    print("  n                jog in the negative logical direction once", flush=True)
    print("  p <count>        repeat positive jog count times", flush=True)
    print("  n <count>        repeat negative jog count times", flush=True)
    print("  speed <raw>      set jog raw speed (clamped to MOTOR_MANUAL_MAX_RAW_SPEED)", flush=True)
    print("  duration <sec>   set jog duration (clamped to MOTOR_MANUAL_MAX_DURATION_S)", flush=True)
    print("  flip-sign        invert runtime motor sign without editing .env", flush=True)
    print("  zero             define the current position as 0 for this session", flush=True)
    print("  mark-left        record the current position as the physical left/min limit", flush=True)
    print("  mark-right       record the current position as the physical right/max limit", flush=True)
    print("  set-pos <value>  manually set the estimated position for this session", flush=True)
    print("  status           print current status and suggested env values", flush=True)
    print("  stop             send an immediate stop", flush=True)
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
                        limit_margin=session.settings.limit_margin,
                    )
                    session.print_status()
                elif command == "flip-sign":
                    session.flip_sign()
                    session.print_status()
                elif command == "zero":
                    session.axis.estimated_position = 0.0
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
                elif command == "status":
                    session.print_status()
                elif command == "stop":
                    session.stop()
                    session.print_status()
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
