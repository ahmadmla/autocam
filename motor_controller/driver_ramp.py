"""Read and set BLD-305S acceleration/deceleration parameters.

The BLD-305S documentation exposes acceleration and deceleration as a shared
register at 0x00E6, with values expressed in 0.1s units. The manual is vague
about byte layout; this helper follows the documented ordering and stores:

- high byte: acceleration time
- low byte: deceleration time
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional

from .config import load_runtime_config
from .control import MixedMotorBus


@dataclass(frozen=True)
class RampTimes:
    accel_ticks: int
    decel_ticks: int

    @property
    def accel_s(self) -> float:
        return self.accel_ticks / 10.0

    @property
    def decel_s(self) -> float:
        return self.decel_ticks / 10.0


def _clamp_tick(value: int) -> int:
    return max(0, min(int(value), 255))


def _seconds_to_ticks(value_s: Optional[float]) -> Optional[int]:
    if value_s is None:
        return None
    return _clamp_tick(round(float(value_s) * 10.0))


def _decode_ramp_times(raw_value: int) -> RampTimes:
    value = int(raw_value) & 0xFFFF
    return RampTimes(
        accel_ticks=(value >> 8) & 0xFF,
        decel_ticks=value & 0xFF,
    )


def _encode_ramp_times(times: RampTimes) -> int:
    return ((_clamp_tick(times.accel_ticks) & 0xFF) << 8) | (_clamp_tick(times.decel_ticks) & 0xFF)


def _format_ramp(prefix: str, raw_value: int, times: RampTimes) -> str:
    return (
        f"{prefix} raw=0x{int(raw_value) & 0xFFFF:04X} "
        f"accel={times.accel_ticks} ({times.accel_s:.1f}s) "
        f"decel={times.decel_ticks} ({times.decel_s:.1f}s)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read or update the BLD-305S driver acceleration/deceleration ramp.",
    )
    parser.add_argument(
        "--axis",
        choices=("pan", "truck"),
        default="truck",
        help="Which configured motor driver to talk to.",
    )
    parser.add_argument(
        "--accel-s",
        type=float,
        default=None,
        help="Acceleration time in seconds (driver units are 0.1s, range 0.0-25.5).",
    )
    parser.add_argument(
        "--decel-s",
        type=float,
        default=None,
        help="Deceleration time in seconds (driver units are 0.1s, range 0.0-25.5).",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Persist the updated values to the driver's non-volatile storage.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_runtime_config()
    motor_id = config.motor.truck_motor_id if args.axis == "truck" else config.motor.pan_motor_id

    bus = MixedMotorBus(
        port=config.motor.port,
        baudrate=config.motor.baudrate,
        timeout_s=config.motor.timeout_s,
        live=config.motor.enable_live,
        pan_motor_id=config.motor.pan_motor_id,
        truck_motor_id=config.motor.truck_motor_id,
        pan_driver_model=config.motor.pan_driver_model,
        truck_driver_model=config.motor.truck_driver_model,
        pan_pole_pairs=config.motor.pan_pole_pairs,
        truck_pole_pairs=config.motor.truck_pole_pairs,
    )

    try:
        bus.connect()
        try:
            current_raw = bus.read_accel_decel_raw(motor_id)
        except RuntimeError as exc:
            print(str(exc), flush=True)
            return 2
        current_times = _decode_ramp_times(current_raw)
        print(_format_ramp("current", current_raw, current_times), flush=True)

        accel_ticks = _seconds_to_ticks(args.accel_s)
        decel_ticks = _seconds_to_ticks(args.decel_s)
        if accel_ticks is None and decel_ticks is None:
            return 0

        next_times = RampTimes(
            accel_ticks=current_times.accel_ticks if accel_ticks is None else accel_ticks,
            decel_ticks=current_times.decel_ticks if decel_ticks is None else decel_ticks,
        )
        next_raw = _encode_ramp_times(next_times)
        bus.write_accel_decel_raw(motor_id, next_raw)
        print(_format_ramp("updated", next_raw, next_times), flush=True)

        if args.save:
            bus.save_parameters(motor_id)
            print("saved driver parameters", flush=True)
        else:
            print("not saved; power-cycling may restore the previous values", flush=True)
        return 0
    finally:
        bus.close()
