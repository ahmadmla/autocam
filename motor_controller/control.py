"""Safe BLD-305S Modbus wrapper used by the autocam motor controller.

This module intentionally exposes only speed/run/stop/status operations. The current
hardware has driver status feedback only, so higher layers must not treat these
commands as absolute position control.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

try:
    from pymodbus.client import ModbusSerialClient
except ImportError:  # pragma: no cover - dry-run development can work without pymodbus.
    ModbusSerialClient = None

LOG = logging.getLogger(__name__)

REG_SPEED_SET = 0x0056
REG_RUN_CMD = 0x0066
REG_FAULT_CLEAR = 0x0076
REG_DRIVE_ADDR = 0x00A6
REG_ACCEL_DECEL = 0x00E6
REG_CONTROL_MODE = 0x0136
REG_SAVE = 0x80FF
SAVE_MAGIC = 0x55AA

REG_ACTUAL_SPEED = 0x005F
REG_RUN_STATUS = 0x0066
REG_FAULT_CODE = 0x0076
REG_ACTUAL_CURRENT = 0x00B6
REG_ACTUAL_VOLTAGE = 0x00C6

RUN_STOP = 0
RUN_FORWARD = 1
RUN_REVERSE = 2
CONTROL_MODE_SPEED = 1


def encode_signed_16(value: int) -> int:
    """Encode a signed raw driver value into one Modbus register."""
    return int(value) & 0xFFFF


def decode_signed_16(value: int) -> int:
    """Decode a signed raw driver value from one Modbus register."""
    raw = int(value) & 0xFFFF
    return raw - 0x10000 if raw & 0x8000 else raw


@dataclass(frozen=True)
class MotorStatus:
    motor_id: int
    actual_speed_raw: Optional[int] = None
    run_status: Optional[int] = None
    fault_code: Optional[int] = None
    current_raw: Optional[int] = None
    voltage_raw: Optional[int] = None

    @property
    def has_fault(self) -> bool:
        return self.fault_code not in (None, 0)


class Bld305sMotorBus:
    """Small guarded wrapper around BLD-305S speed-mode Modbus commands."""

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        timeout_s: float = 1.0,
        live: bool = False,
    ) -> None:
        self.port = port
        self.baudrate = int(baudrate)
        self.timeout_s = float(timeout_s)
        self.live = bool(live)
        self.client = None
        self.connected = False

    def connect(self) -> None:
        if not self.live:
            LOG.warning("motor_bus=dry_run port=%s baud=%s", self.port, self.baudrate)
            self.connected = True
            return
        if ModbusSerialClient is None:
            raise RuntimeError("pymodbus is not installed. Install requirements.txt first.")

        self.client = ModbusSerialClient(
            port=self.port,
            baudrate=self.baudrate,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=self.timeout_s,
        )
        if not self.client.connect():
            raise RuntimeError(f"Could not connect to motor driver bus on {self.port}")
        self.connected = True
        LOG.info("motor_bus=connected port=%s baud=%s", self.port, self.baudrate)

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
        self.connected = False

    def _require_connected(self) -> None:
        if not self.connected:
            raise RuntimeError("Motor bus is not connected")

    def _call_with_motor_id(self, fn, motor_id: int, **kwargs):
        last_error = None
        for key in ("device_id", "slave", "unit"):
            try:
                return fn(**kwargs, **{key: motor_id})
            except TypeError as exc:
                last_error = exc
        raise last_error or RuntimeError("Unable to call pymodbus method with motor id")

    @staticmethod
    def _raise_if_error(result, action: str, addr: int) -> None:
        if result is None:
            raise RuntimeError(f"{action} failed at 0x{addr:04X}: no response")
        if hasattr(result, "isError") and result.isError():
            raise RuntimeError(f"{action} failed at 0x{addr:04X}: {result}")

    def read_reg(self, motor_id: int, addr: int, count: int = 1) -> list[int]:
        self._require_connected()
        if not self.live:
            LOG.debug("motor_bus=dry_read motor=%s addr=0x%04X count=%s", motor_id, addr, count)
            return [0] * int(count)
        result = self._call_with_motor_id(
            self.client.read_holding_registers,
            motor_id,
            address=addr,
            count=count,
        )
        self._raise_if_error(result, "Read", addr)
        return list(result.registers)

    def write_reg(self, motor_id: int, addr: int, value: int) -> None:
        self._require_connected()
        encoded = int(value) & 0xFFFF
        if not self.live:
            LOG.debug("motor_bus=dry_write motor=%s addr=0x%04X value=%s", motor_id, addr, encoded)
            return
        result = self._call_with_motor_id(
            self.client.write_register,
            motor_id,
            address=addr,
            value=encoded,
        )
        self._raise_if_error(result, "Write", addr)

    def configure_speed_mode(self, motor_id: int) -> None:
        self.write_reg(motor_id, REG_CONTROL_MODE, CONTROL_MODE_SPEED)

    def clear_fault(self, motor_id: int) -> None:
        self.write_reg(motor_id, REG_FAULT_CLEAR, 0)

    def read_accel_decel_raw(self, motor_id: int) -> int:
        return int(self.read_reg(motor_id, REG_ACCEL_DECEL)[0]) & 0xFFFF

    def write_accel_decel_raw(self, motor_id: int, value: int) -> None:
        self.write_reg(motor_id, REG_ACCEL_DECEL, int(value) & 0xFFFF)

    def save_parameters(self, motor_id: int) -> None:
        self.write_reg(motor_id, REG_SAVE, SAVE_MAGIC)

    def set_speed(self, motor_id: int, raw_speed: int) -> None:
        # The BLD-305S expects a non-negative speed magnitude; direction is a separate run command.
        self.write_reg(motor_id, REG_SPEED_SET, max(0, min(abs(int(raw_speed)), 4000)))

    def run_forward(self, motor_id: int) -> None:
        self.write_reg(motor_id, REG_RUN_CMD, RUN_FORWARD)

    def run_reverse(self, motor_id: int) -> None:
        self.write_reg(motor_id, REG_RUN_CMD, RUN_REVERSE)

    def run(self, motor_id: int, direction: int = 1) -> None:
        if int(direction) < 0:
            self.run_reverse(motor_id)
            return
        self.run_forward(motor_id)

    def stop(self, motor_id: int) -> None:
        try:
            self.set_speed(motor_id, 0)
        finally:
            self.write_reg(motor_id, REG_RUN_CMD, RUN_STOP)

    def stop_all(self, motor_ids: Iterable[int]) -> None:
        first_error: Optional[BaseException] = None
        for motor_id in motor_ids:
            try:
                self.stop(int(motor_id))
            except BaseException as exc:  # keep trying to stop every axis.
                LOG.exception("motor_stop_failed motor=%s", motor_id)
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def read_status(self, motor_id: int) -> MotorStatus:
        actual_speed = self.read_reg(motor_id, REG_ACTUAL_SPEED)[0]
        run_status = self.read_reg(motor_id, REG_RUN_STATUS)[0]
        fault_code = self.read_reg(motor_id, REG_FAULT_CODE)[0]
        current_raw = self.read_reg(motor_id, REG_ACTUAL_CURRENT)[0]
        voltage_raw = self.read_reg(motor_id, REG_ACTUAL_VOLTAGE)[0]
        return MotorStatus(
            motor_id=motor_id,
            actual_speed_raw=actual_speed,
            run_status=run_status,
            fault_code=fault_code,
            current_raw=current_raw,
            voltage_raw=voltage_raw,
        )


