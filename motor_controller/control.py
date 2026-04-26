"""Safe Modbus motor-driver layer used by the autocam motor controller.

The application talks to this module through a small speed/run/stop/status API.
Driver-specific register maps live behind protocol classes so pan and truck can
share one RS485 port while using different driver models.
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

# BLD-305S register map.
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

# BLD-510B register map. Verify these against the shipped driver manual.
REG_510_CONTROL = 0x8000
REG_510_SPEED = 0x8005
REG_510_ACTUAL_SPEED = 0x8018
REG_510_FAULT = 0x801B
BLD510B_MAX_RPM = 3000


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


class SharedModbusBus:
    """Own the single serial Modbus connection shared by all driver protocols."""

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
    def _raise_if_error(result, action: str, motor_id: int, addr: int) -> None:
        if result is None:
            raise RuntimeError(f"{action} failed motor={motor_id} addr=0x{addr:04X}: no response")
        if hasattr(result, "isError") and result.isError():
            raise RuntimeError(f"{action} failed motor={motor_id} addr=0x{addr:04X}: {result}")

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
        self._raise_if_error(result, "Read", motor_id, addr)
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
        self._raise_if_error(result, "Write", motor_id, addr)


class Bld305sProtocol:
    """BLD-305S speed-mode register behavior."""

    def __init__(self, bus: SharedModbusBus) -> None:
        self.bus = bus

    def configure_speed_mode(self, motor_id: int) -> None:
        self.bus.write_reg(motor_id, REG_CONTROL_MODE, CONTROL_MODE_SPEED)

    def clear_fault(self, motor_id: int) -> None:
        self.bus.write_reg(motor_id, REG_FAULT_CLEAR, 0)

    def read_accel_decel_raw(self, motor_id: int) -> int:
        return int(self.bus.read_reg(motor_id, REG_ACCEL_DECEL)[0]) & 0xFFFF

    def write_accel_decel_raw(self, motor_id: int, value: int) -> None:
        self.bus.write_reg(motor_id, REG_ACCEL_DECEL, int(value) & 0xFFFF)

    def save_parameters(self, motor_id: int) -> None:
        self.bus.write_reg(motor_id, REG_SAVE, SAVE_MAGIC)

    def set_speed(self, motor_id: int, raw_speed: int) -> None:
        # The BLD-305S expects a non-negative speed magnitude; direction is separate.
        self.bus.write_reg(motor_id, REG_SPEED_SET, max(0, min(abs(int(raw_speed)), 4000)))

    def run_forward(self, motor_id: int) -> None:
        self.bus.write_reg(motor_id, REG_RUN_CMD, RUN_FORWARD)

    def run_reverse(self, motor_id: int) -> None:
        self.bus.write_reg(motor_id, REG_RUN_CMD, RUN_REVERSE)

    def run(self, motor_id: int, direction: int = 1) -> None:
        if int(direction) < 0:
            self.run_reverse(motor_id)
            return
        self.run_forward(motor_id)

    def stop(self, motor_id: int) -> None:
        try:
            self.set_speed(motor_id, 0)
        finally:
            self.bus.write_reg(motor_id, REG_RUN_CMD, RUN_STOP)

    def read_status(self, motor_id: int) -> MotorStatus:
        actual_speed = self.bus.read_reg(motor_id, REG_ACTUAL_SPEED)[0]
        run_status = self.bus.read_reg(motor_id, REG_RUN_STATUS)[0]
        fault_code = self.bus.read_reg(motor_id, REG_FAULT_CODE)[0]
        return MotorStatus(
            motor_id=motor_id,
            actual_speed_raw=decode_signed_16(actual_speed),
            run_status=run_status,
            fault_code=fault_code,
        )


class Bld510bProtocol:
    """BLD-510B RS485/internal-speed register behavior."""

    def __init__(self, bus: SharedModbusBus, pole_pairs: int = 2) -> None:
        self.bus = bus
        self.pole_pairs = max(1, min(int(pole_pairs), 255))

    @staticmethod
    def _swap16(value: int) -> int:
        value = int(value) & 0xFFFF
        return ((value & 0x00FF) << 8) | ((value & 0xFF00) >> 8)

    def _control_word(self, enable: bool, direction: int = 1, brake: bool = False) -> int:
        flags = 0
        if enable:
            flags |= 0x01  # EN
        if int(direction) < 0:
            flags |= 0x02  # FR
        if brake:
            flags |= 0x04  # BK
        flags |= 0x08  # NW: RS485 controls start/stop/speed.
        return ((flags & 0xFF) << 8) | (self.pole_pairs & 0xFF)

    def configure_speed_mode(self, motor_id: int) -> None:
        self.bus.write_reg(motor_id, REG_510_CONTROL, self._control_word(enable=False))

    def clear_fault(self, motor_id: int) -> None:
        LOG.info("bld510b_clear_fault_noop motor=%s", motor_id)

    def set_speed(self, motor_id: int, raw_speed: int) -> None:
        rpm = max(0, min(abs(int(raw_speed)), BLD510B_MAX_RPM))
        self.bus.write_reg(motor_id, REG_510_SPEED, self._swap16(rpm))

    def run(self, motor_id: int, direction: int = 1) -> None:
        self.bus.write_reg(
            motor_id,
            REG_510_CONTROL,
            self._control_word(enable=True, direction=direction),
        )

    def stop(self, motor_id: int) -> None:
        try:
            self.set_speed(motor_id, 0)
        finally:
            self.bus.write_reg(motor_id, REG_510_CONTROL, self._control_word(enable=False))

    def read_status(self, motor_id: int) -> MotorStatus:
        actual_speed = self.bus.read_reg(motor_id, REG_510_ACTUAL_SPEED)[0]
        fault_raw = self.bus.read_reg(motor_id, REG_510_FAULT)[0]
        fault_code = (fault_raw >> 8) & 0xFF
        return MotorStatus(
            motor_id=motor_id,
            actual_speed_raw=decode_signed_16(actual_speed),
            run_status=None,
            fault_code=fault_code,
        )


class MixedMotorBus:
    """Route motor operations to the protocol configured for each motor id."""

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        timeout_s: float = 1.0,
        live: bool = False,
        pan_motor_id: Optional[int] = None,
        truck_motor_id: Optional[int] = None,
        pan_driver_model: str = "BLD305S",
        truck_driver_model: str = "BLD305S",
        pan_pole_pairs: int = 2,
        truck_pole_pairs: int = 5,
    ) -> None:
        self.bus = SharedModbusBus(port, baudrate=baudrate, timeout_s=timeout_s, live=live)
        self.motor_protocols = {}
        if pan_motor_id is not None:
            self.motor_protocols[int(pan_motor_id)] = self._make_protocol(
                pan_driver_model,
                pan_pole_pairs,
            )
        if truck_motor_id is not None:
            self.motor_protocols[int(truck_motor_id)] = self._make_protocol(
                truck_driver_model,
                truck_pole_pairs,
            )

    @property
    def connected(self) -> bool:
        return self.bus.connected

    def _make_protocol(self, model: str, pole_pairs: int):
        normalized = str(model).strip().upper().replace("-", "").replace("_", "")
        if normalized == "BLD305S":
            return Bld305sProtocol(self.bus)
        if normalized == "BLD510B":
            return Bld510bProtocol(self.bus, pole_pairs=pole_pairs)
        raise ValueError(f"Unsupported motor driver model: {model}")

    def connect(self) -> None:
        self.bus.connect()

    def close(self) -> None:
        self.bus.close()

    def _protocol(self, motor_id: int):
        try:
            return self.motor_protocols[int(motor_id)]
        except KeyError as exc:
            raise RuntimeError(f"No motor protocol configured for motor ID {motor_id}") from exc

    def configure_speed_mode(self, motor_id: int) -> None:
        self._protocol(motor_id).configure_speed_mode(motor_id)

    def clear_fault(self, motor_id: int) -> None:
        self._protocol(motor_id).clear_fault(motor_id)

    def set_speed(self, motor_id: int, raw_speed: int) -> None:
        self._protocol(motor_id).set_speed(motor_id, raw_speed)

    def run(self, motor_id: int, direction: int = 1) -> None:
        self._protocol(motor_id).run(motor_id, direction)

    def stop(self, motor_id: int) -> None:
        self._protocol(motor_id).stop(motor_id)

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
        return self._protocol(motor_id).read_status(motor_id)

    def read_accel_decel_raw(self, motor_id: int) -> int:
        protocol = self._protocol(motor_id)
        if not hasattr(protocol, "read_accel_decel_raw"):
            raise RuntimeError(f"Driver for motor ID {motor_id} does not expose accel/decel registers")
        return protocol.read_accel_decel_raw(motor_id)

    def write_accel_decel_raw(self, motor_id: int, value: int) -> None:
        protocol = self._protocol(motor_id)
        if not hasattr(protocol, "write_accel_decel_raw"):
            raise RuntimeError(f"Driver for motor ID {motor_id} does not expose accel/decel registers")
        protocol.write_accel_decel_raw(motor_id, value)

    def save_parameters(self, motor_id: int) -> None:
        protocol = self._protocol(motor_id)
        if not hasattr(protocol, "save_parameters"):
            raise RuntimeError(f"Driver for motor ID {motor_id} does not expose save-parameters")
        protocol.save_parameters(motor_id)


class Bld305sMotorBus(MixedMotorBus):
    """Compatibility wrapper for tools that only target BLD-305S drivers."""

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        timeout_s: float = 1.0,
        live: bool = False,
    ) -> None:
        super().__init__(
            port=port,
            baudrate=baudrate,
            timeout_s=timeout_s,
            live=live,
            pan_motor_id=1,
            truck_motor_id=2,
            pan_driver_model="BLD305S",
            truck_driver_model="BLD305S",
        )
