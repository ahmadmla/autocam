"""Dynamic homography motor-control entrypoint for pan/truck autocam."""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    mqtt = None

from .config import RuntimeConfig, load_runtime_config
from .control import Bld305sMotorBus, MotorStatus, RUN_FORWARD, RUN_REVERSE
from .geometry import (
    CameraPoseEstimator,
    ProjectedPoint,
    ProjectionError,
    WorldPoint,
    clamp,
    project_floor_point,
)

LOG = logging.getLogger("motor_controller")


@dataclass
class FilteredPoseMessage:
    node_id: str
    timestamp: float
    x_m: float
    y_m: float
    quality_score: float
    pose_mode: Optional[str]
    filter_mode: Optional[str]
    stationary_locked: bool
    sequence_number: Optional[int]
    received_monotonic: float


@dataclass
class VisionCorrection:
    node_id: str
    timestamp: float
    x_px: float
    y_px: float
    confidence: float
    received_monotonic: float


@dataclass
class AxisCommand:
    pan_raw: int
    truck_raw: int
    mode: str
    projected: Optional[ProjectedPoint]
    error_x_px: Optional[float]
    source: str


class MotorControllerApp:
    def __init__(self, config: RuntimeConfig) -> None:
        if mqtt is None:
            raise RuntimeError("paho-mqtt is not installed. Install requirements.txt first.")
        self.config = config
        if not (config.motor.pan_enabled or config.motor.truck_enabled):
            raise RuntimeError("At least one motor axis must be enabled.")
        self.pose_estimator = CameraPoseEstimator(config.camera_pose, config.motor)
        self.motor_bus = Bld305sMotorBus(
            port=config.motor.port,
            baudrate=config.motor.baudrate,
            timeout_s=config.motor.timeout_s,
            live=config.motor.enable_live,
        )
        self.poses: Dict[str, FilteredPoseMessage] = {}
        self.selected_node = config.mqtt.target_node
        self.vision_correction: Optional[VisionCorrection] = None
        self.armed = False
        self.running = True
        self.mqtt_connected = False
        self.last_loop_monotonic: Optional[float] = None
        self.last_pose_poll_monotonic: Optional[float] = None
        self.last_log_monotonic = 0.0
        self.centered_since_monotonic: Optional[float] = None
        self.truck_error_filtered = 0.0
        self.logical_pan_command = 0
        self.logical_truck_command = 0
        self.driver_pan_command = 0
        self.driver_truck_command = 0
        self.latest_actual_pan_raw = 0.0
        self.latest_actual_truck_raw = 0.0
        self.latest_statuses: Dict[int, MotorStatus] = {}

        try:
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id="autocam_motor_controller",
            )
        except AttributeError:
            self.client = mqtt.Client(client_id="autocam_motor_controller")
        if config.mqtt.username:
            self.client.username_pw_set(config.mqtt.username, config.mqtt.password)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)

    @property
    def motor_ids(self) -> Tuple[int, ...]:
        motor_ids = []
        if self.config.motor.pan_enabled:
            motor_ids.append(self.config.motor.pan_motor_id)
        if self.config.motor.truck_enabled:
            motor_ids.append(self.config.motor.truck_motor_id)
        return tuple(motor_ids)

    def stop_motors(self) -> None:
        self.logical_pan_command = 0
        self.logical_truck_command = 0
        self.driver_pan_command = 0
        self.driver_truck_command = 0
        self.latest_statuses: Dict[int, MotorStatus] = {}
        self.motor_bus.stop_all(self.motor_ids)

    def arm(self) -> None:
        pose = self.pose_estimator.reset_to_start()
        self.armed = True
        self.centered_since_monotonic = None
        self.truck_error_filtered = 0.0
        LOG.warning(
            "motor_state=armed pose_estimate=rail=%.3f x=%.3f y=%.3f pan=%.2f live=%s",
            pose.rail_position_m,
            pose.x_m,
            pose.y_m,
            pose.pan_deg,
            int(self.config.motor.enable_live),
        )

    def disarm(self, reason: str) -> None:
        if self.armed:
            LOG.warning("motor_state=disarmed reason=%s", reason)
        self.armed = False
        self.pose_estimator.invalidate()
        self.stop_motors()

    def maybe_prompt_arm(self) -> None:
        if self.config.motor.confirm_recentered:
            self.arm()
            return
        if not self.config.motor.arm_prompt or not sys.stdin.isatty():
            LOG.warning(
                "motor_state=unarmed_invalid_pose reason=awaiting_recenter_confirmation arm_topic=%s",
                self.config.mqtt.motor_arm_topic,
            )
            return
        answer = input(
            "Manually center pan/truck at configured startup pose, then type ARM to enable motors: "
        ).strip()
        if answer.upper() == "ARM":
            self.arm()
        else:
            LOG.warning("motor_state=unarmed_invalid_pose reason=operator_did_not_arm")

    def on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self.mqtt_connected = True
        pose_topic = f"{self.config.mqtt.pose_topic_base}/+"
        client.subscribe(pose_topic, qos=self.config.mqtt.qos)
        client.subscribe(self.config.mqtt.target_select_topic, qos=self.config.mqtt.qos)
        client.subscribe(self.config.mqtt.vision_correction_topic, qos=self.config.mqtt.qos)
        client.subscribe(self.config.mqtt.motor_arm_topic, qos=self.config.mqtt.qos)
        LOG.info(
            "mqtt=connected rc=%s pose_topic=%s target_topic=%s arm_topic=%s",
            reason_code,
            pose_topic,
            self.config.mqtt.target_select_topic,
            self.config.mqtt.motor_arm_topic,
        )

    def on_disconnect(self, client, userdata, disconnect_flags=None, reason_code=None, properties=None) -> None:
        self.mqtt_connected = False
        LOG.warning("mqtt=disconnected rc=%s", reason_code)
        try:
            self.stop_motors()
        except Exception:
            LOG.exception("mqtt_disconnect_stop_failed")

    def on_message(self, client, userdata, msg) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            topic = str(msg.topic)
            if topic.startswith(f"{self.config.mqtt.pose_topic_base}/"):
                self.handle_pose(payload)
            elif topic == self.config.mqtt.target_select_topic:
                self.handle_target_select(payload)
            elif topic == self.config.mqtt.vision_correction_topic:
                self.handle_vision_correction(payload)
            elif topic == self.config.mqtt.motor_arm_topic:
                self.handle_arm(payload)
        except Exception as exc:
            LOG.exception("mqtt_message_error topic=%s type=%s detail=%s", msg.topic, type(exc).__name__, exc)
            try:
                self.stop_motors()
            except Exception:
                LOG.exception("message_error_stop_failed")

    def handle_pose(self, payload: dict) -> None:
        # Motor control intentionally consumes the final filtered pose only.
        pose = payload.get("pose") or {}
        quality = payload.get("quality") or {}
        node_id = str(payload.get("node_id") or "")
        if not node_id:
            return
        self.poses[node_id] = FilteredPoseMessage(
            node_id=node_id,
            timestamp=float(payload.get("timestamp", time.time())),
            x_m=float(pose["x_m"]),
            y_m=float(pose["y_m"]),
            quality_score=float(quality.get("score", 0.0)),
            pose_mode=quality.get("pose_mode"),
            filter_mode=quality.get("filter_mode"),
            stationary_locked=bool(quality.get("stationary_locked", False)),
            sequence_number=payload.get("sequence_number"),
            received_monotonic=time.monotonic(),
        )

    def handle_target_select(self, payload: dict) -> None:
        node_id = str(payload.get("node_id") or "").strip()
        if not node_id:
            return
        self.selected_node = node_id
        self.centered_since_monotonic = None
        LOG.warning("target_select node=%s", node_id)

    def handle_vision_correction(self, payload: dict) -> None:
        target_px = payload.get("target_px") or {}
        node_id = str(payload.get("node_id") or "").strip()
        if not node_id:
            return
        self.vision_correction = VisionCorrection(
            node_id=node_id,
            timestamp=float(payload.get("timestamp", time.time())),
            x_px=float(target_px["x"]),
            y_px=float(target_px["y"]),
            confidence=float(payload.get("confidence", 0.0)),
            received_monotonic=time.monotonic(),
        )

    def handle_arm(self, payload: dict) -> None:
        if bool(payload.get("armed", False)) or bool(payload.get("confirm_recentered", False)):
            self.arm()
        elif payload.get("armed") is False:
            self.disarm("mqtt_command")

    def get_selected_pose(self, now: float) -> Optional[FilteredPoseMessage]:
        pose = self.poses.get(self.selected_node)
        if pose is None:
            return None
        age_s = now - pose.received_monotonic
        if age_s > self.config.control.target_stale_s:
            return None
        if pose.quality_score < self.config.control.min_quality:
            return None
        return pose

    def apply_vision_correction(
        self,
        node_id: str,
        projected: ProjectedPoint,
        now: float,
    ) -> Tuple[ProjectedPoint, str]:
        blend = clamp(self.config.control.vision_correction_blend, 0.0, 1.0)
        correction = self.vision_correction
        if blend <= 0.0 or correction is None or correction.node_id != node_id:
            return projected, "uwb_projection"
        if now - correction.received_monotonic > self.config.control.vision_correction_stale_s:
            return projected, "uwb_projection"
        if correction.confidence < self.config.control.vision_correction_min_confidence:
            return projected, "uwb_projection"
        return (
            ProjectedPoint(
                x_px=(projected.x_px * (1.0 - blend)) + correction.x_px * blend,
                y_px=(projected.y_px * (1.0 - blend)) + correction.y_px * blend,
                depth_m=projected.depth_m,
            ),
            "uwb_projection_cv_blend",
        )

    def target_image_center(self) -> Tuple[float, float]:
        return (
            self.config.control.target_image_x_px
            if self.config.control.target_image_x_px is not None
            else self.config.camera.image_w / 2.0,
            self.config.control.target_image_y_px
            if self.config.control.target_image_y_px is not None
            else self.config.camera.image_h / 2.0,
        )

    def _deadband(self, value: float, deadband: float) -> float:
        return 0.0 if abs(value) < deadband else value

    def _ramp(self, current: int, target: int, dt_s: float) -> int:
        step = max(self.config.motor.ramp_raw_per_s * max(dt_s, 0.0), 1.0)
        return int(round(clamp(float(target), current - step, current + step)))

    def _driver_commands(self, logical_pan: int, logical_truck: int) -> Tuple[int, int]:
        return (
            int(logical_pan * self.config.motor.pan_sign),
            int(logical_truck * self.config.motor.truck_sign),
        )

    def _read_statuses(self) -> Dict[int, MotorStatus]:
        statuses = {motor_id: self.motor_bus.read_status(motor_id) for motor_id in self.motor_ids}
        self.latest_statuses = statuses
        return statuses

    def _poll_pose_estimate(self, now: Optional[float] = None) -> Dict[int, MotorStatus]:
        if now is None:
            now = time.monotonic()
        statuses = self._read_statuses()
        if self.last_pose_poll_monotonic is None:
            dt_s = 0.0
        else:
            dt_s = clamp(now - self.last_pose_poll_monotonic, 0.0, 2.0)
        self.last_pose_poll_monotonic = now
        actual_pan_raw, actual_truck_raw = self._logical_actual_speeds(statuses)
        self.latest_actual_pan_raw = actual_pan_raw
        self.latest_actual_truck_raw = actual_truck_raw
        self.pose_estimator.update(dt_s, actual_pan_raw, actual_truck_raw)
        self._enforce_soft_limits_immediately()
        return statuses

    def _enforce_soft_limits_immediately(self) -> None:
        if self.config.motor.pan_enabled and self.logical_pan_command != 0 and self.pose_estimator.blocks_pan(self.logical_pan_command):
            self.motor_bus.stop(self.config.motor.pan_motor_id)
            if self.config.motor.debug:
                LOG.info(
                    "soft_limit_immediate axis=pan pose_pan=%.3f cmd_logical=%s cmd_driver=%s",
                    self.pose_estimator.pose.pan_deg,
                    self.logical_pan_command,
                    self.driver_pan_command,
                )
            self.logical_pan_command = 0
            self.driver_pan_command = 0
        if self.config.motor.truck_enabled and self.logical_truck_command != 0 and self.pose_estimator.blocks_truck(self.logical_truck_command):
            self.motor_bus.stop(self.config.motor.truck_motor_id)
            if self.config.motor.debug:
                LOG.info(
                    "soft_limit_immediate axis=truck pose_rail=%.3f cmd_logical=%s cmd_driver=%s",
                    self.pose_estimator.pose.rail_position_m,
                    self.logical_truck_command,
                    self.driver_truck_command,
                )
            self.logical_truck_command = 0
            self.driver_truck_command = 0

    @staticmethod
    def _driver_direction_from_status(status: MotorStatus, fallback_driver_command: int) -> int:
        if status.run_status == RUN_FORWARD:
            return 1
        if status.run_status == RUN_REVERSE:
            return -1
        if fallback_driver_command > 0:
            return 1
        if fallback_driver_command < 0:
            return -1
        return 0

    def _logical_actual_speeds(self, statuses: Dict[int, MotorStatus]) -> Tuple[float, float]:
        pan_raw = 0.0
        truck_raw = 0.0
        if self.config.motor.pan_enabled:
            status = statuses.get(self.config.motor.pan_motor_id)
            if status is not None:
                driver_direction = self._driver_direction_from_status(status, self.driver_pan_command)
                pan_raw = driver_direction * abs(int(status.actual_speed_raw or 0)) * self.config.motor.pan_sign
        if self.config.motor.truck_enabled:
            status = statuses.get(self.config.motor.truck_motor_id)
            if status is not None:
                driver_direction = self._driver_direction_from_status(status, self.driver_truck_command)
                truck_raw = driver_direction * abs(int(status.actual_speed_raw or 0)) * self.config.motor.truck_sign
        return pan_raw, truck_raw

    @staticmethod
    def _raise_faults(statuses: Dict[int, MotorStatus]) -> None:
        faults = [status for status in statuses.values() if status.has_fault]
        if faults:
            fault_text = ",".join(f"motor={status.motor_id}:fault={status.fault_code}" for status in faults)
            raise RuntimeError(f"Driver fault reported: {fault_text}")

    def _safe_envelope(self, projected: ProjectedPoint) -> bool:
        margin = self.config.control.safe_image_margin_px
        return (
            margin <= projected.x_px <= self.config.camera.image_w - margin
            and margin <= projected.y_px <= self.config.camera.image_h - margin
        )

    def build_command(self, target: FilteredPoseMessage, now: float, dt_s: float) -> AxisCommand:
        camera_pose = self.pose_estimator.pose
        projected = project_floor_point(
            WorldPoint(target.x_m, target.y_m, 0.0),
            camera_pose,
            self.config.camera,
        )
        projected, source = self.apply_vision_correction(target.node_id, projected, now)
        target_x, _target_y = self.target_image_center()
        error_x = projected.x_px - target_x
        safe_envelope = self._safe_envelope(projected)

        pan_enabled = self.config.motor.pan_enabled
        truck_enabled = self.config.motor.truck_enabled

        centered = abs(error_x) <= self.config.control.pan_centered_threshold_px
        if centered:
            if self.centered_since_monotonic is None:
                self.centered_since_monotonic = now
        else:
            self.centered_since_monotonic = None

        pan_raw_f = self._deadband(error_x, self.config.control.pan_image_deadband_px)
        pan_raw = int(round(clamp(
            pan_raw_f * self.config.control.pan_kp_raw_per_px,
            -self.config.motor.pan_max_raw_speed,
            self.config.motor.pan_max_raw_speed,
        )))

        alpha = clamp(self.config.control.truck_error_filter_alpha, 0.0, 1.0)
        # Reset the truck error filter when the target crosses sides so the truck can
        # promptly reverse away from a soft limit instead of dragging old error sign.
        if self.truck_error_filtered and error_x and (self.truck_error_filtered * error_x < 0.0):
            self.truck_error_filtered = error_x
        else:
            self.truck_error_filtered = (1.0 - alpha) * self.truck_error_filtered + alpha * error_x
        truck_error = self._deadband(self.truck_error_filtered, self.config.control.truck_image_deadband_px)
        truck_raw = int(round(clamp(
            truck_error * self.config.control.truck_kp_raw_per_px,
            -self.config.motor.truck_max_raw_speed,
            self.config.motor.truck_max_raw_speed,
        )))

        if truck_enabled and not pan_enabled:
            mode = "TRUCK_ONLY_TRACK"
            pan_raw = 0
            truck_blocked = self.pose_estimator.blocks_truck(truck_raw)
            if truck_blocked:
                truck_raw = 0
                mode += "_TRUCK_SOFT_LIMIT"
            if not truck_blocked:
                truck_raw = self._ramp(self.logical_truck_command, truck_raw, dt_s)
            return AxisCommand(
                pan_raw=0,
                truck_raw=truck_raw,
                mode=mode,
                projected=projected,
                error_x_px=error_x,
                source=source,
            )

        if pan_enabled and not truck_enabled:
            mode = "PAN_ONLY_TRACK"
            truck_raw = 0
            pan_blocked = self.pose_estimator.blocks_pan(pan_raw)
            if pan_blocked:
                pan_raw = 0
                mode += "_PAN_SOFT_LIMIT"
            if not pan_blocked:
                pan_raw = self._ramp(self.logical_pan_command, pan_raw, dt_s)
            return AxisCommand(
                pan_raw=pan_raw,
                truck_raw=0,
                mode=mode,
                projected=projected,
                error_x_px=error_x,
                source=source,
            )

        pan_pose = self.pose_estimator.pose.pan_deg
        pan_outside_preferred = (
            pan_pose < self.config.control.pan_preferred_yaw_min_deg
            or pan_pose > self.config.control.pan_preferred_yaw_max_deg
        )
        centered_delay_s = self.config.control.truck_enable_after_pan_centered_ms / 1000.0
        centered_long_enough = (
            self.centered_since_monotonic is not None
            and (now - self.centered_since_monotonic) >= centered_delay_s
        )

        if not safe_envelope:
            mode = "PAN_PRIORITY_SAFE_ENVELOPE"
            truck_raw = 0
        elif abs(error_x) > self.config.control.pan_prealign_threshold_px:
            mode = "PAN_PREALIGN_TRUCK_SUPPRESSED"
            truck_raw = 0
        elif centered_long_enough or pan_outside_preferred:
            mode = "PAN_TRIM_TRUCK_ASSIST"
        else:
            mode = "PAN_TRIM_WAITING_FOR_TRUCK"
            truck_raw = 0

        pan_blocked = self.pose_estimator.blocks_pan(pan_raw)
        if pan_blocked:
            pan_raw = 0
            mode += "_PAN_SOFT_LIMIT"
        truck_blocked = self.pose_estimator.blocks_truck(truck_raw)
        if truck_blocked:
            truck_raw = 0
            mode += "_TRUCK_SOFT_LIMIT"

        if not pan_blocked:
            pan_raw = self._ramp(self.logical_pan_command, pan_raw, dt_s)
        if not truck_blocked:
            truck_raw = self._ramp(self.logical_truck_command, truck_raw, dt_s)
        return AxisCommand(
            pan_raw=pan_raw,
            truck_raw=truck_raw,
            mode=mode,
            projected=projected,
            error_x_px=error_x,
            source=source,
        )

    def send_command(self, command: AxisCommand) -> None:
        pan_driver, truck_driver = self._driver_commands(command.pan_raw, command.truck_raw)
        if self.config.motor.pan_enabled:
            prev_pan_sign = (self.driver_pan_command > 0) - (self.driver_pan_command < 0)
            pan_sign = (pan_driver > 0) - (pan_driver < 0)
            self.motor_bus.set_speed(self.config.motor.pan_motor_id, pan_driver)
            if pan_driver != 0:
                if pan_sign != prev_pan_sign:
                    self.motor_bus.run(self.config.motor.pan_motor_id, direction=pan_sign)
            else:
                if prev_pan_sign != 0:
                    self.motor_bus.stop(self.config.motor.pan_motor_id)
        if self.config.motor.truck_enabled:
            prev_truck_sign = (self.driver_truck_command > 0) - (self.driver_truck_command < 0)
            truck_sign = (truck_driver > 0) - (truck_driver < 0)
            self.motor_bus.set_speed(self.config.motor.truck_motor_id, truck_driver)
            if truck_driver != 0:
                if truck_sign != prev_truck_sign:
                    self.motor_bus.run(self.config.motor.truck_motor_id, direction=truck_sign)
            else:
                if prev_truck_sign != 0:
                    self.motor_bus.stop(self.config.motor.truck_motor_id)
        self.logical_pan_command = command.pan_raw
        self.logical_truck_command = command.truck_raw
        self.driver_pan_command = pan_driver
        self.driver_truck_command = truck_driver

    def check_driver_faults(self, statuses: Optional[Dict[int, MotorStatus]] = None) -> None:
        if statuses is None:
            statuses = self._read_statuses()
        self._raise_faults(statuses)

    def log_tick(self, command: AxisCommand, target: Optional[FilteredPoseMessage], now: float) -> None:
        if now - self.last_log_monotonic < self.config.control.log_interval_s:
            return
        self.last_log_monotonic = now
        pose = self.pose_estimator.pose
        debug_enabled = self.config.motor.debug
        if target is None or command.projected is None:
            if debug_enabled:
                active_truck_min, active_truck_max = self.pose_estimator.active_truck_limits()
                LOG.info(
                    "tick armed=%s selected=%s target=none pose_est=rail=%.3f x=%.3f y=%.3f pan=%.2f "
                    "truck_limits_loaded=(%.3f,%.3f) truck_limits_active=(%.3f,%.3f) actual_logical=(%.1f,%.1f) cmd=0,0",
                    int(self.armed),
                    self.selected_node,
                    pose.rail_position_m,
                    pose.x_m,
                    pose.y_m,
                    pose.pan_deg,
                    self.config.motor.truck_min_rail_m,
                    self.config.motor.truck_max_rail_m,
                    active_truck_min,
                    active_truck_max,
                    self.latest_actual_pan_raw,
                    self.latest_actual_truck_raw,
                )
            else:
                active_truck_min, active_truck_max = self.pose_estimator.active_truck_limits()
                LOG.info(
                    "tick armed=%s selected=%s target=none rail=%.3f limits=(%.3f,%.3f) pan=%.2f cmd=0,0",
                    int(self.armed),
                    self.selected_node,
                    pose.rail_position_m,
                    active_truck_min,
                    active_truck_max,
                    pose.pan_deg,
                )
            return
        if debug_enabled:
            active_truck_min, active_truck_max = self.pose_estimator.active_truck_limits()
            LOG.info(
                "tick armed=%s selected=%s q=%.2f filt=(%.3f,%.3f) pose_est=(rail=%.3f,x=%.3f,y=%.3f,pan=%.2f) "
                "truck_limits_loaded=(%.3f,%.3f) truck_limits_active=(%.3f,%.3f) actual_logical=(%.1f,%.1f) "
                "px=(%.1f,%.1f) err_x=%.1f mode=%s source=%s cmd_logical=(%s,%s) cmd_driver=(%s,%s)",
                int(self.armed),
                target.node_id,
                target.quality_score,
                target.x_m,
                target.y_m,
                pose.rail_position_m,
                pose.x_m,
                pose.y_m,
                pose.pan_deg,
                self.config.motor.truck_min_rail_m,
                self.config.motor.truck_max_rail_m,
                active_truck_min,
                active_truck_max,
                self.latest_actual_pan_raw,
                self.latest_actual_truck_raw,
                command.projected.x_px,
                command.projected.y_px,
                command.error_x_px or 0.0,
                command.mode,
                command.source,
                command.pan_raw,
                command.truck_raw,
                self.driver_pan_command,
                self.driver_truck_command,
            )
        else:
            active_truck_min, active_truck_max = self.pose_estimator.active_truck_limits()
            LOG.info(
                "tick armed=%s selected=%s q=%.2f filt=(%.3f,%.3f) rail=%.3f limits=(%.3f,%.3f) pan=%.2f "
                "px=(%.1f,%.1f) err_x=%.1f mode=%s source=%s cmd_logical=(%s,%s) cmd_driver=(%s,%s)",
                int(self.armed),
                target.node_id,
                target.quality_score,
                target.x_m,
                target.y_m,
                pose.rail_position_m,
                active_truck_min,
                active_truck_max,
                pose.pan_deg,
                command.projected.x_px,
                command.projected.y_px,
                command.error_x_px or 0.0,
                command.mode,
                command.source,
                command.pan_raw,
                command.truck_raw,
                self.driver_pan_command,
                self.driver_truck_command,
            )

    def setup(self) -> None:
        self.motor_bus.connect()
        for motor_id in self.motor_ids:
            self.motor_bus.clear_fault(motor_id)
            self.motor_bus.configure_speed_mode(motor_id)
        self.stop_motors()
        self.client.connect(self.config.mqtt.host, self.config.mqtt.port, keepalive=30)
        self.client.loop_start()
        self.maybe_prompt_arm()

    def loop_once(self) -> None:
        now = time.monotonic()
        if self.last_loop_monotonic is None:
            dt_s = 1.0 / self.config.control.control_hz
        else:
            dt_s = clamp(now - self.last_loop_monotonic, 0.0, 0.25)
        self.last_loop_monotonic = now

        try:
            statuses = self._poll_pose_estimate(now)

            if not self.armed or not self.pose_estimator.valid or not self.mqtt_connected:
                self.stop_motors()
                self.log_tick(AxisCommand(0, 0, "UNARMED_INVALID_POSE", None, None, "none"), None, now)
                return

            target = self.get_selected_pose(now)
            if target is None:
                self.stop_motors()
                self.log_tick(AxisCommand(0, 0, "NO_VALID_FILTERED_TARGET", None, None, "none"), None, now)
                return

            self.check_driver_faults(statuses)
            command = self.build_command(target, now, dt_s)
            self.send_command(command)
            self.log_tick(command, target, now)
        except ProjectionError as exc:
            pose = self.pose_estimator.pose
            target = self.poses.get(self.selected_node)
            if target is None:
                LOG.warning(
                    "projection_rejected selected=%s reason=%s pose_est=(rail=%.3f,x=%.3f,y=%.3f,pan=%.2f)",
                    self.selected_node,
                    exc,
                    pose.rail_position_m,
                    pose.x_m,
                    pose.y_m,
                    pose.pan_deg,
                )
            else:
                LOG.warning(
                    "projection_rejected selected=%s reason=%s target=(%.3f,%.3f) "
                    "pose_est=(rail=%.3f,x=%.3f,y=%.3f,pan=%.2f)",
                    self.selected_node,
                    exc,
                    target.x_m,
                    target.y_m,
                    pose.rail_position_m,
                    pose.x_m,
                    pose.y_m,
                    pose.pan_deg,
                )
            self.stop_motors()
        except Exception:
            LOG.exception("control_loop_fault_stopping_motors")
            self.disarm("control_fault")
            raise

    def run(self) -> None:
        tick_s = 1.0 / self.config.control.control_hz
        try:
            self.setup()
            while self.running:
                tick_started = time.monotonic()
                self.loop_once()
                deadline = tick_started + tick_s
                while self.running:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        break
                    time.sleep(min(self.config.control.status_poll_s, remaining))
                    self._poll_pose_estimate()
        except KeyboardInterrupt:
            LOG.warning("shutdown=keyboard_interrupt")
        finally:
            self.running = False
            try:
                self.stop_motors()
            except Exception:
                LOG.exception("shutdown_stop_failed")
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                LOG.exception("shutdown_mqtt_close_failed")
            self.motor_bus.close()
            LOG.warning("motor_controller=stopped")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> None:
    setup_logging()
    config = load_runtime_config()
    LOG.warning(
        "motor_controller=start live=%s debug=%s selected=%s camera=%s %sx%s pan_enabled=%s truck_enabled=%s "
        "pan_motor=%s truck_motor=%s rail_origin=(%.3f,%.3f) rail_heading_deg=%.2f start_rail_m=%.3f "
        "rail_limits=(%.3f,%.3f) rail_soft_margin=%.3f control_hz=%.1f status_poll_s=%.3f log_interval_s=%.3f",
        int(config.motor.enable_live),
        int(config.motor.debug),
        config.mqtt.target_node,
        config.camera.profile,
        config.camera.image_w,
        config.camera.image_h,
        int(config.motor.pan_enabled),
        int(config.motor.truck_enabled),
        config.motor.pan_motor_id,
        config.motor.truck_motor_id,
        config.camera_pose.rail_origin_x_m,
        config.camera_pose.rail_origin_y_m,
        config.camera_pose.rail_heading_deg,
        config.camera_pose.start_rail_m,
        config.motor.truck_min_rail_m,
        config.motor.truck_max_rail_m,
        config.motor.truck_soft_limit_margin_m,
        config.control.control_hz,
        config.control.status_poll_s,
        config.control.log_interval_s,
    )
    app = MotorControllerApp(config)

    def _handle_signal(signum, frame):
        LOG.warning("shutdown=signal signum=%s", signum)
        app.running = False

    signal.signal(signal.SIGTERM, _handle_signal)
    if hasattr(signal, "SIGINT"):
        signal.signal(signal.SIGINT, _handle_signal)
    app.run()


if __name__ == "__main__":
    main()




