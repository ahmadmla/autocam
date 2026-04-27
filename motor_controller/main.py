"""Dynamic homography motor-control entrypoint for pan/truck autocam."""

from __future__ import annotations

import json
import logging
import math
import signal
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    mqtt = None

from .config import RuntimeConfig, load_runtime_config
from .control import MixedMotorBus, MotorStatus, RUN_FORWARD, RUN_REVERSE
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
    tracking_x_m: Optional[float]
    tracking_y_m: Optional[float]
    quality_score: float
    pose_mode: Optional[str]
    filter_mode: Optional[str]
    stationary_confidence: Optional[float]
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
    desired_bearing_deg: Optional[float] = None
    pan_error_deg: Optional[float] = None
    bearing_rate_deg_s: Optional[float] = None


@dataclass
class StationaryHoldLock:
    node_id: str
    x_m: float
    y_m: float
    rail_m: float
    pan_deg: float
    entered_monotonic: float
    last_distance_m: float = 0.0
    last_error_x_px: float = 0.0


class MotorControllerApp:
    def __init__(self, config: RuntimeConfig) -> None:
        if mqtt is None:
            raise RuntimeError("paho-mqtt is not installed. Install requirements.txt first.")
        self.config = config
        if not (config.motor.pan_enabled or config.motor.truck_enabled):
            raise RuntimeError("At least one motor axis must be enabled.")
        self.pose_estimator = CameraPoseEstimator(config.camera_pose, config.motor)
        self.motor_bus = MixedMotorBus(
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
        self.pan_error_filtered: Optional[float] = None
        self.pan_last_direction = 0
        self.pan_flip_pending_direction = 0
        self.pan_flip_pending_since_monotonic: Optional[float] = None
        self.pan_center_hold_until_monotonic: Optional[float] = None
        self.pan_last_desired_bearing_deg: Optional[float] = None
        self.pan_last_bearing_monotonic: Optional[float] = None
        self.pan_velocity_filtered: Optional[float] = None
        self.truck_error_filtered = 0.0
        self.stationary_hold_lock: Optional[StationaryHoldLock] = None
        self.logical_pan_command = 0
        self.logical_truck_command = 0
        self.driver_pan_command = 0
        self.driver_truck_command = 0
        self.latest_actual_pan_raw = 0.0
        self.latest_actual_truck_raw = 0.0
        self.latest_statuses: Dict[int, MotorStatus] = {}
        self._motor_lock = threading.RLock()
        self._safety_stop_event = threading.Event()
        self._safety_thread: Optional[threading.Thread] = None

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
        with self._motor_lock:
            self.logical_pan_command = 0
            self.logical_truck_command = 0
            self.driver_pan_command = 0
            self.driver_truck_command = 0
            self.latest_statuses = {}
            self.motor_bus.stop_all(self.motor_ids)

    def arm(self) -> None:
        pose = self.pose_estimator.reset_to_start()
        self.armed = True
        self.centered_since_monotonic = None
        self._reset_pan_control_state()
        self.truck_error_filtered = 0.0
        self._clear_stationary_hold("arm")
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
        self._reset_pan_control_state()
        self._clear_stationary_hold("disarm")
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
            self._clear_stationary_hold("mqtt_disconnect")
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
        # Motor control uses the final pose for framing and the live tracker pose for hold-unlock distance.
        pose = payload.get("pose") or {}
        tracking_pose = payload.get("tracking_pose") or {}
        quality = payload.get("quality") or {}
        node_id = str(payload.get("node_id") or "")
        if not node_id:
            return
        self.poses[node_id] = FilteredPoseMessage(
            node_id=node_id,
            timestamp=float(payload.get("timestamp", time.time())),
            x_m=float(pose["x_m"]),
            y_m=float(pose["y_m"]),
            tracking_x_m=_optional_float(tracking_pose.get("x_m")),
            tracking_y_m=_optional_float(tracking_pose.get("y_m")),
            quality_score=float(quality.get("score", 0.0)),
            pose_mode=quality.get("pose_mode"),
            filter_mode=quality.get("filter_mode"),
            stationary_confidence=_optional_float(quality.get("stationary_confidence")),
            stationary_locked=bool(quality.get("stationary_locked", False)),
            sequence_number=payload.get("sequence_number"),
            received_monotonic=time.monotonic(),
        )

    def handle_target_select(self, payload: dict) -> None:
        node_id = str(payload.get("node_id") or "").strip()
        if not node_id:
            return
        if node_id == self.selected_node:
            return
        self.selected_node = node_id
        self.centered_since_monotonic = None
        self._reset_pan_control_state()
        self._clear_stationary_hold("target_select")
        LOG.warning("target_select node=%s source=%s", node_id, payload.get("source") or "manual")

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

    def _ramp_axis(self, axis_name: str, current: int, target: int, dt_s: float) -> int:
        if axis_name == "pan":
            ramp_raw_per_s = self.config.motor.pan_ramp_raw_per_s
        elif axis_name == "truck":
            ramp_raw_per_s = self.config.motor.truck_ramp_raw_per_s
        else:
            ramp_raw_per_s = self.config.motor.ramp_raw_per_s
        step = max(float(ramp_raw_per_s) * max(dt_s, 0.0), 1.0)
        return int(round(clamp(float(target), current - step, current + step)))

    @staticmethod
    def _ramp_with_rate(current: int, target: int, ramp_raw_per_s: float, dt_s: float) -> int:
        if current and target and ((current > 0) != (target > 0)):
            target = 0
        step = max(float(ramp_raw_per_s) * max(dt_s, 0.0), 1.0)
        return int(round(clamp(float(target), current - step, current + step)))

    @staticmethod
    def _sign(value: int | float) -> int:
        if value > 0:
            return 1
        if value < 0:
            return -1
        return 0

    def _reset_pan_control_state(self) -> None:
        self.pan_error_filtered = None
        self.pan_last_direction = 0
        self.pan_flip_pending_direction = 0
        self.pan_flip_pending_since_monotonic = None
        self.pan_center_hold_until_monotonic = None
        self.pan_last_desired_bearing_deg = None
        self.pan_last_bearing_monotonic = None
        self.pan_velocity_filtered = None

    def _reset_tracking_error_state(self) -> None:
        self._reset_pan_control_state()
        self.truck_error_filtered = 0.0

    def _clear_stationary_hold(self, reason: str) -> None:
        if self.stationary_hold_lock is not None:
            LOG.info("stationary_hold=clear reason=%s", reason)
        self.stationary_hold_lock = None

    def _stationary_hold_distance_m(self, target: FilteredPoseMessage) -> Optional[float]:
        lock = self.stationary_hold_lock
        if lock is None or lock.node_id != target.node_id:
            return None
        target_x = target.tracking_x_m if target.tracking_x_m is not None else target.x_m
        target_y = target.tracking_y_m if target.tracking_y_m is not None else target.y_m
        return math.hypot(target_x - lock.x_m, target_y - lock.y_m)

    def _target_stationary_for_hold(self, target: FilteredPoseMessage) -> bool:
        return target.stationary_locked

    def _enter_stationary_hold(self, target: FilteredPoseMessage, error_x_px: float, now: float) -> None:
        pose = self.pose_estimator.pose
        self.stationary_hold_lock = StationaryHoldLock(
            node_id=target.node_id,
            x_m=target.x_m,
            y_m=target.y_m,
            rail_m=pose.rail_position_m,
            pan_deg=pose.pan_deg,
            entered_monotonic=now,
            last_error_x_px=error_x_px,
        )
        self._reset_tracking_error_state()
        LOG.info(
            "stationary_hold=enter node=%s pose=(%.3f,%.3f) rail=%.3f pan=%.2f err_x=%.1f q=%.2f stat=%s",
            target.node_id,
            target.x_m,
            target.y_m,
            pose.rail_position_m,
            pose.pan_deg,
            error_x_px,
            target.quality_score,
            _format_optional_float(target.stationary_confidence),
        )

    def _stationary_hold_active(self, target: FilteredPoseMessage, error_x_px: float, now: float) -> bool:
        if not self.config.control.stationary_hold_enabled:
            self._clear_stationary_hold("disabled")
            return False

        lock = self.stationary_hold_lock
        if lock is not None:
            distance_m = self._stationary_hold_distance_m(target)
            lock.last_error_x_px = error_x_px
            lock.last_distance_m = distance_m if distance_m is not None else 0.0
            if lock.node_id != target.node_id:
                self._clear_stationary_hold("node_changed")
                return False
            if not self._target_stationary_for_hold(target):
                self._clear_stationary_hold("stationary_unlocked")
                return False
            if target.quality_score < self.config.control.stationary_hold_min_quality:
                self._clear_stationary_hold("low_quality")
                return False
            if distance_m is None or distance_m > self.config.control.stationary_hold_room_radius_m:
                self._clear_stationary_hold("room_motion")
                return False
            if abs(error_x_px) > self.config.control.stationary_hold_escape_window_px:
                self._clear_stationary_hold("image_escape")
                return False
            return True

        can_acquire = (
            self._target_stationary_for_hold(target)
            and target.quality_score >= self.config.control.stationary_hold_min_quality
            and abs(error_x_px) <= self.config.control.stationary_hold_pan_window_px
        )
        if can_acquire:
            self._enter_stationary_hold(target, error_x_px, now)
            return True
        return False

    def _filter_pan_error(self, error_x_px: float, dt_s: float) -> float:
        if self.pan_error_filtered is None or not math.isfinite(self.pan_error_filtered):
            self.pan_error_filtered = error_x_px
            return self.pan_error_filtered

        tau_s = max(0.0, self.config.control.pan_error_filter_tau_s)
        if tau_s > 0.0:
            alpha = 1.0 - math.exp(-max(0.0, dt_s) / tau_s)
        else:
            alpha = clamp(self.config.control.pan_error_filter_alpha, 0.0, 1.0)
        self.pan_error_filtered = (1.0 - alpha) * self.pan_error_filtered + alpha * error_x_px
        return self.pan_error_filtered

    @staticmethod
    def _normalize_angle_deg(angle_deg: float) -> float:
        return (angle_deg + 180.0) % 360.0 - 180.0

    @staticmethod
    def _desired_bearing_deg(camera_pose, target: FilteredPoseMessage) -> float:
        dx = target.x_m - camera_pose.x_m
        dy = target.y_m - camera_pose.y_m
        if abs(dx) < 1e-9 and abs(dy) < 1e-9:
            return camera_pose.pan_deg
        return math.degrees(math.atan2(dx, dy))

    def _target_image_x_angle_offset_deg(self) -> float:
        target_x, _target_y = self.target_image_center()
        fx = max(abs(self.config.camera.fx), 1e-9)
        return math.degrees(math.atan((target_x - self.config.camera.cx) / fx))

    def _desired_pan_deg_for_framing(self, camera_pose, target: FilteredPoseMessage) -> Tuple[float, float]:
        desired_bearing_deg = self._desired_bearing_deg(camera_pose, target)
        return desired_bearing_deg, desired_bearing_deg - self._target_image_x_angle_offset_deg()

    def _bearing_velocity_pan_command(
        self,
        target: FilteredPoseMessage,
        camera_pose,
        now: float,
        dt_s: float,
    ) -> Tuple[int, str, float, float, float]:
        desired_bearing_deg, desired_pan_deg = self._desired_pan_deg_for_framing(camera_pose, target)
        pan_error_deg = self._normalize_angle_deg(desired_pan_deg - camera_pose.pan_deg)
        bearing_rate_deg_s = 0.0
        if self.pan_last_desired_bearing_deg is not None and self.pan_last_bearing_monotonic is not None:
            bearing_dt_s = clamp(now - self.pan_last_bearing_monotonic, 0.0, 1.0)
            if bearing_dt_s > 1e-6:
                bearing_delta_deg = self._normalize_angle_deg(desired_bearing_deg - self.pan_last_desired_bearing_deg)
                bearing_rate_deg_s = bearing_delta_deg / bearing_dt_s
        self.pan_last_desired_bearing_deg = desired_bearing_deg
        self.pan_last_bearing_monotonic = now

        deadband_deg = max(0.0, self.config.control.pan_bearing_moving_deadband_deg)
        if abs(pan_error_deg) <= deadband_deg:
            effective_error_deg = 0.0
        else:
            effective_error_deg = math.copysign(abs(pan_error_deg) - deadband_deg, pan_error_deg)
        raw_target = (
            self.config.control.pan_bearing_kp_raw_per_deg * effective_error_deg
            + self.config.control.pan_bearing_kd_raw_per_deg_s * bearing_rate_deg_s
        )
        raw_target = clamp(
            raw_target,
            -self.config.motor.pan_max_raw_speed,
            self.config.motor.pan_max_raw_speed,
        )

        tau_s = max(0.0, self.config.control.pan_velocity_filter_tau_s)
        if self.pan_velocity_filtered is None or not math.isfinite(self.pan_velocity_filtered):
            self.pan_velocity_filtered = raw_target
        elif tau_s > 0.0:
            alpha = 1.0 - math.exp(-max(0.0, dt_s) / tau_s)
            self.pan_velocity_filtered = (1.0 - alpha) * self.pan_velocity_filtered + alpha * raw_target
        else:
            self.pan_velocity_filtered = raw_target

        filtered_target = int(round(clamp(
            self.pan_velocity_filtered,
            -self.config.motor.pan_max_raw_speed,
            self.config.motor.pan_max_raw_speed,
        )))
        pan_raw = self._ramp_with_rate(
            self.logical_pan_command,
            filtered_target,
            self.config.control.pan_command_ramp_raw_per_s,
            dt_s,
        )
        return pan_raw, "_BEARING_VELOCITY", desired_bearing_deg, pan_error_deg, bearing_rate_deg_s

    def _bearing_goto_pan_command(
        self,
        target: FilteredPoseMessage,
        camera_pose,
        now: float,
        dt_s: float,
    ) -> Tuple[int, str, float, float, float]:
        desired_bearing_deg, desired_pan_deg = self._desired_pan_deg_for_framing(camera_pose, target)
        pan_error_deg = self._normalize_angle_deg(desired_pan_deg - camera_pose.pan_deg)
        bearing_rate_deg_s = 0.0
        if self.pan_last_desired_bearing_deg is not None and self.pan_last_bearing_monotonic is not None:
            bearing_dt_s = clamp(now - self.pan_last_bearing_monotonic, 0.0, 1.0)
            if bearing_dt_s > 1e-6:
                bearing_delta_deg = self._normalize_angle_deg(desired_bearing_deg - self.pan_last_desired_bearing_deg)
                bearing_rate_deg_s = bearing_delta_deg / bearing_dt_s
        self.pan_last_desired_bearing_deg = desired_bearing_deg
        self.pan_last_bearing_monotonic = now

        tolerance_deg = max(0.0, self.config.control.pan_bearing_deadband_deg)
        effective_error_deg = max(0.0, abs(pan_error_deg) - tolerance_deg)
        if effective_error_deg <= 0.0:
            raw_target = 0.0
        else:
            raw_abs = (
                self.config.control.pan_goto_speed_factor
                * effective_error_deg
                / max(
                    self.config.motor.pan_deg_per_raw_speed_s
                    * self.config.control.pan_goto_target_time_s,
                    1e-9,
                )
            )
            raw_abs = max(float(self.config.control.pan_goto_min_raw_speed), raw_abs)
            raw_target = math.copysign(
                min(raw_abs, float(self.config.motor.pan_max_raw_speed)),
                pan_error_deg,
            )

        pan_raw = self._ramp_with_rate(
            self.logical_pan_command,
            int(round(raw_target)),
            self.config.control.pan_command_ramp_raw_per_s,
            dt_s,
        )
        return pan_raw, "_BEARING_GOTO", desired_bearing_deg, pan_error_deg, bearing_rate_deg_s

    def _apply_pan_direction_hysteresis(self, desired_pan_raw: int, now: float) -> Tuple[int, str]:
        desired_direction = self._sign(desired_pan_raw)
        active_direction = self._sign(self.logical_pan_command) or self.pan_last_direction

        if desired_direction == 0:
            self.pan_flip_pending_direction = 0
            self.pan_flip_pending_since_monotonic = None
            center_hold_s = max(0.0, self.config.control.pan_center_hold_ms) / 1000.0
            if center_hold_s > 0.0:
                self.pan_center_hold_until_monotonic = now + center_hold_s
            return 0, ""

        if (
            self.pan_center_hold_until_monotonic is not None
            and now < self.pan_center_hold_until_monotonic
        ):
            return 0, "_PAN_CENTER_HOLD"
        self.pan_center_hold_until_monotonic = None

        flip_hold_s = max(0.0, self.config.control.pan_direction_flip_hold_ms) / 1000.0
        if active_direction and desired_direction != active_direction and flip_hold_s > 0.0:
            if self.pan_flip_pending_direction != desired_direction:
                self.pan_flip_pending_direction = desired_direction
                self.pan_flip_pending_since_monotonic = now
            elapsed_s = now - (self.pan_flip_pending_since_monotonic or now)
            if elapsed_s < flip_hold_s:
                return 0, "_PAN_FLIP_HOLD"

        self.pan_flip_pending_direction = 0
        self.pan_flip_pending_since_monotonic = None
        self.pan_last_direction = desired_direction
        return desired_pan_raw, ""

    def _driver_commands(self, logical_pan: int, logical_truck: int) -> Tuple[int, int]:
        return (
            int(logical_pan * self.config.motor.pan_sign),
            int(logical_truck * self.config.motor.truck_sign),
        )

    def _read_statuses(self) -> Dict[int, MotorStatus]:
        with self._motor_lock:
            statuses = {motor_id: self.motor_bus.read_status(motor_id) for motor_id in self.motor_ids}
            self.latest_statuses = statuses
        return statuses

    def _poll_pose_estimate(self, now: Optional[float] = None) -> Dict[int, MotorStatus]:
        if now is None:
            now = time.monotonic()
        statuses = self._read_statuses()
        with self._motor_lock:
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

    def _safety_loop(self) -> None:
        poll_s = max(0.001, float(self.config.control.status_poll_s))
        LOG.info("safety_loop=start poll_s=%.3f", poll_s)
        while not self._safety_stop_event.is_set():
            try:
                if self.armed and self.pose_estimator.valid:
                    self._poll_pose_estimate()
            except Exception:
                LOG.exception("safety_loop_fault")
                try:
                    self.disarm("safety_loop_fault")
                except Exception:
                    LOG.exception("safety_loop_disarm_failed")
            self._safety_stop_event.wait(poll_s)
        LOG.info("safety_loop=stop")

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
        if self.config.motor.truck_enabled and self.logical_truck_command != 0:
            horizon_s = max(0.0, self.config.motor.truck_stop_horizon_s)
            velocity_m_s = self.latest_actual_truck_raw * self.config.motor.truck_m_per_raw_speed_s
            predicted_rail = self.pose_estimator.pose.rail_position_m + velocity_m_s * horizon_s
            truck_min_limit, truck_max_limit = self.pose_estimator.active_truck_limits()
            predicted_breach = (
                (predicted_rail >= truck_max_limit and self.logical_truck_command > 0)
                or (predicted_rail <= truck_min_limit and self.logical_truck_command < 0)
            )
            if predicted_breach or self.pose_estimator.blocks_truck(self.logical_truck_command):
                self.motor_bus.stop(self.config.motor.truck_motor_id)
                if self.config.motor.debug:
                    LOG.info(
                        "soft_limit_immediate axis=truck pose_rail=%.3f predicted_rail=%.3f vel_m_s=%.3f cmd_logical=%s cmd_driver=%s",
                        self.pose_estimator.pose.rail_position_m,
                        predicted_rail,
                        velocity_m_s,
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

    def _safe_horizontal_envelope(self, projected: ProjectedPoint) -> bool:
        margin = self.config.control.safe_image_margin_px
        return margin <= projected.x_px <= self.config.camera.image_w - margin

    def build_command(self, target: FilteredPoseMessage, now: float, dt_s: float) -> AxisCommand:
        camera_pose = self.pose_estimator.pose
        projected: Optional[ProjectedPoint]
        error_x: Optional[float]
        source = "uwb_projection"
        projection_valid = True
        desired_bearing_deg, desired_pan_deg = self._desired_pan_deg_for_framing(camera_pose, target)
        pan_error_deg = self._normalize_angle_deg(desired_pan_deg - camera_pose.pan_deg)
        try:
            projected = project_floor_point(
                WorldPoint(target.x_m, target.y_m, 0.0),
                camera_pose,
                self.config.camera,
            )
            projected, source = self.apply_vision_correction(target.node_id, projected, now)
            target_x, _target_y = self.target_image_center()
            error_x = projected.x_px - target_x
        except ProjectionError:
            if self.config.control.pan_control_mode not in {"BEARING_VELOCITY", "BEARING_GOTO"}:
                raise
            projected = None
            error_x = None
            projection_valid = False
            source = "bearing_only_projection_invalid"
            self._clear_stationary_hold("projection_invalid")

        stationary_hold_active = (
            projection_valid
            and error_x is not None
            and self._stationary_hold_active(target, error_x, now)
        )
        if stationary_hold_active:
            return AxisCommand(
                pan_raw=0,
                truck_raw=0,
                mode="STATIONARY_HOLD",
                projected=projected,
                error_x_px=error_x,
                source=source,
                desired_bearing_deg=desired_bearing_deg,
                pan_error_deg=pan_error_deg,
                bearing_rate_deg_s=0.0,
            )

        safe_horizontal_envelope = bool(projected is not None and self._safe_horizontal_envelope(projected))

        pan_enabled = self.config.motor.pan_enabled
        truck_enabled = self.config.motor.truck_enabled
        pan_already_ramped = False
        bearing_rate_deg_s: Optional[float] = None

        if self.config.control.pan_control_mode == "BEARING_GOTO":
            filtered_error_x = error_x or 0.0
            (
                pan_raw,
                pan_hold_mode,
                desired_bearing_deg,
                pan_error_deg,
                bearing_rate_deg_s,
            ) = self._bearing_goto_pan_command(target, camera_pose, now, dt_s)
            pan_already_ramped = True
        elif self.config.control.pan_control_mode == "BEARING_VELOCITY":
            filtered_error_x = error_x or 0.0
            (
                pan_raw,
                pan_hold_mode,
                desired_bearing_deg,
                pan_error_deg,
                bearing_rate_deg_s,
            ) = self._bearing_velocity_pan_command(target, camera_pose, now, dt_s)
            pan_already_ramped = True
        else:
            if error_x is None:
                raise ProjectionError("Projection unavailable for pixel pan control")
            filtered_error_x = self._filter_pan_error(error_x, dt_s)
            pan_raw_f = self._deadband(filtered_error_x, self.config.control.pan_image_deadband_px)
            pan_raw = int(round(clamp(
                pan_raw_f * self.config.control.pan_kp_raw_per_px,
                -self.config.motor.pan_max_raw_speed,
                self.config.motor.pan_max_raw_speed,
            )))
            pan_raw, pan_hold_mode = self._apply_pan_direction_hysteresis(pan_raw, now)

        if not projection_valid:
            mode = "PAN_BEARING_RECOVERY" + pan_hold_mode
            if not pan_enabled:
                pan_raw = 0
            pan_blocked = self.pose_estimator.blocks_pan(pan_raw)
            if pan_blocked:
                pan_raw = 0
                mode += "_PAN_SOFT_LIMIT"
            return AxisCommand(
                pan_raw=pan_raw,
                truck_raw=0,
                mode=mode,
                projected=None,
                error_x_px=None,
                source=source,
                desired_bearing_deg=desired_bearing_deg,
                pan_error_deg=pan_error_deg,
                bearing_rate_deg_s=bearing_rate_deg_s,
            )

        centered = abs(filtered_error_x) <= self.config.control.pan_centered_threshold_px
        if centered:
            if self.centered_since_monotonic is None:
                self.centered_since_monotonic = now
        else:
            self.centered_since_monotonic = None

        tau_s = max(0.0, self.config.control.truck_error_filter_tau_s)
        if tau_s > 0.0:
            alpha = 1.0 - math.exp(-max(0.0, dt_s) / tau_s)
        else:
            alpha = clamp(self.config.control.truck_error_filter_alpha, 0.0, 1.0)
        # Snap the filter to the raw error when the target crosses sides or when the
        # error magnitude has jumped far from the filtered value, so the truck reacts
        # promptly to targets appearing at new positions instead of creeping up via EWMA.
        snap_threshold_px = max(
            self.config.control.truck_image_deadband_px,
            abs(self.truck_error_filtered) * 0.5,
        )
        sign_flipped = bool(self.truck_error_filtered) and bool(error_x) and (self.truck_error_filtered * error_x < 0.0)
        large_jump = abs(error_x - self.truck_error_filtered) > snap_threshold_px
        if sign_flipped or large_jump:
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
                truck_raw = self._ramp_axis("truck", self.logical_truck_command, truck_raw, dt_s)
            return AxisCommand(
                pan_raw=0,
                truck_raw=truck_raw,
                mode=mode,
                projected=projected,
                error_x_px=error_x,
                source=source,
                desired_bearing_deg=desired_bearing_deg,
                pan_error_deg=pan_error_deg,
                bearing_rate_deg_s=bearing_rate_deg_s,
            )

        if pan_enabled and not truck_enabled:
            mode = "PAN_ONLY_TRACK" + pan_hold_mode
            truck_raw = 0
            pan_blocked = self.pose_estimator.blocks_pan(pan_raw)
            if pan_blocked:
                pan_raw = 0
                mode += "_PAN_SOFT_LIMIT"
            if not pan_blocked and not pan_already_ramped:
                pan_raw = self._ramp_axis("pan", self.logical_pan_command, pan_raw, dt_s)
            return AxisCommand(
                pan_raw=pan_raw,
                truck_raw=0,
                mode=mode,
                projected=projected,
                error_x_px=error_x,
                source=source,
                desired_bearing_deg=desired_bearing_deg,
                pan_error_deg=pan_error_deg,
                bearing_rate_deg_s=bearing_rate_deg_s,
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

        if not safe_horizontal_envelope:
            mode = "PAN_PRIORITY_SAFE_ENVELOPE"
            truck_raw = 0
        elif abs(filtered_error_x) > self.config.control.pan_prealign_threshold_px:
            mode = "PAN_PREALIGN_TRUCK_SUPPRESSED"
            truck_raw = 0
        elif centered_long_enough or pan_outside_preferred:
            mode = "PAN_TRIM_TRUCK_ASSIST"
        else:
            mode = "PAN_TRIM_WAITING_FOR_TRUCK"
            truck_raw = 0

        pan_blocked = self.pose_estimator.blocks_pan(pan_raw)
        mode += pan_hold_mode
        if pan_blocked:
            pan_raw = 0
            mode += "_PAN_SOFT_LIMIT"
        truck_blocked = self.pose_estimator.blocks_truck(truck_raw)
        if truck_blocked:
            truck_raw = 0
            mode += "_TRUCK_SOFT_LIMIT"

        if not pan_blocked and not pan_already_ramped:
            pan_raw = self._ramp_axis("pan", self.logical_pan_command, pan_raw, dt_s)
        if not truck_blocked:
            truck_raw = self._ramp_axis("truck", self.logical_truck_command, truck_raw, dt_s)
        return AxisCommand(
            pan_raw=pan_raw,
            truck_raw=truck_raw,
            mode=mode,
            projected=projected,
            error_x_px=error_x,
            source=source,
            desired_bearing_deg=desired_bearing_deg,
            pan_error_deg=pan_error_deg,
            bearing_rate_deg_s=bearing_rate_deg_s,
        )

    def send_command(self, command: AxisCommand) -> None:
        pan_driver, truck_driver = self._driver_commands(command.pan_raw, command.truck_raw)
        with self._motor_lock:
            if self.config.motor.pan_enabled:
                prev_pan_sign = (self.driver_pan_command > 0) - (self.driver_pan_command < 0)
                pan_sign = (pan_driver > 0) - (pan_driver < 0)
                self.motor_bus.set_speed(self.config.motor.pan_motor_id, pan_driver)
                if pan_driver != 0:
                    if pan_sign != prev_pan_sign:
                        if prev_pan_sign != 0:
                            self.motor_bus.stop(self.config.motor.pan_motor_id)
                            self.motor_bus.set_speed(self.config.motor.pan_motor_id, pan_driver)
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
                        if prev_truck_sign != 0:
                            self.motor_bus.stop(self.config.motor.truck_motor_id)
                            self.motor_bus.set_speed(self.config.motor.truck_motor_id, truck_driver)
                        self.motor_bus.run(self.config.motor.truck_motor_id, direction=truck_sign)
                else:
                    if prev_truck_sign != 0:
                        self.motor_bus.stop(self.config.motor.truck_motor_id)
            self.logical_pan_command = command.pan_raw
            self.logical_truck_command = command.truck_raw
            self.driver_pan_command = pan_driver
            self.driver_truck_command = truck_driver

    def log_tick(self, command: AxisCommand, target: Optional[FilteredPoseMessage], now: float) -> None:
        if now - self.last_log_monotonic < self.config.control.log_interval_s:
            return
        self.last_log_monotonic = now
        pose = self.pose_estimator.pose
        debug_enabled = self.config.motor.debug
        if target is None:
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
        if command.projected is None:
            active_truck_min, active_truck_max = self.pose_estimator.active_truck_limits()
            stationary_confidence_log = (
                target.stationary_confidence if target.stationary_confidence is not None else -1.0
            )
            desired_bearing_log = command.desired_bearing_deg if command.desired_bearing_deg is not None else -999.0
            pan_error_deg_log = command.pan_error_deg if command.pan_error_deg is not None else -999.0
            bearing_rate_log = command.bearing_rate_deg_s if command.bearing_rate_deg_s is not None else 0.0
            if debug_enabled:
                LOG.info(
                    "tick armed=%s selected=%s q=%.2f stat=%.2f filt=(%.3f,%.3f) pose_est=(rail=%.3f,x=%.3f,y=%.3f,pan=%.2f) "
                    "truck_limits_loaded=(%.3f,%.3f) truck_limits_active=(%.3f,%.3f) actual_logical=(%.1f,%.1f) "
                    "px=invalid bearing=%.2f pan_err_deg=%.2f bearing_rate=%.2f stationary_locked=%s mode=%s source=%s "
                    "cmd_logical=(%s,%s) cmd_driver=(%s,%s)",
                    int(self.armed),
                    target.node_id,
                    target.quality_score,
                    stationary_confidence_log,
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
                    desired_bearing_log,
                    pan_error_deg_log,
                    bearing_rate_log,
                    int(target.stationary_locked),
                    command.mode,
                    command.source,
                    command.pan_raw,
                    command.truck_raw,
                    self.driver_pan_command,
                    self.driver_truck_command,
                )
            else:
                LOG.info(
                    "tick armed=%s selected=%s q=%.2f stat=%.2f filt=(%.3f,%.3f) rail=%.3f limits=(%.3f,%.3f) pan=%.2f "
                    "px=invalid bearing=%.2f pan_err_deg=%.2f bearing_rate=%.2f stationary_locked=%s mode=%s source=%s "
                    "cmd_logical=(%s,%s) cmd_driver=(%s,%s)",
                    int(self.armed),
                    target.node_id,
                    target.quality_score,
                    stationary_confidence_log,
                    target.x_m,
                    target.y_m,
                    pose.rail_position_m,
                    active_truck_min,
                    active_truck_max,
                    pose.pan_deg,
                    desired_bearing_log,
                    pan_error_deg_log,
                    bearing_rate_log,
                    int(target.stationary_locked),
                    command.mode,
                    command.source,
                    command.pan_raw,
                    command.truck_raw,
                    self.driver_pan_command,
                    self.driver_truck_command,
                )
            return
        hold_distance_m = self._stationary_hold_distance_m(target)
        hold_distance_log = hold_distance_m if hold_distance_m is not None else -1.0
        stationary_confidence_log = (
            target.stationary_confidence if target.stationary_confidence is not None else -1.0
        )
        desired_bearing_log = command.desired_bearing_deg if command.desired_bearing_deg is not None else -999.0
        pan_error_deg_log = command.pan_error_deg if command.pan_error_deg is not None else -999.0
        bearing_rate_log = command.bearing_rate_deg_s if command.bearing_rate_deg_s is not None else 0.0
        if debug_enabled:
            active_truck_min, active_truck_max = self.pose_estimator.active_truck_limits()
            LOG.info(
                "tick armed=%s selected=%s q=%.2f stat=%.2f filt=(%.3f,%.3f) pose_est=(rail=%.3f,x=%.3f,y=%.3f,pan=%.2f) "
                "truck_limits_loaded=(%.3f,%.3f) truck_limits_active=(%.3f,%.3f) actual_logical=(%.1f,%.1f) "
                "px=(%.1f,%.1f) err_x=%.1f bearing=%.2f pan_err_deg=%.2f bearing_rate=%.2f "
                "stationary_locked=%s hold_dist=%.3f mode=%s source=%s cmd_logical=(%s,%s) cmd_driver=(%s,%s)",
                int(self.armed),
                target.node_id,
                target.quality_score,
                stationary_confidence_log,
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
                desired_bearing_log,
                pan_error_deg_log,
                bearing_rate_log,
                int(target.stationary_locked),
                hold_distance_log,
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
                "tick armed=%s selected=%s q=%.2f stat=%.2f filt=(%.3f,%.3f) rail=%.3f limits=(%.3f,%.3f) pan=%.2f "
                "px=(%.1f,%.1f) err_x=%.1f bearing=%.2f pan_err_deg=%.2f bearing_rate=%.2f "
                "stationary_locked=%s hold_dist=%.3f mode=%s source=%s cmd_logical=(%s,%s) cmd_driver=(%s,%s)",
                int(self.armed),
                target.node_id,
                target.quality_score,
                stationary_confidence_log,
                target.x_m,
                target.y_m,
                pose.rail_position_m,
                active_truck_min,
                active_truck_max,
                pose.pan_deg,
                command.projected.x_px,
                command.projected.y_px,
                command.error_x_px or 0.0,
                desired_bearing_log,
                pan_error_deg_log,
                bearing_rate_log,
                int(target.stationary_locked),
                hold_distance_log,
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
        self._safety_stop_event.clear()
        self._safety_thread = threading.Thread(
            target=self._safety_loop,
            name="motor_controller_safety",
            daemon=True,
        )
        self._safety_thread.start()
        self.maybe_prompt_arm()

    def loop_once(self) -> None:
        now = time.monotonic()
        if self.last_loop_monotonic is None:
            dt_s = 1.0 / self.config.control.control_hz
        else:
            dt_s = clamp(now - self.last_loop_monotonic, 0.0, 0.25)
        self.last_loop_monotonic = now

        try:
            if not self.armed or not self.pose_estimator.valid or not self.mqtt_connected:
                self._clear_stationary_hold("unarmed_invalid_pose")
                self.stop_motors()
                self.log_tick(AxisCommand(0, 0, "UNARMED_INVALID_POSE", None, None, "none"), None, now)
                return

            target = self.get_selected_pose(now)
            if target is None:
                self._clear_stationary_hold("no_valid_target")
                self.stop_motors()
                self.log_tick(AxisCommand(0, 0, "NO_VALID_FILTERED_TARGET", None, None, "none"), None, now)
                return

            statuses = dict(self.latest_statuses)
            if statuses:
                self._raise_faults(statuses)
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
            self._clear_stationary_hold("projection_rejected")
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
                remaining = (tick_started + tick_s) - time.monotonic()
                if remaining > 0.0:
                    time.sleep(remaining)
        except KeyboardInterrupt:
            LOG.warning("shutdown=keyboard_interrupt")
        finally:
            self.running = False
            self._safety_stop_event.set()
            if self._safety_thread is not None:
                self._safety_thread.join(timeout=2.0)
                self._safety_thread = None
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


def _optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_optional_float(value: Optional[float]) -> str:
    if value is None:
        return "none"
    return f"{value:.2f}"


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
        "pan_motor=%s pan_driver=%s truck_motor=%s truck_driver=%s rail_origin=(%.3f,%.3f) rail_heading_deg=%.2f start_rail_m=%.3f "
        "rail_limits=(%.3f,%.3f) rail_soft_margin=%.3f control_hz=%.1f status_poll_s=%.3f log_interval_s=%.3f "
        "pan_mode=%s pan_max=%s pan_deadband=%.1f pan_kp=%.3f pan_bearing_kp=%.3f pan_bearing_kd=%.3f "
        "pan_bearing_deadband=%.2f pan_velocity_filter_tau=%.3f pan_command_ramp=%.1f "
        "pan_goto_target_time=%.3f pan_goto_speed_factor=%.2f pan_goto_min_raw=%s "
        "pan_ramp=%.1f pan_filter_tau=%.3f pan_flip_hold_ms=%.0f pan_center_hold_ms=%.0f "
        "stationary_hold=%s stationary_hold_min_q=%.2f stationary_hold_window_px=%.1f "
        "stationary_hold_escape_px=%.1f stationary_hold_room_radius_m=%.3f",
        int(config.motor.enable_live),
        int(config.motor.debug),
        config.mqtt.target_node,
        config.camera.profile,
        config.camera.image_w,
        config.camera.image_h,
        int(config.motor.pan_enabled),
        int(config.motor.truck_enabled),
        config.motor.pan_motor_id,
        config.motor.pan_driver_model,
        config.motor.truck_motor_id,
        config.motor.truck_driver_model,
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
        config.control.pan_control_mode,
        config.motor.pan_max_raw_speed,
        config.control.pan_image_deadband_px,
        config.control.pan_kp_raw_per_px,
        config.control.pan_bearing_kp_raw_per_deg,
        config.control.pan_bearing_kd_raw_per_deg_s,
        config.control.pan_bearing_deadband_deg,
        config.control.pan_velocity_filter_tau_s,
        config.control.pan_command_ramp_raw_per_s,
        config.control.pan_goto_target_time_s,
        config.control.pan_goto_speed_factor,
        config.control.pan_goto_min_raw_speed,
        config.motor.pan_ramp_raw_per_s,
        config.control.pan_error_filter_tau_s,
        config.control.pan_direction_flip_hold_ms,
        config.control.pan_center_hold_ms,
        int(config.control.stationary_hold_enabled),
        config.control.stationary_hold_min_quality,
        config.control.stationary_hold_pan_window_px,
        config.control.stationary_hold_escape_window_px,
        config.control.stationary_hold_room_radius_m,
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




