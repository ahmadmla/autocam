"""Runtime configuration for dynamic homography and pan/truck control."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from env_loader import load_repo_env


load_repo_env()


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
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def env_optional_float(name: str, default: Optional[float] = None) -> Optional[float]:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class CameraIntrinsics:
    profile: str
    image_w: int
    image_h: int
    fx: float
    fy: float
    cx: float
    cy: float
    k1: float = 0.0
    k2: float = 0.0
    p1: float = 0.0
    p2: float = 0.0
    k3: float = 0.0


@dataclass(frozen=True)
class CameraPoseConfig:
    rail_origin_x_m: float
    rail_origin_y_m: float
    rail_heading_deg: float
    start_rail_m: float
    start_pan_deg: float
    height_m: float
    pitch_deg: float
    roll_deg: float


@dataclass(frozen=True)
class MotorRuntimeConfig:
    enable_live: bool
    debug: bool
    port: str
    baudrate: int
    timeout_s: float
    pan_enabled: bool
    truck_enabled: bool
    pan_motor_id: int
    truck_motor_id: int
    pan_driver_model: str
    truck_driver_model: str
    pan_pole_pairs: int
    truck_pole_pairs: int
    pan_sign: int
    truck_sign: int
    pan_deg_per_raw_speed_s: float
    truck_m_per_raw_speed_s: float
    ramp_raw_per_s: float
    pan_ramp_raw_per_s: float
    truck_ramp_raw_per_s: float
    pan_max_raw_speed: int
    truck_max_raw_speed: int
    pan_min_deg: float
    pan_max_deg: float
    truck_min_rail_m: float
    truck_max_rail_m: float
    truck_soft_limit_margin_m: float
    truck_stop_horizon_s: float
    confirm_recentered: bool
    arm_prompt: bool


@dataclass(frozen=True)
class ControlRuntimeConfig:
    target_stale_s: float
    min_quality: float
    control_hz: float
    status_poll_s: float
    log_interval_s: float
    pan_image_deadband_px: float
    truck_image_deadband_px: float
    pan_prealign_threshold_px: float
    pan_centered_threshold_px: float
    truck_enable_after_pan_centered_ms: float
    pan_control_mode: str
    pan_kp_raw_per_px: float
    pan_bearing_kp_raw_per_deg: float
    pan_bearing_kd_raw_per_deg_s: float
    pan_bearing_moving_deadband_deg: float
    pan_velocity_filter_tau_s: float
    pan_command_ramp_raw_per_s: float
    truck_kp_raw_per_px: float
    pan_error_filter_alpha: float
    pan_error_filter_tau_s: float
    pan_direction_flip_hold_ms: float
    pan_center_hold_ms: float
    truck_error_filter_alpha: float
    truck_error_filter_tau_s: float
    pan_preferred_yaw_min_deg: float
    pan_preferred_yaw_max_deg: float
    safe_image_margin_px: float
    target_image_x_px: Optional[float]
    target_image_y_px: Optional[float]
    stationary_hold_enabled: bool
    stationary_hold_min_quality: float
    stationary_hold_pan_window_px: float
    stationary_hold_escape_window_px: float
    stationary_hold_room_radius_m: float
    vision_correction_stale_s: float
    vision_correction_min_confidence: float
    vision_correction_blend: float


@dataclass(frozen=True)
class MqttRuntimeConfig:
    host: str
    port: int
    username: str
    password: str
    qos: int
    pose_topic_base: str
    target_select_topic: str
    vision_correction_topic: str
    motor_arm_topic: str
    target_node: str


@dataclass(frozen=True)
class RuntimeConfig:
    camera: CameraIntrinsics
    camera_pose: CameraPoseConfig
    motor: MotorRuntimeConfig
    control: ControlRuntimeConfig
    mqtt: MqttRuntimeConfig


CAMERA_PRESETS = {
    "iphone_13_pro_max_main_1x": CameraIntrinsics(
        profile="iphone_13_pro_max_main_1x",
        image_w=1920,
        image_h=1080,
        fx=1450.0,
        fy=1450.0,
        cx=960.0,
        cy=540.0,
    )
}


def _signed_env_int(name: str, default: int) -> int:
    value = env_int(name, default)
    return -1 if value < 0 else 1


def load_camera_intrinsics() -> CameraIntrinsics:
    profile = env_str("CAMERA_PROFILE", "iphone_13_pro_max_main_1x")
    preset = CAMERA_PRESETS.get(
        profile,
        CameraIntrinsics(
            profile=profile,
            image_w=1920,
            image_h=1080,
            fx=1450.0,
            fy=1450.0,
            cx=960.0,
            cy=540.0,
        ),
    )
    image_w = env_int("CAMERA_IMAGE_W", preset.image_w)
    image_h = env_int("CAMERA_IMAGE_H", preset.image_h)
    return CameraIntrinsics(
        profile=profile,
        image_w=image_w,
        image_h=image_h,
        fx=env_float("CAMERA_FX", preset.fx),
        fy=env_float("CAMERA_FY", preset.fy),
        cx=env_float("CAMERA_CX", preset.cx if preset.cx else image_w / 2.0),
        cy=env_float("CAMERA_CY", preset.cy if preset.cy else image_h / 2.0),
        k1=env_float("CAMERA_K1", preset.k1),
        k2=env_float("CAMERA_K2", preset.k2),
        p1=env_float("CAMERA_P1", preset.p1),
        p2=env_float("CAMERA_P2", preset.p2),
        k3=env_float("CAMERA_K3", preset.k3),
    )


def load_runtime_config() -> RuntimeConfig:
    camera = load_camera_intrinsics()
    legacy_start_x_m = env_float("CAMERA_START_X_M", 0.0)
    legacy_start_y_m = env_float("CAMERA_START_Y_M", 0.0)
    camera_pose = CameraPoseConfig(
        rail_origin_x_m=env_float("CAMERA_RAIL_ORIGIN_X_M", 0.0),
        rail_origin_y_m=env_float("CAMERA_RAIL_ORIGIN_Y_M", legacy_start_y_m),
        rail_heading_deg=env_float("CAMERA_RAIL_HEADING_DEG", 0.0),
        start_rail_m=env_float("CAMERA_START_RAIL_M", legacy_start_x_m),
        start_pan_deg=env_float("CAMERA_START_PAN_DEG", 0.0),
        height_m=env_float("CAMERA_HEIGHT_M", 1.4),
        pitch_deg=env_float("CAMERA_PITCH_DEG", 35.0),
        roll_deg=env_float("CAMERA_ROLL_DEG", 0.0),
    )
    legacy_truck_min_x_m = env_float("TRUCK_MIN_X_M", -1.0)
    legacy_truck_max_x_m = env_float("TRUCK_MAX_X_M", 1.0)
    ramp_raw_per_s = env_float("MOTOR_RAMP_RAW_PER_S", 200.0)
    motor = MotorRuntimeConfig(
        enable_live=env_bool("MOTOR_ENABLE_LIVE", False),
        debug=env_bool("MOTOR_DEBUG", False),
        port=env_str("MOTOR_MODBUS_PORT", "/dev/ttyUSB0"),
        baudrate=env_int("MOTOR_MODBUS_BAUD", 9600),
        timeout_s=env_float("MOTOR_MODBUS_TIMEOUT_S", 1.0),
        pan_enabled=env_bool("PAN_ENABLED", True),
        truck_enabled=env_bool("TRUCK_ENABLED", True),
        pan_motor_id=env_int("PAN_MOTOR_ID", 1),
        truck_motor_id=env_int("TRUCK_MOTOR_ID", 2),
        pan_driver_model=env_str("PAN_DRIVER_MODEL", "BLD510B"),
        truck_driver_model=env_str("TRUCK_DRIVER_MODEL", "BLD305S"),
        pan_pole_pairs=env_int("PAN_POLE_PAIRS", 2),
        truck_pole_pairs=env_int("TRUCK_POLE_PAIRS", 5),
        pan_sign=_signed_env_int("PAN_SIGN", 1),
        truck_sign=_signed_env_int("TRUCK_SIGN", 1),
        pan_deg_per_raw_speed_s=env_float(
            "PAN_DEG_PER_RAW_SPEED_S",
            env_float("MOTOR_MANUAL_PAN_DEG_PER_RAW_SPEED_S", 0.01),
        ),
        truck_m_per_raw_speed_s=env_float("TRUCK_M_PER_RAW_SPEED_S", 0.00005),
        ramp_raw_per_s=ramp_raw_per_s,
        pan_ramp_raw_per_s=env_float("PAN_RAMP_RAW_PER_S", ramp_raw_per_s),
        truck_ramp_raw_per_s=env_float("TRUCK_RAMP_RAW_PER_S", ramp_raw_per_s),
        pan_max_raw_speed=env_int("PAN_MAX_RAW_SPEED", 30),
        truck_max_raw_speed=env_int("TRUCK_MAX_RAW_SPEED", 60),
        pan_min_deg=env_float("PAN_MIN_DEG", -70.0),
        pan_max_deg=env_float("PAN_MAX_DEG", 70.0),
        truck_min_rail_m=env_float("TRUCK_MIN_RAIL_M", legacy_truck_min_x_m),
        truck_max_rail_m=env_float("TRUCK_MAX_RAIL_M", legacy_truck_max_x_m),
        truck_soft_limit_margin_m=max(
            0.0,
            env_float(
                "TRUCK_SOFT_LIMIT_MARGIN_M",
                env_float("MOTOR_MANUAL_LIMIT_MARGIN_M", 0.0),
            ),
        ),
        truck_stop_horizon_s=max(0.0, env_float("TRUCK_STOP_HORIZON_S", 0.25)),
        confirm_recentered=env_bool("MOTOR_CONFIRM_RECENTERED", False),
        arm_prompt=env_bool("MOTOR_ARM_PROMPT", True),
    )
    control = ControlRuntimeConfig(
        target_stale_s=env_float("MOTOR_TARGET_STALE_S", 0.75),
        min_quality=env_float("MOTOR_MIN_QUALITY", 0.55),
        control_hz=max(env_float("PAN_CONTROL_HZ", 20.0), 1.0),
        status_poll_s=max(
            0.005,
            env_float(
                "MOTOR_STATUS_POLL_S",
                env_float("MOTOR_MANUAL_STATUS_POLL_S", 1.0 / max(env_float("PAN_CONTROL_HZ", 20.0), 1.0)),
            ),
        ),
        log_interval_s=max(0.0, env_float("MOTOR_LOG_INTERVAL_S", 1.0)),
        pan_image_deadband_px=env_float("PAN_IMAGE_DEADBAND_PX", 40.0),
        truck_image_deadband_px=env_float("TRUCK_IMAGE_DEADBAND_PX", 80.0),
        pan_prealign_threshold_px=env_float("PAN_PREALIGN_THRESHOLD_PX", 160.0),
        pan_centered_threshold_px=env_float("PAN_CENTERED_THRESHOLD_PX", 60.0),
        truck_enable_after_pan_centered_ms=env_float("TRUCK_ENABLE_AFTER_PAN_CENTERED_MS", 500.0),
        pan_control_mode=env_str("PAN_CONTROL_MODE", "BEARING_VELOCITY").strip().upper(),
        pan_kp_raw_per_px=env_float("PAN_KP_RAW_PER_PX", 0.45),
        pan_bearing_kp_raw_per_deg=env_float("PAN_BEARING_KP_RAW_PER_DEG", 3.0),
        pan_bearing_kd_raw_per_deg_s=env_float("PAN_BEARING_KD_RAW_PER_DEG_S", 0.50),
        pan_bearing_moving_deadband_deg=env_float("PAN_BEARING_MOVING_DEADBAND_DEG", 1.0),
        pan_velocity_filter_tau_s=env_float("PAN_VELOCITY_FILTER_TAU_S", 0.12),
        pan_command_ramp_raw_per_s=env_float("PAN_COMMAND_RAMP_RAW_PER_S", max(motor.pan_ramp_raw_per_s, 120.0)),
        truck_kp_raw_per_px=env_float("TRUCK_KP_RAW_PER_PX", 0.08),
        pan_error_filter_alpha=env_float("PAN_ERROR_FILTER_ALPHA", 1.0),
        pan_error_filter_tau_s=env_float("PAN_ERROR_FILTER_TAU_S", 0.0),
        pan_direction_flip_hold_ms=env_float("PAN_DIRECTION_FLIP_HOLD_MS", 0.0),
        pan_center_hold_ms=env_float("PAN_CENTER_HOLD_MS", 0.0),
        truck_error_filter_alpha=env_float("TRUCK_ERROR_FILTER_ALPHA", 0.05),
        truck_error_filter_tau_s=env_float("TRUCK_ERROR_FILTER_TAU_S", 0.15),
        pan_preferred_yaw_min_deg=env_float("PAN_PREFERRED_YAW_MIN_DEG", -25.0),
        pan_preferred_yaw_max_deg=env_float("PAN_PREFERRED_YAW_MAX_DEG", 25.0),
        safe_image_margin_px=env_float("MOTOR_SAFE_IMAGE_MARGIN_PX", 120.0),
        target_image_x_px=env_optional_float("MOTOR_TARGET_IMAGE_X_PX"),
        target_image_y_px=env_optional_float("MOTOR_TARGET_IMAGE_Y_PX"),
        stationary_hold_enabled=env_bool("STATIONARY_HOLD_ENABLED", True),
        stationary_hold_min_quality=env_float("STATIONARY_HOLD_MIN_QUALITY", 0.60),
        stationary_hold_pan_window_px=env_float("STATIONARY_HOLD_PAN_WINDOW_PX", 500.0),
        stationary_hold_escape_window_px=env_float("STATIONARY_HOLD_ESCAPE_WINDOW_PX", 900.0),
        stationary_hold_room_radius_m=env_float("STATIONARY_HOLD_ROOM_RADIUS_M", 0.12),
        vision_correction_stale_s=env_float("VISION_CORRECTION_STALE_S", 0.25),
        vision_correction_min_confidence=env_float("VISION_CORRECTION_MIN_CONFIDENCE", 0.60),
        vision_correction_blend=env_float("VISION_CORRECTION_BLEND", 0.0),
    )
    mqtt = MqttRuntimeConfig(
        host=env_str("MQTT_HOST", "127.0.0.1"),
        port=env_int("MQTT_PORT", 1883),
        username=env_str("MQTT_USERNAME", ""),
        password=env_str("MQTT_PASSWORD", ""),
        qos=env_int("MQTT_QOS", 0),
        pose_topic_base=env_str("AUTOCAM_POSE_TOPIC_BASE", "autocam/pose"),
        target_select_topic=env_str("AUTOCAM_TARGET_SELECT_TOPIC", "autocam/target/select"),
        vision_correction_topic=env_str("AUTOCAM_VISION_CORRECTION_TOPIC", "autocam/vision/correction"),
        motor_arm_topic=env_str("AUTOCAM_MOTOR_ARM_TOPIC", "autocam/motor/arm"),
        target_node=env_str("AUTOCAM_TARGET_NODE", "rpi1"),
    )
    return RuntimeConfig(camera=camera, camera_pose=camera_pose, motor=motor, control=control, mqtt=mqtt)



