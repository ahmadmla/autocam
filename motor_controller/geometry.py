"""Camera pose estimation and floor-plane projection for autocam control."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple

from .config import CameraIntrinsics, CameraPoseConfig, MotorRuntimeConfig


@dataclass(frozen=True)
class WorldPoint:
    x_m: float
    y_m: float
    z_m: float = 0.0


@dataclass(frozen=True)
class ProjectedPoint:
    x_px: float
    y_px: float
    depth_m: float


@dataclass(frozen=True)
class EstimatedCameraPose:
    rail_position_m: float
    x_m: float
    y_m: float
    pan_deg: float
    height_m: float
    pitch_deg: float
    roll_deg: float


class ProjectionError(ValueError):
    pass


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _rotate_pitch_roll(x: float, y: float, z: float, pitch_deg: float, roll_deg: float) -> Tuple[float, float, float]:
    pitch = math.radians(pitch_deg)
    cp = math.cos(pitch)
    sp = math.sin(pitch)
    # Positive pitch tilts the optical axis downward toward the floor.
    y2 = cp * y - sp * z
    z2 = sp * y + cp * z

    roll = math.radians(roll_deg)
    cr = math.cos(roll)
    sr = math.sin(roll)
    x3 = cr * x - sr * y2
    y3 = sr * x + cr * y2
    return x3, y3, z2


def project_floor_point(
    point: WorldPoint,
    camera_pose: EstimatedCameraPose,
    intrinsics: CameraIntrinsics,
) -> ProjectedPoint:
    """Project one world point on/near the floor into image coordinates.

    Convention: pan 0 faces +room Y, camera local +X is image-right, +Y is image-down,
    and +Z is forward through the lens.
    """
    rel_x = point.x_m - camera_pose.x_m
    rel_y = point.y_m - camera_pose.y_m
    rel_z = point.z_m - camera_pose.height_m

    yaw = math.radians(camera_pose.pan_deg)
    right = (math.cos(yaw), -math.sin(yaw), 0.0)
    forward = (math.sin(yaw), math.cos(yaw), 0.0)
    down = (0.0, 0.0, -1.0)

    cam_x = rel_x * right[0] + rel_y * right[1] + rel_z * right[2]
    cam_y = rel_x * down[0] + rel_y * down[1] + rel_z * down[2]
    cam_z = rel_x * forward[0] + rel_y * forward[1] + rel_z * forward[2]
    cam_x, cam_y, cam_z = _rotate_pitch_roll(
        cam_x,
        cam_y,
        cam_z,
        camera_pose.pitch_deg,
        camera_pose.roll_deg,
    )

    if not math.isfinite(cam_z) or cam_z <= 1e-6:
        raise ProjectionError("Target is behind the camera or too close to the image plane")

    x_norm = cam_x / cam_z
    y_norm = cam_y / cam_z
    if not (math.isfinite(x_norm) and math.isfinite(y_norm)):
        raise ProjectionError("Projection produced non-finite normalized coordinates")

    r2 = x_norm * x_norm + y_norm * y_norm
    radial = 1.0 + intrinsics.k1 * r2 + intrinsics.k2 * r2 * r2 + intrinsics.k3 * r2 * r2 * r2
    x_dist = x_norm * radial + 2.0 * intrinsics.p1 * x_norm * y_norm + intrinsics.p2 * (r2 + 2.0 * x_norm * x_norm)
    y_dist = y_norm * radial + intrinsics.p1 * (r2 + 2.0 * y_norm * y_norm) + 2.0 * intrinsics.p2 * x_norm * y_norm

    x_px = intrinsics.fx * x_dist + intrinsics.cx
    y_px = intrinsics.fy * y_dist + intrinsics.cy
    if not (math.isfinite(x_px) and math.isfinite(y_px)):
        raise ProjectionError("Projection produced non-finite pixel coordinates")
    return ProjectedPoint(x_px=x_px, y_px=y_px, depth_m=cam_z)


class CameraPoseEstimator:
    """Dead-reckoned camera pose estimate from commanded pan/truck speeds."""

    def __init__(self, pose_config: CameraPoseConfig, motor_config: MotorRuntimeConfig) -> None:
        self.pose_config = pose_config
        self.motor_config = motor_config
        self.valid = False
        self.pose = self._pose_from_rail(pose_config.start_rail_m, pose_config.start_pan_deg)

    def _world_xy_from_rail(self, rail_position_m: float) -> Tuple[float, float]:
        heading_rad = math.radians(self.pose_config.rail_heading_deg)
        world_x_m = self.pose_config.rail_origin_x_m + rail_position_m * math.cos(heading_rad)
        world_y_m = self.pose_config.rail_origin_y_m + rail_position_m * math.sin(heading_rad)
        return world_x_m, world_y_m

    def _pose_from_rail(self, rail_position_m: float, pan_deg: float) -> EstimatedCameraPose:
        world_x_m, world_y_m = self._world_xy_from_rail(rail_position_m)
        return EstimatedCameraPose(
            rail_position_m=rail_position_m,
            x_m=world_x_m,
            y_m=world_y_m,
            pan_deg=pan_deg,
            height_m=self.pose_config.height_m,
            pitch_deg=self.pose_config.pitch_deg,
            roll_deg=self.pose_config.roll_deg,
        )

    def _active_truck_limits(self) -> Tuple[float, float]:
        margin = max(0.0, self.motor_config.truck_soft_limit_margin_m)
        min_limit = self.motor_config.truck_min_rail_m + margin
        max_limit = self.motor_config.truck_max_rail_m - margin
        if min_limit > max_limit:
            midpoint = (self.motor_config.truck_min_rail_m + self.motor_config.truck_max_rail_m) / 2.0
            return midpoint, midpoint
        return min_limit, max_limit

    def reset_to_start(self) -> EstimatedCameraPose:
        self.pose = self._pose_from_rail(
            self.pose_config.start_rail_m,
            self.pose_config.start_pan_deg,
        )
        self.valid = True
        return self.pose

    def invalidate(self) -> None:
        self.valid = False

    def update(self, dt_s: float, logical_pan_raw: float, logical_truck_raw: float) -> EstimatedCameraPose:
        if not self.valid:
            return self.pose
        dt = clamp(float(dt_s), 0.0, 0.25)
        if self.motor_config.pan_enabled:
            next_pan = clamp(
                self.pose.pan_deg + logical_pan_raw * self.motor_config.pan_deg_per_raw_speed_s * dt,
                self.motor_config.pan_min_deg,
                self.motor_config.pan_max_deg,
            )
        else:
            # In truck-only mode the camera has a fixed heading; preserve the configured startup pan.
            next_pan = self.pose.pan_deg
        next_rail = self.pose.rail_position_m + logical_truck_raw * self.motor_config.truck_m_per_raw_speed_s * dt
        truck_min_limit, truck_max_limit = self._active_truck_limits()
        self.pose = self._pose_from_rail(
            clamp(next_rail, truck_min_limit, truck_max_limit),
            next_pan,
        )
        return self.pose

    def blocks_pan(self, logical_raw: float) -> bool:
        return (
            (self.pose.pan_deg <= self.motor_config.pan_min_deg and logical_raw < 0.0)
            or (self.pose.pan_deg >= self.motor_config.pan_max_deg and logical_raw > 0.0)
        )

    def blocks_truck(self, logical_raw: float) -> bool:
        truck_min_limit, truck_max_limit = self._active_truck_limits()
        return (
            (self.pose.rail_position_m <= truck_min_limit and logical_raw < 0.0)
            or (self.pose.rail_position_m >= truck_max_limit and logical_raw > 0.0)
        )
