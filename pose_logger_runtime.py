"""
Runtime entrypoint for the central pose logger.

Geometry/state, solver logic, and I/O helpers live in `uwb_pose/`.
"""

import json
import math
import time
from collections import deque
from typing import Dict, Optional, Tuple

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

from uwb_pose import geometry as geom_state
from uwb_pose.config import (
    AUTOCAM_POSE_TOPIC_BASE,
    FILTER_ALPHA_MAX,
    FILTER_ALPHA_MIN,
    FILTER_BETA_MAX,
    FILTER_BETA_MIN,
    FILTER_HOLD_VELOCITY_DAMPING,
    FILTER_VELOCITY_DAMPING,
    FAST_MOTION_SCALE_M,
    INNOVATION_DEADZONE_M,
    INNOVATION_SPEED_ALLOWANCE_MPS,
    LOW_QUALITY_BLEND_END_SCORE,
    LOW_QUALITY_HOLD_BASE_M,
    LOW_QUALITY_HOLD_SCORE,
    LOW_QUALITY_HOLD_SPEED_ALLOWANCE_MPS,
    LOW_QUALITY_INNOVATION_DEADZONE_BONUS_M,
    LOW_QUALITY_MAX_INNOVATION_M,
    LOW_QUALITY_MEASUREMENT_GAIN_MIN,
    MAX_DT_S,
    MAX_TRACK_SPEED_MPS,
    MIN_DT_S,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_QOS,
    MQTT_TOPIC,
    MQTT_USERNAME,
    NODE_HEIGHT_DEFAULT_M,
    OUTPUT_DEADBAND_M,
    OUTPUT_LOW_QUALITY_DEADBAND_BONUS_M,
    PIPELINE_DIAGNOSTIC_INTERVAL_S,
    STATIONARY_ANALYSIS_MAX_SPREAD_M,
    STATIONARY_ANALYSIS_MIN_ANCHOR_SAMPLES,
    STATIONARY_ANALYSIS_MIN_QUALITY,
    STATIONARY_ANALYSIS_START_CONFIDENCE,
    STATIONARY_ANALYSIS_STOP_CONFIDENCE,
    STATIONARY_ANALYSIS_WARMUP_SAMPLES,
    STATIONARY_ANALYSIS_WINDOW,
    STATIONARY_ENTRY_INNOVATION_M,
    STATIONARY_ENTRY_SPEED_MPS,
    STATIONARY_EXIT_INNOVATION_M,
    STATIONARY_EXIT_SPEED_MPS,
    STATIONARY_MEASUREMENT_GAIN_MIN,
    STATIONARY_MIN_QUALITY,
    STATIONARY_OUTPUT_DEADBAND_BONUS_M,
    STATIONARY_VELOCITY_DAMPING,
    node_height_m,
)
from uwb_pose.geometry import clamp, lerp, median, trimmed_mean
from uwb_pose.io_helpers import (
    HistoryWriter,
    close_text_logs,
    emit_log,
    format_log_timestamp,
    parse_block,
    reset_text_logs,
)
from uwb_pose.solver import (
    AnchorState,
    SolverResult,
    apply_geometry_bias,
    apply_radial_deadzone,
    build_quality,
    format_pose,
    point_dict,
    round_float,
    rounded_range_map,
    rounded_value_map,
    sanitize_distance_cm,
    solve_with_outlier_retry,
)


class PoseTracker:
    # Initialize the tracker state used to smooth and gate successive pose estimates.
    def __init__(self) -> None:
        self.x: Optional[float] = None
        self.y: Optional[float] = None
        self.vx = 0.0
        self.vy = 0.0
        self.reported_x: Optional[float] = None
        self.reported_y: Optional[float] = None
        self.last_timestamp: Optional[float] = None
        self.stationary_confidence = 0.0

    # Predict the next pose from the current state and velocity at the given timestamp.
    def predict(self, timestamp: float) -> Optional[Tuple[float, float]]:
        if self.x is None or self.y is None or self.last_timestamp is None:
            return None

        dt = clamp(timestamp - self.last_timestamp, MIN_DT_S, MAX_DT_S)
        px = self.x + self.vx * dt
        py = self.y + self.vy * dt
        return geom_state.soft_project_point(px, py)

    # Keep the internal state inside the valid room bounds and zero outward velocity at edges.
    def _enforce_bounds(self) -> None:
        if self.x is None or self.y is None:
            return

        self.x, self.y = geom_state.soft_project_point(self.x, self.y)
        self.x, self.y = geom_state.project_point(self.x, self.y)

        if (self.x <= geom_state.GEOM.min_x + 1e-6 and self.vx < 0.0) or (
            self.x >= geom_state.GEOM.max_x - 1e-6 and self.vx > 0.0
        ):
            self.vx = 0.0

        if (self.y <= geom_state.GEOM.min_y + 1e-6 and self.vy < 0.0) or (
            self.y >= geom_state.GEOM.max_y - 1e-6 and self.vy > 0.0
        ):
            self.vy = 0.0

    # Apply output deadband logic so small pose changes do not visibly jitter the reported point.
    def _update_reported_pose(self, quality_score: float) -> Tuple[Optional[Tuple[float, float]], dict]:
        if self.x is None or self.y is None:
            return None, {
                "output_deadband_m": None,
                "reported_step_m": None,
                "deadband_hold": False,
            }

        target_x, target_y = geom_state.project_point(self.x, self.y)
        deadband_m = OUTPUT_DEADBAND_M + (
            (1.0 - clamp(quality_score, 0.0, 1.0)) * OUTPUT_LOW_QUALITY_DEADBAND_BONUS_M
        )
        deadband_m += self.stationary_confidence * STATIONARY_OUTPUT_DEADBAND_BONUS_M

        if self.reported_x is None or self.reported_y is None:
            self.reported_x = target_x
            self.reported_y = target_y
            return (
                (self.reported_x, self.reported_y),
                {
                    "output_deadband_m": round_float(deadband_m, 4),
                    "reported_step_m": 0.0,
                    "deadband_hold": False,
                    "stationary_confidence": round_float(self.stationary_confidence, 4),
                },
            )

        dx = target_x - self.reported_x
        dy = target_y - self.reported_y
        delta_m = math.hypot(dx, dy)

        if delta_m <= deadband_m:
            return (
                (self.reported_x, self.reported_y),
                {
                    "output_deadband_m": round_float(deadband_m, 4),
                    "reported_step_m": 0.0,
                    "deadband_hold": True,
                    "stationary_confidence": round_float(self.stationary_confidence, 4),
                },
            )

        release_scale = (delta_m - deadband_m) / max(delta_m, 1e-9)
        self.reported_x += dx * release_scale
        self.reported_y += dy * release_scale
        self.reported_x, self.reported_y = geom_state.soft_project_point(
            self.reported_x,
            self.reported_y,
        )
        self.reported_x, self.reported_y = geom_state.project_point(
            self.reported_x,
            self.reported_y,
        )
        reported_step_m = math.hypot(dx * release_scale, dy * release_scale)
        return (
            (self.reported_x, self.reported_y),
            {
                "output_deadband_m": round_float(deadband_m, 4),
                "reported_step_m": round_float(reported_step_m, 4),
                "deadband_hold": False,
                "stationary_confidence": round_float(self.stationary_confidence, 4),
                },
            )

    # Update the tracker???s belief that the tag is stationary based on innovation, speed, and quality.
    def _update_stationary_confidence(
        self,
        raw_innovation_m: float,
        quality_score: float,
        prior_speed_mps: float,
    ) -> None:
        stable = (
            quality_score >= STATIONARY_MIN_QUALITY
            and raw_innovation_m <= STATIONARY_ENTRY_INNOVATION_M
            and prior_speed_mps <= STATIONARY_ENTRY_SPEED_MPS
        )
        moving = (
            raw_innovation_m >= STATIONARY_EXIT_INNOVATION_M
            or prior_speed_mps >= STATIONARY_EXIT_SPEED_MPS
        )

        if stable:
            target = 1.0
            alpha = 0.50
        elif moving:
            target = 0.0
            alpha = 0.10
        elif quality_score >= STATIONARY_MIN_QUALITY:
            target = 0.92
            alpha = 0.20
        else:
            target = 0.0
            alpha = 0.10

        self.stationary_confidence = clamp(
            lerp(self.stationary_confidence, target, alpha),
            0.0,
            1.0,
        )

    # Advance the tracker without a new measurement by damping velocity and holding position.
    def hold(self, timestamp: float) -> Optional[Tuple[float, float]]:
        if self.x is None or self.y is None:
            return None
        if self.last_timestamp is None:
            self.last_timestamp = timestamp
            return (self.x, self.y)

        dt = clamp(timestamp - self.last_timestamp, MIN_DT_S, MAX_DT_S)
        self.x += self.vx * dt
        self.y += self.vy * dt
        self.vx *= FILTER_HOLD_VELOCITY_DAMPING
        self.vy *= FILTER_HOLD_VELOCITY_DAMPING
        self._enforce_bounds()
        self.last_timestamp = timestamp
        return (self.x, self.y)

    # Fold a new measured pose into the tracker and return the stabilized reported pose plus diagnostics.
    def update(
        self,
        measurement_xy: Optional[Tuple[float, float]],
        timestamp: float,
        quality_score: float,
    ) -> Tuple[Optional[Tuple[float, float]], dict]:
        if measurement_xy is None:
            held = self.hold(timestamp)
            if held is None:
                return None, {
                    "mode": "hold",
                    "innovation_m": None,
                    "innovation_deadzone_m": None,
                    "speed_mps": None,
                    "output_deadband_m": None,
                    "reported_step_m": None,
                    "deadband_hold": False,
                }

            reported_pose, output_debug = self._update_reported_pose(quality_score)
            return reported_pose, {
                "mode": "hold",
                "innovation_m": None,
                "innovation_deadzone_m": None,
                "speed_mps": round_float(math.hypot(self.vx, self.vy), 4),
                **output_debug,
            }

        mx, my = geom_state.soft_project_point(*measurement_xy)
        if self.x is None or self.y is None or self.last_timestamp is None:
            self.x, self.y = geom_state.project_point(mx, my)
            self.vx = 0.0
            self.vy = 0.0
            self.last_timestamp = timestamp
            reported_pose, output_debug = self._update_reported_pose(quality_score)
            return (
                reported_pose,
                {
                    "mode": "init",
                    "innovation_m": 0.0,
                    "innovation_deadzone_m": round_float(
                        INNOVATION_DEADZONE_M
                        + ((1.0 - clamp(quality_score, 0.0, 1.0)) * LOW_QUALITY_INNOVATION_DEADZONE_BONUS_M),
                        4,
                    ),
                    "speed_mps": 0.0,
                    **output_debug,
                },
            )

        dt = clamp(timestamp - self.last_timestamp, MIN_DT_S, MAX_DT_S)
        prior_speed_mps = math.hypot(self.vx, self.vy)
        pred_x = self.x + self.vx * dt
        pred_y = self.y + self.vy * dt
        pred_x, pred_y = geom_state.soft_project_point(pred_x, pred_y)

        innovation_x = mx - pred_x
        innovation_y = my - pred_y
        raw_innovation_m = math.hypot(innovation_x, innovation_y)
        innovation_m = raw_innovation_m
        self._update_stationary_confidence(raw_innovation_m, quality_score, prior_speed_mps)

        if quality_score < LOW_QUALITY_HOLD_SCORE:
            guarded_hold_limit = LOW_QUALITY_HOLD_BASE_M + LOW_QUALITY_HOLD_SPEED_ALLOWANCE_MPS * dt
            if raw_innovation_m > guarded_hold_limit:
                held = self.hold(timestamp)
                if held is None:
                    return None, {
                        "mode": "guarded_hold",
                        "innovation_m": round_float(raw_innovation_m, 4),
                        "innovation_deadzone_m": None,
                        "speed_mps": None,
                        "output_deadband_m": None,
                        "reported_step_m": None,
                        "deadband_hold": False,
                    }

                reported_pose, output_debug = self._update_reported_pose(quality_score)
                return reported_pose, {
                    "mode": "guarded_hold",
                    "innovation_m": round_float(raw_innovation_m, 4),
                    "innovation_deadzone_m": 0.0,
                    "speed_mps": round_float(math.hypot(self.vx, self.vy), 4),
                    **output_debug,
                }

        if quality_score < 0.70:
            max_innovation = LOW_QUALITY_MAX_INNOVATION_M + INNOVATION_SPEED_ALLOWANCE_MPS * dt
            if innovation_m > max_innovation:
                scale = max_innovation / max(innovation_m, 1e-9)
                innovation_x *= scale
                innovation_y *= scale
                innovation_m = math.hypot(innovation_x, innovation_y)

        if quality_score < LOW_QUALITY_BLEND_END_SCORE:
            measurement_gain = lerp(
                LOW_QUALITY_MEASUREMENT_GAIN_MIN,
                1.0,
                (quality_score - LOW_QUALITY_HOLD_SCORE)
                / max(LOW_QUALITY_BLEND_END_SCORE - LOW_QUALITY_HOLD_SCORE, 1e-9),
            )
            innovation_x *= measurement_gain
            innovation_y *= measurement_gain
            innovation_m = math.hypot(innovation_x, innovation_y)

        innovation_deadzone_m = INNOVATION_DEADZONE_M + (
            (1.0 - clamp(quality_score, 0.0, 1.0)) * LOW_QUALITY_INNOVATION_DEADZONE_BONUS_M
        )
        innovation_x, innovation_y, innovation_m = apply_radial_deadzone(
            innovation_x,
            innovation_y,
            innovation_deadzone_m,
        )

        if self.stationary_confidence > 0.0 and raw_innovation_m <= STATIONARY_EXIT_INNOVATION_M:
            stationary_gain = lerp(
                1.0,
                STATIONARY_MEASUREMENT_GAIN_MIN,
                self.stationary_confidence,
            )
            innovation_x *= stationary_gain
            innovation_y *= stationary_gain
            innovation_m = math.hypot(innovation_x, innovation_y)

        if self.stationary_confidence >= 0.75 and prior_speed_mps <= 0.03:
            self.vx = 0.0
            self.vy = 0.0

        motion_factor = clamp(innovation_m / FAST_MOTION_SCALE_M, 0.0, 1.0)
        response = max(quality_score, motion_factor)

        alpha = lerp(FILTER_ALPHA_MIN, FILTER_ALPHA_MAX, response)
        beta = lerp(FILTER_BETA_MIN, FILTER_BETA_MAX, response)

        self.x = pred_x + alpha * innovation_x
        self.y = pred_y + alpha * innovation_y
        self.vx = FILTER_VELOCITY_DAMPING * self.vx + (beta / dt) * innovation_x
        self.vy = FILTER_VELOCITY_DAMPING * self.vy + (beta / dt) * innovation_y

        if self.stationary_confidence > 0.0:
            velocity_damping = lerp(1.0, STATIONARY_VELOCITY_DAMPING, self.stationary_confidence)
            self.vx *= velocity_damping
            self.vy *= velocity_damping

        speed_mps = math.hypot(self.vx, self.vy)
        if speed_mps > MAX_TRACK_SPEED_MPS:
            scale = MAX_TRACK_SPEED_MPS / max(speed_mps, 1e-9)
            self.vx *= scale
            self.vy *= scale
            speed_mps = MAX_TRACK_SPEED_MPS

        self._enforce_bounds()
        self.last_timestamp = timestamp
        reported_pose, output_debug = self._update_reported_pose(quality_score)
        return (
            reported_pose,
            {
                "mode": "track",
                "innovation_m": round_float(innovation_m, 4),
                "innovation_deadzone_m": round_float(innovation_deadzone_m, 4),
                "speed_mps": round_float(speed_mps, 4),
                "stationary_confidence": round_float(self.stationary_confidence, 4),
                **output_debug,
            },
        )


class TrackingPipeline:
    _last_global_emit_monotonic: Optional[float] = None

    # Build the per-node tracking pipeline that owns anchor filters, solver state, and diagnostics.
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.node_height_m = node_height_m(node_id)
        self.anchor_states = {
            anchor_id: AnchorState(
                anchor_id=anchor_id,
                x=float(geom_state.ANCHORS[anchor_id]["x"]),
                y=float(geom_state.ANCHORS[anchor_id]["y"]),
                vertical_delta_m=self.node_height_m,
            )
            for anchor_id in geom_state.ANCHOR_IDS
        }
        self.tracker = PoseTracker()
        self.last_raw_pose: Optional[Tuple[float, float]] = None
        self.last_measurement_pose: Optional[Tuple[float, float]] = None
        self.anchor_profile_name = geom_state.ANCHOR_PROFILE_NAME
        self.last_sequence_number: Optional[int] = None
        self.last_slot_token: Optional[str] = None
        self.last_anchor_fingerprint: Optional[str] = None
        self.last_measurement_timestamp: Optional[float] = None
        self.blocks_seen = 0
        self.frames_emitted = 0
        self.duplicate_blocks = 0
        self.no_pose_blocks = 0
        self.last_diagnostic_monotonic = time.monotonic()
        self.last_emit_monotonic: Optional[float] = None
        self.last_success_anchor_ids = ""
        self.last_status_summary = ""
        self.stationary_samples = deque(
            maxlen=STATIONARY_ANALYSIS_WARMUP_SAMPLES + STATIONARY_ANALYSIS_WINDOW
        )
        self.stationary_locked_pose: Optional[Tuple[float, float]] = None
        self.stationary_analysis_spread_m: Optional[float] = None

    # Rebuild per-anchor state when the shared anchor configuration changes.
    def _apply_anchor_config(self) -> None:
        self.anchor_profile_name = geom_state.ANCHOR_PROFILE_NAME
        for anchor_id in geom_state.ANCHOR_IDS:
            anchor = geom_state.ANCHORS[anchor_id]
            state = self.anchor_states.get(anchor_id)
            if state is None:
                self.anchor_states[anchor_id] = AnchorState(
                    anchor_id=anchor_id,
                    x=float(anchor["x"]),
                    y=float(anchor["y"]),
                    vertical_delta_m=self.node_height_m,
                )
                continue

            state.x = float(anchor["x"])
            state.y = float(anchor["y"])
            state.vertical_delta_m = self.node_height_m
            state.raw_window.clear()
            state.raw_range_m = None
            state.filtered_range_m = None
            state.status = "MISSING"
            state.last_good_time = None
            state.last_good_sequence = None

        self.anchor_states = {
            anchor_id: self.anchor_states[anchor_id]
            for anchor_id in geom_state.ANCHOR_IDS
        }

    # Reload the anchor layout from disk when it changes and reset dependent tracking state.
    def reload_anchor_config_if_needed(self) -> None:
        if not geom_state.refresh_anchor_config_if_needed():
            return
        self._apply_anchor_config()
        self.tracker = PoseTracker()
        self.last_raw_pose = None
        self.last_measurement_pose = None
        emit_log(f"node={self.node_id} anchors=reloaded profile={self.anchor_profile_name}")

    # Periodically log why a pipeline is still not emitting poses yet.
    def emit_waiting_diagnostic_if_needed(self) -> None:
        if self.frames_emitted > 0:
            return
        now = time.monotonic()
        if (now - self.last_diagnostic_monotonic) < PIPELINE_DIAGNOSTIC_INTERVAL_S:
            return
        emit_log(
            f"node={self.node_id} pipeline=waiting "
            f"blocks={self.blocks_seen} "
            f"duplicates={self.duplicate_blocks} "
            f"no_pose={self.no_pose_blocks} "
            f"last_seq={self.last_sequence_number} "
            f"success_ids={self.last_success_anchor_ids or '-'} "
            f"statuses={self.last_status_summary or '-'}"
        )
        self.last_diagnostic_monotonic = now

    # Parse one raw serial block and hand it off to the normal measurement path.
    def process_block(self, block: str, timestamp: float) -> Optional[dict]:
        sequence_number, anchor_info = parse_block(block)
        return self.process_measurement(
            timestamp=timestamp,
            sequence_number=sequence_number,
            anchor_info=anchor_info,
        )

    # Convert one timestamped anchor snapshot into a solved, filtered history entry when possible.
    def process_measurement(
        self,
        timestamp: float,
        sequence_number: Optional[int],
        anchor_info: Dict[str, dict],
        slot_token: Optional[str] = None,
    ) -> Optional[dict]:
        self.reload_anchor_config_if_needed()
        self.blocks_seen += 1

        anchor_fingerprint = json.dumps(
            anchor_info,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

        if slot_token is not None:
            if slot_token == self.last_slot_token:
                self.duplicate_blocks += 1
                self.emit_waiting_diagnostic_if_needed()
                return None
            self.last_slot_token = slot_token
        elif sequence_number is not None:
            immediate_repeat = (
                self.last_measurement_timestamp is not None
                and abs(timestamp - self.last_measurement_timestamp) <= 0.25
            )
            same_payload = anchor_fingerprint == self.last_anchor_fingerprint
            if (
                sequence_number == self.last_sequence_number
                and immediate_repeat
                and same_payload
            ):
                self.duplicate_blocks += 1
                self.emit_waiting_diagnostic_if_needed()
                return None

        self.last_sequence_number = sequence_number
        self.last_anchor_fingerprint = anchor_fingerprint
        self.last_measurement_timestamp = timestamp

        raw_slant_ranges_m = {}
        raw_ranges_m = {}
        for anchor_id in geom_state.ANCHOR_IDS:
            info = anchor_info.get(anchor_id, {"status": "MISSING", "distance_cm": None})
            distance_cm = sanitize_distance_cm(info.get("distance_cm"))
            if info.get("status") == "SUCCESS" and distance_cm is not None:
                raw_slant_ranges_m[anchor_id] = distance_cm / 100.0
            raw_range = self.anchor_states[anchor_id].update(
                status=info["status"],
                distance_cm=info["distance_cm"],
                timestamp=timestamp,
                sequence_number=sequence_number,
            )
            if raw_range is not None:
                raw_ranges_m[anchor_id] = raw_range

        success_ids = [anchor_id for anchor_id in geom_state.ANCHOR_IDS if anchor_id in raw_ranges_m]
        status_counts = {}
        for anchor_id in geom_state.ANCHOR_IDS:
            status = anchor_info.get(anchor_id, {"status": "MISSING"})["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        self.last_success_anchor_ids = ",".join(success_ids)
        self.last_status_summary = ",".join(
            f"{status}:{count}" for status, count in sorted(status_counts.items())
        )

        success_count = len(raw_ranges_m)
        predicted_seed = self.tracker.predict(timestamp)
        raw_geometry_seed = predicted_seed or self.last_measurement_pose or self.last_raw_pose

        raw_items = {
            anchor_id: {
                "anchor_id": anchor_id,
                "x": self.anchor_states[anchor_id].x,
                "y": self.anchor_states[anchor_id].y,
                "range_m": range_m,
                "weight": 1.0,
                "age_s": 0.0,
                "stale": False,
            }
            for anchor_id, range_m in raw_ranges_m.items()
        }
        weighted_raw_items = apply_geometry_bias(raw_items, raw_geometry_seed)

        raw_result = solve_with_outlier_retry(
            weighted_raw_items,
            self.last_raw_pose or predicted_seed or self.last_measurement_pose,
        )
        raw_pose = raw_result.point if raw_result else None
        if raw_pose is not None:
            self.last_raw_pose = raw_pose

        filtered_items = {}
        for anchor_id in geom_state.ANCHOR_IDS:
            item = self.anchor_states[anchor_id].filtered_measurement(timestamp)
            if item is not None:
                filtered_items[anchor_id] = item

        filtered_geometry_seed = predicted_seed or raw_pose or self.last_measurement_pose
        weighted_filtered_items = apply_geometry_bias(filtered_items, filtered_geometry_seed)

        measurement_result = solve_with_outlier_retry(
            weighted_filtered_items,
            predicted_seed or raw_pose or self.last_measurement_pose,
        )

        measurement_pose = measurement_result.point if measurement_result else raw_pose
        pose_mode = "filtered_ranges"
        selected_result = measurement_result

        if measurement_result is None and raw_pose is not None:
            pose_mode = "raw_fallback"
            selected_result = raw_result
        elif measurement_pose is None:
            pose_mode = "hold"

        if measurement_pose is not None:
            self.last_measurement_pose = measurement_pose

        quality = build_quality(selected_result, measurement_result, raw_result, success_count)
        final_pose, filter_debug = self.tracker.update(
            measurement_pose,
            timestamp,
            quality["score"],
        )

        self._update_stationary_analysis(
            measurement_result,
            filtered_items,
            quality["score"],
        )
        stationary_locked = self.stationary_locked_pose is not None
        if stationary_locked:
            final_pose = self.stationary_locked_pose

        if final_pose is None:
            self.no_pose_blocks += 1
            self.emit_waiting_diagnostic_if_needed()
            return None

        quality["pose_mode"] = pose_mode
        quality["filter_mode"] = filter_debug["mode"]
        quality["innovation_m"] = filter_debug["innovation_m"]
        quality["innovation_deadzone_m"] = filter_debug["innovation_deadzone_m"]
        quality["speed_mps"] = filter_debug["speed_mps"]
        quality["stationary_confidence"] = filter_debug.get("stationary_confidence")
        quality["output_deadband_m"] = filter_debug["output_deadband_m"]
        quality["reported_step_m"] = filter_debug["reported_step_m"]
        quality["deadband_hold"] = filter_debug["deadband_hold"]
        quality["stationary_locked"] = stationary_locked
        quality["stationary_spread_m"] = self.stationary_analysis_spread_m

        entry = {
            "node_id": self.node_id,
            "node_height_m": round_float(self.node_height_m),
            "timestamp": timestamp,
            "sequence_number": sequence_number,
            "slot_token": slot_token,
            "anchors": anchor_info,
            "raw_slant_ranges_m": rounded_range_map(raw_slant_ranges_m),
            "raw_ranges_m": rounded_range_map(raw_ranges_m),
            "filtered_ranges_m": rounded_range_map(
                {
                    anchor_id: item["range_m"]
                    for anchor_id, item in filtered_items.items()
                }
            ),
            "raw_anchor_weights": rounded_value_map(
                {
                    anchor_id: item["weight"]
                    for anchor_id, item in weighted_raw_items.items()
                }
            ),
            "filtered_anchor_weights": rounded_value_map(
                {
                    anchor_id: item["weight"]
                    for anchor_id, item in weighted_filtered_items.items()
                }
            ),
            "raw_pose": point_dict(raw_pose),
            "pose": point_dict(final_pose),
            "quality": quality,
        }

        self.frames_emitted += 1
        emit_wallclock = time.time()
        emit_monotonic = time.monotonic()
        global_gap_ms = (
            None
            if TrackingPipeline._last_global_emit_monotonic is None
            else (emit_monotonic - TrackingPipeline._last_global_emit_monotonic) * 1000.0
        )
        node_gap_ms = (
            None
            if self.last_emit_monotonic is None
            else (emit_monotonic - self.last_emit_monotonic) * 1000.0
        )
        TrackingPipeline._last_global_emit_monotonic = emit_monotonic
        self.last_emit_monotonic = emit_monotonic
        log_line = (
            f"at={format_log_timestamp(emit_wallclock)} "
            f"node={self.node_id} "
            f"seq={sequence_number} "
            f"src_ts={format_log_timestamp(timestamp)} "
            f"raw={format_pose(raw_pose)} "
            f"filt={format_pose(final_pose)} "
            f"height_m={self.node_height_m:.3f} "
            f"anchors={success_count}/{len(filtered_items)} "
            f"quality={quality['score']:.2f} "
            f"stat={quality['stationary_confidence']:.2f}"
        )
        if global_gap_ms is not None:
            log_line += f" gap_ms={global_gap_ms:.0f}"
        if node_gap_ms is not None:
            log_line += f" node_gap_ms={node_gap_ms:.0f}"
        if quality["selected_rmse_m"] is not None:
            log_line += f" rms={quality['selected_rmse_m']:.3f}"
        if quality["stale_anchors_used"]:
            log_line += f" stale={quality['stale_anchors_used']}"
        if quality["dropped_anchor_id"] is not None:
            log_line += f" drop={quality['dropped_anchor_id']}"
        if stationary_locked:
            log_line += f" lock=1 spread={self.stationary_analysis_spread_m}"

        emit_log(log_line)
        return entry

    # Clear any accumulated stationary-analysis samples and derived locked pose.
    def _reset_stationary_analysis(self) -> None:
        self.stationary_samples.clear()
        self.stationary_locked_pose = None
        self.stationary_analysis_spread_m = None

    # Derive a locked pose from a stable stationary sample window when the spread is tight enough.
    def _build_stationary_locked_pose(self) -> Optional[Tuple[float, float]]:
        if len(self.stationary_samples) < (
            STATIONARY_ANALYSIS_WARMUP_SAMPLES + STATIONARY_ANALYSIS_WINDOW
        ):
            return None

        usable = list(self.stationary_samples)[STATIONARY_ANALYSIS_WARMUP_SAMPLES:]
        xs = [sample["point"][0] for sample in usable]
        ys = [sample["point"][1] for sample in usable]
        seed_x = median(xs)
        seed_y = median(ys)
        if seed_x is None or seed_y is None:
            return None
        seed_xy = (seed_x, seed_y)

        radii = [
            math.hypot(sample["point"][0] - seed_x, sample["point"][1] - seed_y)
            for sample in usable
        ]
        spread_m = trimmed_mean(radii, 0.15)
        self.stationary_analysis_spread_m = round_float(spread_m, 4)
        if spread_m is None or spread_m > STATIONARY_ANALYSIS_MAX_SPREAD_M:
            return None

        averaged_ranges = {}
        for anchor_id in geom_state.ANCHOR_IDS:
            values = [
                sample["ranges_m"][anchor_id]
                for sample in usable
                if anchor_id in sample["ranges_m"]
            ]
            if len(values) >= STATIONARY_ANALYSIS_MIN_ANCHOR_SAMPLES:
                averaged = trimmed_mean(values, 0.15)
                if averaged is not None:
                    averaged_ranges[anchor_id] = averaged

        if len(averaged_ranges) < 4:
            return None

        avg_items = {
            anchor_id: {
                "anchor_id": anchor_id,
                "x": self.anchor_states[anchor_id].x,
                "y": self.anchor_states[anchor_id].y,
                "range_m": range_m,
                "weight": 1.0,
                "age_s": 0.0,
                "stale": False,
            }
            for anchor_id, range_m in averaged_ranges.items()
        }

        weighted_avg_items = apply_geometry_bias(avg_items, seed_xy)
        locked_result = solve_with_outlier_retry(weighted_avg_items, seed_xy)
        if locked_result is None:
            return None
        return locked_result.point

    # Feed stable measurements into the stationary-analysis window and lock a pose when warranted.
    def _update_stationary_analysis(
        self,
        measurement_result: Optional[SolverResult],
        filtered_items: Dict[str, dict],
        quality_score: float,
    ) -> None:
        confidence = self.tracker.stationary_confidence
        if confidence <= STATIONARY_ANALYSIS_STOP_CONFIDENCE:
            if self.stationary_locked_pose is not None or self.stationary_samples:
                emit_log(f"node={self.node_id} stationary_analysis=reset conf={confidence:.2f}")
            self._reset_stationary_analysis()
            return

        if self.stationary_locked_pose is not None:
            return

        if (
            confidence < STATIONARY_ANALYSIS_START_CONFIDENCE
            or measurement_result is None
            or quality_score < STATIONARY_ANALYSIS_MIN_QUALITY
        ):
            return

        self.stationary_samples.append(
            {
                "point": measurement_result.point,
                "rmse_m": measurement_result.rmse_m,
                "quality": quality_score,
                "ranges_m": {
                    anchor_id: item["range_m"]
                    for anchor_id, item in filtered_items.items()
                },
            }
        )

        locked_pose = self._build_stationary_locked_pose()
        if locked_pose is None:
            return

        self.stationary_locked_pose = geom_state.project_point(*locked_pose)
        emit_log(
            f"node={self.node_id} stationary_analysis=locked "
            f"pose={format_pose(self.stationary_locked_pose)} "
            f"samples={len(self.stationary_samples)} "
            f"spread={self.stationary_analysis_spread_m}"
        )


class CentralCollector:
    # Hold per-node pipelines and the shared output history for the central process.
    def __init__(self) -> None:
        self.pipelines: Dict[str, TrackingPipeline] = {}
        self.history = []
        self.history_writer = HistoryWriter()

    # Return the existing node pipeline or create one on first sight of a node id.
    def get_pipeline(self, node_id: str) -> TrackingPipeline:
        pipeline = self.pipelines.get(node_id)
        if pipeline is None:
            pipeline = TrackingPipeline(node_id=node_id)
            self.pipelines[node_id] = pipeline
            default_note = ""
            if node_id not in ("rpi1", "rpi2", "rpi3", "rpi4") and pipeline.node_height_m == NODE_HEIGHT_DEFAULT_M:
                default_note = " default_height=1"
            emit_log(
                f"node={node_id} pipeline=created "
                f"height_m={pipeline.node_height_m:.3f}{default_note}"
            )
        return pipeline

    # Append a new history entry through the buffered history writer.
    def append_entry(self, entry: dict) -> None:
        self.history_writer.append(self.history, entry)

    # Flush buffered history entries to disk when shutting down or on demand.
    def flush(self, force: bool = False) -> None:
        self.history_writer.flush(self.history, force=force)


class MqttRawReceiver:
    # Create the MQTT client that receives raw node measurements for the central collector.
    def __init__(self, collector: CentralCollector) -> None:
        if mqtt is None:
            raise RuntimeError(
                "paho-mqtt is not installed. Install it with 'python -m pip install paho-mqtt'."
            )

        self.collector = collector
        try:
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id="central_pose_logger",
            )
        except AttributeError:
            self.client = mqtt.Client(client_id="central_pose_logger")

        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)

    # Subscribe to the raw measurement topic once the MQTT connection comes up.
    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        emit_log(f"mqtt=connected host={MQTT_HOST} port={MQTT_PORT} rc={reason_code}")
        client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
        emit_log(f"mqtt=subscribed topic={MQTT_TOPIC}")

    # Log when the MQTT client disconnects from the broker.
    def on_disconnect(self, client, userdata, disconnect_flags=None, reason_code=None, properties=None):
        emit_log(f"mqtt=disconnected rc={reason_code}")

    # Publish the same stabilized filtered pose used by history/dashboard for motor control.
    def publish_filtered_pose(self, entry: dict) -> None:
        node_id = str(entry.get("node_id") or "")
        pose = entry.get("pose") or {}
        quality = entry.get("quality") or {}
        if not node_id or pose.get("x_m") is None or pose.get("y_m") is None:
            return

        payload = {
            "node_id": node_id,
            "timestamp": entry.get("timestamp"),
            "sequence_number": entry.get("sequence_number"),
            "pose": {
                "x_m": pose.get("x_m"),
                "y_m": pose.get("y_m"),
            },
            "quality": {
                "score": quality.get("score"),
                "pose_mode": quality.get("pose_mode"),
                "filter_mode": quality.get("filter_mode"),
                "stationary_locked": quality.get("stationary_locked", False),
            },
        }
        topic = f"{AUTOCAM_POSE_TOPIC_BASE}/{node_id}"
        self.client.publish(
            topic,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=MQTT_QOS,
            retain=False,
        )
    # Decode one MQTT payload and push it through the per-node processing pipeline.
    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            node_id = str(payload["node_id"])
            timestamp = float(payload.get("timestamp", time.time()))
            sequence_number = payload.get("sequence_number")
            anchor_info = payload["anchors"]
            slot_token = payload.get("slot_token")

            pipeline = self.collector.get_pipeline(node_id)
            entry = pipeline.process_measurement(
                timestamp=timestamp,
                sequence_number=sequence_number,
                anchor_info=anchor_info,
                slot_token=str(slot_token) if slot_token is not None else None,
            )
            if entry is not None:
                self.collector.append_entry(entry)
                self.publish_filtered_pose(entry)
        except Exception as exc:
            emit_log(
                f"mqtt=message_error topic={msg.topic} "
                f"type={type(exc).__name__} detail={exc}"
            )

    # Connect to MQTT and keep processing messages until the client stops.
    def run_forever(self) -> None:
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        self.client.loop_forever()

    # Disconnect the MQTT client during central logger shutdown.
    def close(self) -> None:
        self.client.disconnect()


# Start the central pose logger and keep it running until shutdown or an unrecoverable error.
def main() -> None:
    reset_text_logs()
    collector = CentralCollector()
    receiver = MqttRawReceiver(collector)

    emit_log(
        f"central_pose_logger=start "
        f"mqtt_host={MQTT_HOST} mqtt_port={MQTT_PORT} topic={MQTT_TOPIC}"
    )
    emit_log(
        f"geometry=profile={geom_state.ANCHOR_PROFILE_NAME} "
        f"x[{geom_state.GEOM.min_x:.4f},{geom_state.GEOM.max_x:.4f}] "
        f"y[{geom_state.GEOM.min_y:.4f},{geom_state.GEOM.max_y:.4f}] "
        f"center=({geom_state.GEOM.center_x:.4f},{geom_state.GEOM.center_y:.4f}) "
        f"half=({geom_state.GEOM.half_x:.4f},{geom_state.GEOM.half_y:.4f})"
    )

    try:
        receiver.run_forever()
    except KeyboardInterrupt:
        collector.flush(force=True)
        receiver.close()
        emit_log("central_pose_logger=stopped")
        close_text_logs()
        return
    except RuntimeError as exc:
        collector.flush(force=True)
        emit_log(f"central_pose_logger runtime_error={exc}")
        close_text_logs()
        return
    except Exception as exc:
        collector.flush(force=True)
        emit_log(
            f"central_pose_logger unexpected_error={type(exc).__name__} detail={exc}"
        )
        close_text_logs()
        return
