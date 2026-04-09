import math
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from . import geometry as geom_state
from .config import (
    GEOMETRY_BIAS_MAX,
    GEOMETRY_BIAS_MIN,
    GEOMETRY_CENTER_RELAX_END,
    GEOMETRY_CENTER_RELAX_START,
    GEOMETRY_DISTANCE_SCALE_M,
    GEOMETRY_HORIZONTAL_BIAS,
    GEOMETRY_PROXIMITY_BIAS,
    GEOMETRY_VERTICAL_BIAS,
    HOLD_LAST_RANGE_S,
    INVALID_RANGE_HOLD_S,
    INVALID_RANGE_WEIGHT_FLOOR,
    MAX_DT_S,
    MIN_DT_S,
    MIN_VALID_DISTANCE_CM,
    OUTLIER_REJECT_MARGIN_M,
    OUTLIER_REJECT_RESIDUAL_M,
    OUTLIER_RETRY_IMPROVEMENT_M,
    QUALITY_MAX_RESID_SCALE_M,
    QUALITY_RMSE_SCALE_M,
    RANGE_ALPHA_FAST,
    RANGE_ALPHA_SLOW,
    RANGE_FAST_DELTA_M,
    RANGE_MAX_RATE_MPS,
    RANGE_MEDIAN_WINDOW,
    RANGE_STEP_BASE_M,
    SOLVER_DAMPING,
    SOLVER_HUBER_DELTA_M,
    SOLVER_MAX_BASE_WEIGHT,
    SOLVER_MAX_ITERS,
    SOLVER_STEP_LIMIT,
    STALE_WEIGHT_FLOOR,
    TOP_CENTER_ANCHOR_ID,
    TOP_CENTER_BIAS,
)
from .geometry import clamp, huber_weight, lerp, median


# Round a float for logs and JSON output while preserving `None`.
def round_float(value: Optional[float], digits: int = 4) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


# Convert a measured slant range into a 2D planar range using a known vertical offset.
def planar_range_from_slant(slant_range_m: float, vertical_delta_m: float) -> float:
    planar_sq_m = (slant_range_m * slant_range_m) - (vertical_delta_m * vertical_delta_m)
    return math.sqrt(max(planar_sq_m, 0.0))


# Format a 2D point as the history JSON structure used by the dashboard.
def point_dict(point: Optional[Tuple[float, float]]) -> Optional[dict]:
    if point is None:
        return None
    return {"x_m": round(point[0], 4), "y_m": round(point[1], 4)}


# Round each range measurement in an anchor-to-range map for output.
def rounded_range_map(range_map: Dict[str, float]) -> Dict[str, float]:
    return {anchor_id: round(range_m, 4) for anchor_id, range_m in range_map.items()}


# Round each numeric value in a generic mapping for output.
def rounded_value_map(value_map: Dict[str, float]) -> Dict[str, float]:
    return {key: round(float(value), 4) for key, value in value_map.items()}


# Reject missing or physically invalid distance readings from the UWB device.
def sanitize_distance_cm(distance_cm: Optional[int]) -> Optional[int]:
    if distance_cm is None:
        return None
    if distance_cm < MIN_VALID_DISTANCE_CM:
        return None
    return distance_cm


# Shrink small innovations to zero so jitter does not move the tracker.
def apply_radial_deadzone(
    dx: float,
    dy: float,
    deadzone_m: float,
) -> Tuple[float, float, float]:
    magnitude = math.hypot(dx, dy)
    if magnitude <= deadzone_m:
        return 0.0, 0.0, 0.0

    scale = (magnitude - deadzone_m) / max(magnitude, 1e-9)
    return dx * scale, dy * scale, magnitude * scale


# Compute a soft anchor weight bias based on the predicted pose and room geometry.
def geometry_bias_for_anchor(
    anchor_id: str,
    item: dict,
    seed_xy: Optional[Tuple[float, float]],
) -> float:
    if seed_xy is None:
        return 1.0

    sx, sy = geom_state.project_point(*seed_xy)
    x_norm = clamp((sx - geom_state.GEOM.center_x) / geom_state.GEOM.half_x, -1.0, 1.0)
    y_norm = clamp((sy - geom_state.GEOM.center_y) / geom_state.GEOM.half_y, -1.0, 1.0)
    ax_norm = clamp((item["x"] - geom_state.GEOM.center_x) / geom_state.GEOM.half_x, -1.0, 1.0)
    ay_norm = clamp((item["y"] - geom_state.GEOM.center_y) / geom_state.GEOM.half_y, -1.0, 1.0)

    distance_m = math.hypot(sx - item["x"], sy - item["y"])
    proximity = math.exp(-((distance_m / max(GEOMETRY_DISTANCE_SCALE_M, 1e-6)) ** 2))

    bias = 1.0
    if abs(ax_norm) > 1e-6:
        bias += GEOMETRY_HORIZONTAL_BIAS * x_norm * ax_norm
    bias += GEOMETRY_VERTICAL_BIAS * y_norm * ay_norm
    bias += GEOMETRY_PROXIMITY_BIAS * (2.0 * proximity - 1.0)

    if anchor_id == TOP_CENTER_ANCHOR_ID:
        bias += TOP_CENTER_BIAS * max(0.0, y_norm) * (1.0 - abs(x_norm))

    bias = clamp(bias, GEOMETRY_BIAS_MIN, GEOMETRY_BIAS_MAX)
    center_radius = math.hypot(x_norm, y_norm)
    relax_t = (
        (center_radius - GEOMETRY_CENTER_RELAX_START)
        / max(GEOMETRY_CENTER_RELAX_END - GEOMETRY_CENTER_RELAX_START, 1e-9)
    )
    relax_t = clamp(relax_t, 0.0, 1.0)
    return lerp(1.0, bias, relax_t)


# Apply the geometry-driven weight bias to every active range item.
def apply_geometry_bias(
    range_items: Dict[str, dict],
    seed_xy: Optional[Tuple[float, float]],
) -> Dict[str, dict]:
    biased_items = {}
    for anchor_id, item in range_items.items():
        geometry_bias = geometry_bias_for_anchor(anchor_id, item, seed_xy)
        biased_item = dict(item)
        biased_item["geometry_bias"] = geometry_bias
        biased_item["weight"] = clamp(
            item.get("weight", 1.0) * geometry_bias,
            0.05,
            GEOMETRY_BIAS_MAX,
        )
        biased_items[anchor_id] = biased_item
    return biased_items


@dataclass
class AnchorState:
    anchor_id: str
    x: float
    y: float
    vertical_delta_m: float = 0.0
    raw_window: deque = field(default_factory=lambda: deque(maxlen=RANGE_MEDIAN_WINDOW))
    raw_range_m: Optional[float] = None
    filtered_range_m: Optional[float] = None
    status: str = "MISSING"
    last_good_time: Optional[float] = None
    last_good_sequence: Optional[int] = None

    # Update one anchor's rolling range state from a fresh raw device reading.
    def update(
        self,
        status: str,
        distance_cm: Optional[int],
        timestamp: float,
        sequence_number: Optional[int],
    ) -> Optional[float]:
        distance_cm = sanitize_distance_cm(distance_cm)
        if status == "SUCCESS" and distance_cm is None:
            self.status = "INVALID_RANGE"
            self.raw_range_m = None
            return None

        self.status = status

        if status != "SUCCESS" or distance_cm is None:
            self.raw_range_m = None
            if (
                self.last_good_time is not None
                and (timestamp - self.last_good_time) > HOLD_LAST_RANGE_S
            ):
                self.raw_window.clear()
            return None

        slant_range_m = distance_cm / 100.0
        planar_range_m = planar_range_from_slant(slant_range_m, self.vertical_delta_m)
        self.raw_range_m = planar_range_m

        if (
            self.last_good_time is not None
            and (timestamp - self.last_good_time) > HOLD_LAST_RANGE_S
        ):
            self.raw_window.clear()

        self.raw_window.append(planar_range_m)
        smoothed_input = median(self.raw_window)
        if smoothed_input is None:
            return None

        if self.filtered_range_m is None or self.last_good_time is None:
            self.filtered_range_m = smoothed_input
        else:
            dt = clamp(timestamp - self.last_good_time, MIN_DT_S, MAX_DT_S)
            error = smoothed_input - self.filtered_range_m
            alpha = RANGE_ALPHA_FAST if abs(error) >= RANGE_FAST_DELTA_M else RANGE_ALPHA_SLOW
            max_step = RANGE_STEP_BASE_M + RANGE_MAX_RATE_MPS * dt
            limited_error = clamp(error, -max_step, max_step)
            self.filtered_range_m += alpha * limited_error

        self.last_good_time = timestamp
        self.last_good_sequence = sequence_number
        return planar_range_m

    # Return the best currently usable filtered measurement for this anchor.
    def filtered_measurement(self, timestamp: float) -> Optional[dict]:
        if self.filtered_range_m is None or self.last_good_time is None:
            return None

        age_s = max(0.0, timestamp - self.last_good_time)
        if self.status == "INVALID_RANGE":
            hold_limit_s = INVALID_RANGE_HOLD_S
            weight_floor = INVALID_RANGE_WEIGHT_FLOOR
        else:
            hold_limit_s = HOLD_LAST_RANGE_S
            weight_floor = STALE_WEIGHT_FLOOR

        if age_s > hold_limit_s:
            return None

        stale = self.status != "SUCCESS"
        if stale:
            freshness = 1.0 - clamp(age_s / hold_limit_s, 0.0, 1.0)
            weight = lerp(weight_floor, 1.0, freshness)
        else:
            weight = 1.0

        return {
            "anchor_id": self.anchor_id,
            "x": self.x,
            "y": self.y,
            "range_m": self.filtered_range_m,
            "weight": weight,
            "age_s": age_s,
            "stale": stale,
        }


@dataclass
class SolverResult:
    x: float
    y: float
    residuals_m: Dict[str, float]
    rmse_m: float
    max_residual_m: float
    anchors_used: int
    stale_used: int
    max_age_s: float
    dropped_anchor_id: Optional[str] = None

    @property
    # Expose the solved XY coordinates as a tuple for downstream consumers.
    def point(self) -> Tuple[float, float]:
        return (self.x, self.y)


# Choose a reasonable solver seed from the previous pose or a weighted anchor centroid.
def initial_guess(
    range_items: Dict[str, dict],
    fallback_xy: Optional[Tuple[float, float]],
) -> Tuple[float, float]:
    if fallback_xy is not None:
        return geom_state.project_point(*fallback_xy)

    total_weight = 0.0
    guess_x = 0.0
    guess_y = 0.0
    for item in range_items.values():
        weight = item.get("weight", 1.0) / max(item["range_m"], 0.15)
        guess_x += item["x"] * weight
        guess_y += item["y"] * weight
        total_weight += weight

    if total_weight <= 0.0:
        return (geom_state.GEOM.center_x, geom_state.GEOM.center_y)

    return geom_state.project_point(guess_x / total_weight, guess_y / total_weight)


# Solve for the bounded 2D pose using damped Gauss-Newton multilateration.
def solve_bounded_position(
    range_items: Dict[str, dict],
    fallback_xy: Optional[Tuple[float, float]],
) -> Optional[SolverResult]:
    if len(range_items) < 3:
        return None

    start_x, start_y = initial_guess(range_items, fallback_xy)
    u, v = geom_state.xy_to_uv(start_x, start_y)

    for _ in range(SOLVER_MAX_ITERS):
        tx = math.tanh(u)
        ty = math.tanh(v)
        x = geom_state.GEOM.center_x + geom_state.GEOM.half_x * tx
        y = geom_state.GEOM.center_y + geom_state.GEOM.half_y * ty

        dx_du = max(geom_state.GEOM.half_x * (1.0 - tx * tx), 1e-5)
        dy_dv = max(geom_state.GEOM.half_y * (1.0 - ty * ty), 1e-5)

        lhs00 = SOLVER_DAMPING
        lhs01 = 0.0
        lhs11 = SOLVER_DAMPING
        rhs0 = 0.0
        rhs1 = 0.0

        for item in range_items.values():
            dx = x - item["x"]
            dy = y - item["y"]
            predicted_range = max(math.hypot(dx, dy), 1e-6)
            residual = predicted_range - item["range_m"]

            base_weight = clamp(item.get("weight", 1.0), 0.05, SOLVER_MAX_BASE_WEIGHT)
            robust_weight = huber_weight(residual, SOLVER_HUBER_DELTA_M)
            sqrt_weight = math.sqrt(base_weight * robust_weight)

            j0 = sqrt_weight * (dx / predicted_range) * dx_du
            j1 = sqrt_weight * (dy / predicted_range) * dy_dv
            weighted_residual = sqrt_weight * residual

            lhs00 += j0 * j0
            lhs01 += j0 * j1
            lhs11 += j1 * j1
            rhs0 -= j0 * weighted_residual
            rhs1 -= j1 * weighted_residual

        det = lhs00 * lhs11 - lhs01 * lhs01
        if abs(det) > 1e-12:
            delta_u = (rhs0 * lhs11 - rhs1 * lhs01) / det
            delta_v = (lhs00 * rhs1 - lhs01 * rhs0) / det
        else:
            delta_u = rhs0 / max(lhs00, 1e-9)
            delta_v = rhs1 / max(lhs11, 1e-9)

        step_norm = math.hypot(delta_u, delta_v)
        if step_norm > SOLVER_STEP_LIMIT:
            scale = SOLVER_STEP_LIMIT / max(step_norm, 1e-9)
            delta_u *= scale
            delta_v *= scale

        u += delta_u
        v += delta_v

        if step_norm < 1e-4:
            break

    x, y = geom_state.uv_to_xy(u, v)
    residuals_m = {}
    stale_used = 0
    max_age_s = 0.0
    squared_error_sum = 0.0
    max_residual_m = 0.0

    for anchor_id, item in range_items.items():
        residual = math.hypot(x - item["x"], y - item["y"]) - item["range_m"]
        residuals_m[anchor_id] = residual
        squared_error_sum += residual * residual
        max_residual_m = max(max_residual_m, abs(residual))
        stale_used += int(bool(item.get("stale")))
        max_age_s = max(max_age_s, float(item.get("age_s", 0.0)))

    rmse_m = math.sqrt(squared_error_sum / len(range_items))
    return SolverResult(
        x=x,
        y=y,
        residuals_m=residuals_m,
        rmse_m=rmse_m,
        max_residual_m=max_residual_m,
        anchors_used=len(range_items),
        stale_used=stale_used,
        max_age_s=max_age_s,
    )


# Retry the solve once after dropping the worst outlier anchor when it clearly improves the fit.
def solve_with_outlier_retry(
    range_items: Dict[str, dict],
    fallback_xy: Optional[Tuple[float, float]],
) -> Optional[SolverResult]:
    result = solve_bounded_position(range_items, fallback_xy)
    if result is None or len(range_items) < 4:
        return result

    ranked = sorted(
        result.residuals_m.items(),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    worst_anchor_id, worst_residual = ranked[0]
    second_residual = abs(ranked[1][1]) if len(ranked) > 1 else 0.0

    if abs(worst_residual) < OUTLIER_REJECT_RESIDUAL_M:
        return result
    if abs(worst_residual) < second_residual + OUTLIER_REJECT_MARGIN_M:
        return result

    reduced_items = {
        anchor_id: item
        for anchor_id, item in range_items.items()
        if anchor_id != worst_anchor_id
    }
    retry = solve_bounded_position(reduced_items, result.point)
    if retry is None:
        return result

    if retry.rmse_m + OUTLIER_RETRY_IMPROVEMENT_M < result.rmse_m:
        retry.dropped_anchor_id = worst_anchor_id
        return retry

    return result


# Build the final quality score and diagnostics for a selected pose solution.
def build_quality(
    selected_result: Optional[SolverResult],
    filtered_result: Optional[SolverResult],
    raw_result: Optional[SolverResult],
    success_count: int,
) -> dict:
    if selected_result is None:
        return {
            "score": 0.0,
            "anchors_success": success_count,
            "anchors_used": 0,
            "stale_anchors_used": 0,
            "max_anchor_age_s": 0.0,
            "raw_rmse_m": round_float(raw_result.rmse_m) if raw_result else None,
            "selected_rmse_m": None,
            "selected_max_residual_m": None,
            "filtered_rmse_m": None,
            "filtered_max_residual_m": None,
            "dropped_anchor_id": None,
        }

    anchor_score = clamp((selected_result.anchors_used - 2) / 3.0, 0.0, 1.0)
    success_score = clamp((success_count - 2) / 3.0, 0.0, 1.0)
    stale_ratio = selected_result.stale_used / max(selected_result.anchors_used, 1)
    freshness_score = clamp(
        1.0 - 0.65 * stale_ratio - 0.35 * clamp(selected_result.max_age_s / HOLD_LAST_RANGE_S, 0.0, 1.0),
        0.0,
        1.0,
    )
    residual_score = math.exp(-((selected_result.rmse_m / QUALITY_RMSE_SCALE_M) ** 2))
    max_residual_score = math.exp(-((selected_result.max_residual_m / QUALITY_MAX_RESID_SCALE_M) ** 2))

    score = clamp(
        0.30 * anchor_score
        + 0.20 * success_score
        + 0.20 * freshness_score
        + 0.20 * residual_score
        + 0.10 * max_residual_score,
        0.0,
        1.0,
    )

    return {
        "score": round(score, 3),
        "anchors_success": success_count,
        "anchors_used": selected_result.anchors_used,
        "stale_anchors_used": selected_result.stale_used,
        "max_anchor_age_s": round_float(selected_result.max_age_s),
        "raw_rmse_m": round_float(raw_result.rmse_m) if raw_result else None,
        "selected_rmse_m": round_float(selected_result.rmse_m),
        "selected_max_residual_m": round_float(selected_result.max_residual_m),
        "filtered_rmse_m": round_float(filtered_result.rmse_m) if filtered_result else None,
        "filtered_max_residual_m": round_float(filtered_result.max_residual_m) if filtered_result else None,
        "dropped_anchor_id": selected_result.dropped_anchor_id,
    }


# Format a pose tuple as a compact log-friendly string.
def format_pose(point: Optional[Tuple[float, float]]) -> str:
    if point is None:
        return "(None,None)"
    return f"({point[0]:.3f},{point[1]:.3f})"
