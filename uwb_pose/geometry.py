import json
import math
import os
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .config import (
    ANCHORS_PATH,
    DEFAULT_ANCHOR_PROFILES,
    DEFAULT_PROFILE_NAME,
    EDGE_SOFT_MARGIN_M,
)


@dataclass(frozen=True)
class Geometry:
    min_x: float
    max_x: float
    min_y: float
    max_y: float
    center_x: float
    center_y: float
    half_x: float
    half_y: float
    hull: Tuple[Tuple[float, float], ...]


# Clamp a numeric value into the closed interval between `lo` and `hi`.
def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# Linearly interpolate from `a` to `b` using a clamped interpolation factor.
def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * clamp(t, 0.0, 1.0)


# Return the median of a collection of values or `None` when it is empty.
def median(values) -> Optional[float]:
    vals = sorted(values)
    n = len(vals)
    if n == 0:
        return None
    if n % 2 == 1:
        return float(vals[n // 2])
    return 0.5 * (vals[n // 2 - 1] + vals[n // 2])


# Return the mean after trimming a fraction of the smallest and largest values.
def trimmed_mean(values, trim_ratio: float = 0.15) -> Optional[float]:
    vals = sorted(float(v) for v in values)
    if not vals:
        return None
    trim_n = int(len(vals) * trim_ratio)
    if (2 * trim_n) >= len(vals):
        trim_n = max((len(vals) - 1) // 2, 0)
    trimmed = vals[trim_n : len(vals) - trim_n] if trim_n > 0 else vals
    if not trimmed:
        return None
    return sum(trimmed) / len(trimmed)


# Down-weight large residuals using a Huber-style robust weighting function.
def huber_weight(residual: float, delta: float) -> float:
    abs_residual = abs(residual)
    if abs_residual <= delta:
        return 1.0
    return delta / max(abs_residual, 1e-9)


# Compute the signed 2D cross product used by convex hull construction.
def cross(o, a, b) -> float:
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


# Build the convex hull of a set of 2D points using the monotonic chain algorithm.
def convex_hull(points) -> Tuple[Tuple[float, float], ...]:
    unique = sorted({(float(x), float(y)) for x, y in points})
    if len(unique) <= 1:
        return tuple(unique)

    lower = []
    for point in unique:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0.0:
            lower.pop()
        lower.append(point)

    upper = []
    for point in reversed(unique):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0.0:
            upper.pop()
        upper.append(point)

    return tuple(lower[:-1] + upper[:-1])


# Compute the centroid of a polygon, with sensible fallbacks for degenerate cases.
def polygon_centroid(points: Tuple[Tuple[float, float], ...]) -> Tuple[float, float]:
    if not points:
        return (0.0, 0.0)
    if len(points) == 1:
        return points[0]
    if len(points) == 2:
        return (
            (points[0][0] + points[1][0]) * 0.5,
            (points[0][1] + points[1][1]) * 0.5,
        )

    area2 = 0.0
    cx = 0.0
    cy = 0.0
    for i, (x1, y1) in enumerate(points):
        x2, y2 = points[(i + 1) % len(points)]
        cross_term = x1 * y2 - x2 * y1
        area2 += cross_term
        cx += (x1 + x2) * cross_term
        cy += (y1 + y2) * cross_term

    if abs(area2) < 1e-9:
        avg_x = sum(x for x, _ in points) / len(points)
        avg_y = sum(y for _, y in points) / len(points)
        return (avg_x, avg_y)

    scale = 1.0 / (3.0 * area2)
    return (cx * scale, cy * scale)


# Return whether a point lies on the line segment from A to B within a small tolerance.
def point_on_segment(
    px: float,
    py: float,
    ax: float,
    ay: float,
    bx: float,
    by: float,
    eps: float = 1e-9,
) -> bool:
    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    cross_val = abx * apy - aby * apx
    if abs(cross_val) > eps:
        return False

    dot = apx * abx + apy * aby
    if dot < -eps:
        return False

    length_sq = abx * abx + aby * aby
    if dot - length_sq > eps:
        return False

    return True


# Return whether a point lies inside or on the boundary of a polygon.
def point_in_polygon(
    x: float,
    y: float,
    polygon: Tuple[Tuple[float, float], ...],
) -> bool:
    if not polygon:
        return False
    if len(polygon) == 1:
        return math.hypot(x - polygon[0][0], y - polygon[0][1]) <= 1e-9
    if len(polygon) == 2:
        return point_on_segment(
            x,
            y,
            polygon[0][0],
            polygon[0][1],
            polygon[1][0],
            polygon[1][1],
        )

    inside = False
    for i, (x1, y1) in enumerate(polygon):
        x2, y2 = polygon[(i + 1) % len(polygon)]
        if point_on_segment(x, y, x1, y1, x2, y2):
            return True

        intersects = ((y1 > y) != (y2 > y)) and (
            x < (x2 - x1) * (y - y1) / (y2 - y1) + x1
        )
        if intersects:
            inside = not inside

    return inside


# Return the closest point on a line segment to the provided 2D point.
def closest_point_on_segment(
    px: float,
    py: float,
    ax: float,
    ay: float,
    bx: float,
    by: float,
) -> Tuple[float, float]:
    abx = bx - ax
    aby = by - ay
    length_sq = abx * abx + aby * aby
    if length_sq <= 1e-12:
        return (ax, ay)

    t = clamp(((px - ax) * abx + (py - ay) * aby) / length_sq, 0.0, 1.0)
    return (ax + t * abx, ay + t * aby)


# Return the closest point on a polygon boundary to the provided 2D point.
def closest_point_on_polygon(
    x: float,
    y: float,
    polygon: Tuple[Tuple[float, float], ...],
) -> Tuple[float, float]:
    if not polygon:
        return (x, y)
    if len(polygon) == 1:
        return polygon[0]
    if len(polygon) == 2:
        return closest_point_on_segment(
            x,
            y,
            polygon[0][0],
            polygon[0][1],
            polygon[1][0],
            polygon[1][1],
        )

    best_point = polygon[0]
    best_dist_sq = float("inf")
    for i, (ax, ay) in enumerate(polygon):
        bx, by = polygon[(i + 1) % len(polygon)]
        cx, cy = closest_point_on_segment(x, y, ax, ay, bx, by)
        dist_sq = (cx - x) * (cx - x) + (cy - y) * (cy - y)
        if dist_sq < best_dist_sq:
            best_dist_sq = dist_sq
            best_point = (cx, cy)
    return best_point


# Parse a value as a finite float and return `None` for anything invalid.
def numeric_or_none(value) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


# Deep-copy an anchor map while normalizing coordinates to floats.
def clone_anchor_map(anchor_map: Dict[str, dict]) -> Dict[str, dict]:
    return {
        anchor_id: {"x": float(item["x"]), "y": float(item["y"])}
        for anchor_id, item in anchor_map.items()
    }


# Normalize an anchor map against a fallback profile and repair invalid coordinates.
def canonicalize_anchor_map(
    anchor_map,
    fallback_map: Dict[str, dict],
) -> Dict[str, dict]:
    anchors = {}
    for anchor_id, default in fallback_map.items():
        item = anchor_map.get(anchor_id, default) if isinstance(anchor_map, dict) else default
        x = numeric_or_none(item.get("x") if isinstance(item, dict) else None)
        y = numeric_or_none(item.get("y") if isinstance(item, dict) else None)
        if x is None or y is None:
            x = default["x"]
            y = default["y"]
        anchors[anchor_id] = {"x": x, "y": y}
    return anchors


# Load the active anchor layout from disk or fall back to the built-in defaults.
def load_anchor_config() -> Tuple[str, Dict[str, dict]]:
    default_map = clone_anchor_map(DEFAULT_ANCHOR_PROFILES[DEFAULT_PROFILE_NAME])

    if not os.path.exists(ANCHORS_PATH):
        return DEFAULT_PROFILE_NAME, default_map

    try:
        with open(ANCHORS_PATH, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return DEFAULT_PROFILE_NAME, default_map

    if isinstance(loaded, dict) and "1" in loaded:
        return DEFAULT_PROFILE_NAME, canonicalize_anchor_map(loaded, default_map)

    profiles = loaded.get("profiles") if isinstance(loaded, dict) else None
    active_name = loaded.get("active") if isinstance(loaded, dict) else None
    active_name = (
        active_name
        if isinstance(active_name, str) and active_name
        else DEFAULT_PROFILE_NAME
    )

    if not isinstance(profiles, dict) or not profiles:
        return DEFAULT_PROFILE_NAME, default_map

    selected = profiles.get(active_name)
    if not isinstance(selected, dict):
        fallback_name = next(iter(DEFAULT_ANCHOR_PROFILES.keys()))
        selected = profiles.get(fallback_name) or next(
            (profile for profile in profiles.values() if isinstance(profile, dict)),
            default_map,
        )
        active_name = fallback_name if isinstance(selected, dict) else DEFAULT_PROFILE_NAME

    fallback_map = DEFAULT_ANCHOR_PROFILES.get(active_name, default_map)
    return active_name, canonicalize_anchor_map(selected, fallback_map)


# Load the active anchor layout and include the config file modification time when available.
def load_anchor_config_with_mtime() -> Tuple[str, Dict[str, dict], Optional[float]]:
    profile_name, anchors = load_anchor_config()
    try:
        mtime = float(os.stat(ANCHORS_PATH).st_mtime_ns)
    except OSError:
        mtime = None
    return profile_name, anchors, mtime


# Build the derived room geometry bounds and hull from the current anchor map.
def build_geometry(anchors: Dict[str, dict]) -> Geometry:
    xs = [float(item["x"]) for item in anchors.values()]
    ys = [float(item["y"]) for item in anchors.values()]
    hull = convex_hull(zip(xs, ys))

    min_x = min(xs)
    max_x = max(xs)
    min_y = min(ys)
    max_y = max(ys)

    center_x, center_y = polygon_centroid(hull)
    half_x = max(max(max_x - center_x, center_x - min_x), 1e-6)
    half_y = max(max(max_y - center_y, center_y - min_y), 1e-6)

    return Geometry(
        min_x=min_x,
        max_x=max_x,
        min_y=min_y,
        max_y=max_y,
        center_x=center_x,
        center_y=center_y,
        half_x=half_x,
        half_y=half_y,
        hull=hull,
    )


ANCHOR_PROFILE_NAME, ANCHORS, ANCHORS_MTIME = load_anchor_config_with_mtime()
GEOM = build_geometry(ANCHORS)
ANCHOR_IDS = sorted(ANCHORS, key=int)


# Reload the anchor config and derived geometry when the config file changes.
def refresh_anchor_config_if_needed(force: bool = False) -> bool:
    global ANCHOR_PROFILE_NAME, ANCHORS, ANCHORS_MTIME, GEOM, ANCHOR_IDS

    try:
        current_mtime = float(os.stat(ANCHORS_PATH).st_mtime_ns)
    except OSError:
        current_mtime = None

    if not force and current_mtime == ANCHORS_MTIME:
        return False

    ANCHOR_PROFILE_NAME, ANCHORS, ANCHORS_MTIME = load_anchor_config_with_mtime()
    GEOM = build_geometry(ANCHORS)
    ANCHOR_IDS = sorted(ANCHORS, key=int)
    return True


# Softly compress an axis toward the valid range instead of hard-clipping immediately.
def soft_limit_axis(value: float, lo: float, hi: float, margin: float) -> float:
    if margin <= 0.0:
        return clamp(value, lo, hi)

    if lo > hi:
        lo, hi = hi, lo

    inner_lo = min(lo + margin, hi)
    inner_hi = max(hi - margin, lo)

    if value < inner_lo:
        overflow = inner_lo - value
        softened = inner_lo - margin * math.tanh(overflow / max(margin, 1e-6))
        return max(lo, softened)

    if value > inner_hi:
        overflow = value - inner_hi
        softened = inner_hi + margin * math.tanh(overflow / max(margin, 1e-6))
        return min(hi, softened)

    return value


# Project a point directly onto the valid room hull if it falls outside.
def project_point(x: float, y: float) -> Tuple[float, float]:
    if point_in_polygon(x, y, GEOM.hull):
        return (x, y)
    return closest_point_on_polygon(x, y, GEOM.hull)


# Soft-limit a point to the room bounds before projecting it onto the valid hull.
def soft_project_point(x: float, y: float) -> Tuple[float, float]:
    sx = soft_limit_axis(x, GEOM.min_x, GEOM.max_x, EDGE_SOFT_MARGIN_M)
    sy = soft_limit_axis(y, GEOM.min_y, GEOM.max_y, EDGE_SOFT_MARGIN_M)
    return project_point(sx, sy)


# Compute a numerically safe inverse hyperbolic tangent near the domain edges.
def safe_atanh(value: float) -> float:
    return math.atanh(clamp(value, -0.999999, 0.999999))


# Convert XY room coordinates into the bounded solver space.
def xy_to_uv(x: float, y: float) -> Tuple[float, float]:
    px, py = project_point(x, y)
    nx = (px - GEOM.center_x) / GEOM.half_x
    ny = (py - GEOM.center_y) / GEOM.half_y
    return safe_atanh(nx), safe_atanh(ny)


# Convert bounded solver coordinates back into projected room coordinates.
def uv_to_xy(u: float, v: float) -> Tuple[float, float]:
    x = GEOM.center_x + GEOM.half_x * math.tanh(u)
    y = GEOM.center_y + GEOM.half_y * math.tanh(v)
    return project_point(x, y)
