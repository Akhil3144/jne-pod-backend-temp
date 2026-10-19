"""Independent scan/photo evidence validation. No destination or photo AI logic."""
import math

DEFAULT_POLICY = {"allowedDeliveryRadiusMeters": 250.0, "maxLocationAgeMillis": 30000, "maxAccuracyMeters": 50.0}
RESCAN_REASON = "Photo location is more than {radius:g} m from the package scan location. Please scan the package again at the delivery location and take a new photo."


def number(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def validate_scan(scan, awb):
    if not isinstance(scan, dict) or not isinstance(scan.get("scanAttemptId"), str) or not scan["scanAttemptId"].strip():
        raise ValueError("scanAttemptId is required")
    if scan.get("waybill") != awb:
        raise ValueError("Scan AWB does not match photo AWB")
    if not isinstance(scan.get("scannedAtEpochMillis"), int) or isinstance(scan["scannedAtEpochMillis"], bool) or scan["scannedAtEpochMillis"] <= 0:
        raise ValueError("Invalid scan timestamp")
    if scan.get("inputSource") not in ("BARCODE", "MANUAL_TEST", "DELIVERY_STATUS"):
        raise ValueError("Invalid scan inputSource")
    validate_point(scan.get("location"))


def validate_point(point):
    if point is None:
        return
    if not isinstance(point, dict):
        raise ValueError("Invalid location object")
    lat, lon = point.get("latitude"), point.get("longitude")
    if not number(lat) or not number(lon) or not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("Invalid GPS coordinate range")
    accuracy = point.get("accuracyMeters")
    if accuracy is not None and (not number(accuracy) or accuracy < 0):
        raise ValueError("Invalid GPS accuracy")
    if not isinstance(point.get("timestampEpochMillis"), int) or isinstance(point["timestampEpochMillis"], bool):
        raise ValueError("Location timestamp required")
    if not isinstance(point.get("isMock"), bool) or not isinstance(point.get("source"), str) or not point["source"]:
        raise ValueError("Location integrity metadata required")


def evaluate(scan, photo, photo_at, policy=DEFAULT_POLICY):
    a = scan.get("location") if scan else None
    if a is None or photo is None:
        return "GPS_UNAVAILABLE", "Scan or photo GPS missing", None
    if any(p["isMock"] for p in (a, photo)):
        return "GPS_NEEDS_REVIEW", "Mock location indicator", None
    if any(p["timestampEpochMillis"] <= 0 or abs(at - p["timestampEpochMillis"]) > policy["maxLocationAgeMillis"]
           for p, at in ((a, scan["scannedAtEpochMillis"]), (photo, photo_at))):
        return "GPS_RETRY_REQUIRED", "Stale GPS reading", None
    if any(p.get("accuracyMeters") is None or p["accuracyMeters"] > policy["maxAccuracyMeters"] for p in (a, photo)):
        return "GPS_NEEDS_REVIEW", "GPS accuracy insufficient", None
    lat1, lat2 = map(math.radians, (a["latitude"], photo["latitude"]))
    dlat = lat2 - lat1
    dlon = math.radians(photo["longitude"] - a["longitude"])
    h = min(1., max(0., math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2))
    distance = 6371000 * 2 * math.atan2(math.sqrt(h), math.sqrt(1-h))
    radius = policy["allowedDeliveryRadiusMeters"]
    uncertainty = a["accuracyMeters"] + photo["accuracyMeters"]
    if uncertainty > 0 and distance - uncertainty <= radius and distance + uncertainty > radius:
        return "GPS_NEEDS_REVIEW", "Accuracy overlaps scan radius boundary", distance
    if distance <= radius + 1e-7:
        return "GPS_PASS", "Scan-to-Photo GPS Verified", distance
    return "GPS_FAIL_OUTSIDE_SCAN_RADIUS", RESCAN_REASON.format(radius=radius), distance
