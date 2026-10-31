"""TOML execution policy; explicit CLI config overrides task policy."""
from pathlib import Path
import tomllib


DEFAULTS = dict(schedule_policy="windowed", night_start_hour=1, night_end_hour=9,
                reset_lead_minutes=30, interval_seconds=60)


def load_config(path=None):
    values = {}
    if path is not None:
        with Path(path).open("rb") as stream:
            document = tomllib.load(stream)
        if set(document) - {"scheduler"}:
            raise ValueError("config supports only the [scheduler] table")
        values = document.get("scheduler", {})
        if not isinstance(values, dict) or set(values) - DEFAULTS.keys():
            raise ValueError("unknown scheduler configuration key")
    result = {**DEFAULTS, **values}
    if result["schedule_policy"] not in ("windowed", "continuous"):
        raise ValueError("schedule_policy must be windowed or continuous")
    for key, low, high in (("night_start_hour", 0, 23), ("night_end_hour", 0, 23),
                           ("reset_lead_minutes", 1, 300), ("interval_seconds", 1, 86400)):
        value = result[key]
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{key} must be an integer between {low} and {high}")
    if result["night_start_hour"] == result["night_end_hour"]:
        raise ValueError("night start and end hours must differ")
    return result
