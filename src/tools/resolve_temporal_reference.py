import calendar
from datetime import datetime
from typing import Dict, Optional, Tuple, Union

import pytz
from dateutil.relativedelta import relativedelta
from pytz import timezone


def adjust_datetime_boundary(dt: datetime, boundary: Optional[str], first_day_of_week: int = 0) -> datetime:
    """
    Adjust a datetime to a specified boundary.

    Supported boundaries:
      - "start_of_year", "end_of_year"
      - "start_of_month", "end_of_month"
      - "start_of_day", "end_of_day"
      - "start_of_week", "end_of_week"

    If boundary is None, returns dt unchanged.
    """
    if not boundary:
        return dt

    b = boundary.lower()
    if b == "start_of_year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if b == "end_of_year":
        return dt.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
    if b == "start_of_month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if b == "end_of_month":
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)
    if b == "start_of_day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if b == "end_of_day":
        return dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    if b == "start_of_week":
        days_to_subtract = (dt.weekday() - first_day_of_week) % 7
        return (dt - relativedelta(days=days_to_subtract)).replace(hour=0, minute=0, second=0, microsecond=0)
    if b == "end_of_week":
        days_to_add = 6 - (dt.weekday() - first_day_of_week) % 7
        return (dt + relativedelta(days=days_to_add)).replace(hour=23, minute=59, second=59, microsecond=999999)
    raise ValueError(f"Unknown boundary: {boundary}")


def adjust_to_weekday(dt: datetime, target: int, modifier: str = "this") -> datetime:
    """
    Snap dt to a target weekday.

    modifier:
      - "last": move to the most recent target weekday strictly before dt.
      - "next": move to the first target weekday strictly after dt.
      - "this": adjust within the current week.
    """
    current = dt.weekday()
    if modifier == "last":
        diff = current - target
        if diff <= 0:
            diff += 7
        return dt - relativedelta(days=diff)
    if modifier == "next":
        diff = target - current
        if diff <= 0:
            diff += 7
        return dt + relativedelta(days=diff)
    if modifier == "this":
        diff = target - current
        return dt + relativedelta(days=diff)
    raise ValueError("Modifier must be 'last', 'this', or 'next'")


def adjust_to_month(dt: datetime, target: int, modifier: str = "this") -> datetime:
    """
    Snap dt to a target month.

    modifier:
      - "next": if dt.month is target, move to target in the next year.
      - "last": if dt.month is target, move to target in the previous year.
      - "this": use the same year.
    """
    year = dt.year
    if modifier == "next" and dt.month >= target:
        year += 1
    if modifier == "last" and dt.month <= target:
        year -= 1

    last_day = calendar.monthrange(year, target)[1]
    day = min(dt.day, last_day)
    return dt.replace(year=year, month=target, day=day)


def resolve_temporal_reference(
    reference: Optional[datetime] = None,
    start: Optional[Dict] = None,
    end: Optional[Dict] = None,
    single_day_mode: bool = False,
    tz: str = "UTC",
    first_day_of_week: int = 0,
) -> Tuple[Union[datetime, str], Optional[datetime]]:
    """
    Compute a date (or date range) based on a reference datetime and separate configuration
    dictionaries for the start and end.

    Each configuration dictionary may include:
      - "offset": a dict of offsets (passed to relativedelta)
      - "boundary": a string like "start_of_month", "end_of_day", etc.
      - "snap": a dict for snapping the date. For example:
          {"type": "weekday", "target": 1, "modifier": "last"}
          {"type": "month", "target": 2, "modifier": "next"}

    If no configuration is provided, the value defaults to the reference datetime.
    If single_day_mode is True, verifies that start and end fall on the same calendar day and
    returns a tuple of (date_str, None).
    """
    tz_obj = timezone(tz)
    # Localize the reference datetime (or use now)
    if reference:
        ref = reference if reference.tzinfo else tz_obj.localize(reference)
        ref = ref.astimezone(tz_obj)
    else:
        ref = datetime.now(tz_obj)

    # Default configurations to empty dictionaries.
    start_cfg = start or {}
    end_cfg = end or {}

    # Compute the raw dates with offsets.
    start_dt = ref + relativedelta(**start_cfg.get("offset", {}))
    end_dt = ref + relativedelta(**end_cfg.get("offset", {}))

    # Apply boundary adjustments.
    if "boundary" in start_cfg:
        start_dt = adjust_datetime_boundary(start_dt, start_cfg["boundary"], first_day_of_week)
    if "boundary" in end_cfg:
        end_dt = adjust_datetime_boundary(end_dt, end_cfg["boundary"], first_day_of_week)

    # Apply "snap" adjustments if provided.
    if "snap" in start_cfg:
        snap = start_cfg["snap"]
        modifier = snap.get("modifier", "this")
        if snap["type"] == "weekday":
            start_dt = adjust_to_weekday(start_dt, snap["target"], modifier)
        elif snap["type"] == "month":
            start_dt = adjust_to_month(start_dt, snap["target"], modifier)
        else:
            raise ValueError("Unknown snap type. Use 'weekday' or 'month'.")
    if "snap" in end_cfg:
        snap = end_cfg["snap"]
        modifier = snap.get("modifier", "this")
        if snap["type"] == "weekday":
            end_dt = adjust_to_weekday(end_dt, snap["target"], modifier)
        elif snap["type"] == "month":
            end_dt = adjust_to_month(end_dt, snap["target"], modifier)
        else:
            raise ValueError("Unknown snap type. Use 'weekday' or 'month'.")

    if single_day_mode:
        if start_dt.date() != end_dt.date():
            raise ValueError("single_day_mode is True, but start and end dates differ.")
        return (start_dt.strftime("%Y-%m-%d"), None)

    return (start_dt, end_dt)


# ---------------------------
# Examples of Additional Temporal Expressions
# ---------------------------
if __name__ == "__main__":
    # We'll use a fixed reference datetime for testing:
    ref = datetime(2025, 2, 2, 15, 30, 0)  # This is a Wednesday.
    tz = "UTC"

    # 1. Tomorrow (single-day mode)
    params_tomorrow = {
        "start": {"offset": {"days": 1}, "boundary": "start_of_day"},
        "end": {"offset": {"days": 1}, "boundary": "end_of_day"},
    }
    tomorrow = resolve_temporal_reference(reference=ref, **params_tomorrow, tz=tz, single_day_mode=True)
    print("Tomorrow:", tomorrow)

    # 2. Next Week Range
    params_next_week = {
        "start": {"offset": {"weeks": 1}, "boundary": "start_of_week"},
        "end": {"offset": {"weeks": 1}, "boundary": "end_of_week"},
    }
    next_week_start, next_week_end = resolve_temporal_reference(reference=ref, **params_next_week, tz=tz)
    print("\nNext Week Range:")
    print("  Start:", next_week_start)
    print("  End:  ", next_week_end)

    # 3. Last Week Range
    params_last_week = {
        "start": {"offset": {"weeks": -1}, "boundary": "start_of_week"},
        "end": {"offset": {"weeks": -1}, "boundary": "end_of_week"},
    }
    last_week_start, last_week_end = resolve_temporal_reference(reference=ref, **params_last_week, tz=tz)
    print("\nLast Week Range:")
    print("  Start:", last_week_start)
    print("  End:  ", last_week_end)

    # 4. Next Year Range
    params_next_year = {
        "start": {"offset": {"years": 1}, "boundary": "start_of_year"},
        "end": {"offset": {"years": 1}, "boundary": "end_of_year"},
    }
    next_year_start, next_year_end = resolve_temporal_reference(reference=ref, **params_next_year, tz=tz)
    print("\nNext Year Range:")
    print("  Start:", next_year_start)
    print("  End:  ", next_year_end)

    # 5. Last Year Range
    params_last_year = {
        "start": {"offset": {"years": -1}, "boundary": "start_of_year"},
        "end": {"offset": {"years": -1}, "boundary": "end_of_year"},
    }
    last_year_start, last_year_end = resolve_temporal_reference(reference=ref, **params_last_year, tz=tz)
    print("\nLast Year Range:")
    print("  Start:", last_year_start)
    print("  End:  ", last_year_end)

    # 6. In 3 Days (single-day mode)
    params_in3days = {
        "start": {"offset": {"days": 3}, "boundary": "start_of_day"},
        "end": {"offset": {"days": 3}, "boundary": "end_of_day"},
    }
    in3days = resolve_temporal_reference(reference=ref, **params_in3days, tz=tz, single_day_mode=True)
    print("\nIn 3 Days:", in3days)

    # 7. This Weekend
    # Assuming a week starting on Monday, snap to Saturday (target 5) and Sunday (target 6)
    params_this_weekend = {
        "start": {"boundary": "start_of_day", "snap": {"type": "weekday", "target": 5, "modifier": "this"}},
        "end": {"boundary": "end_of_day", "snap": {"type": "weekday", "target": 6, "modifier": "this"}},
    }
    weekend_start, weekend_end = resolve_temporal_reference(reference=ref, **params_this_weekend, tz=tz)
    print("\nThis Weekend:")
    print("  Start:", weekend_start)
    print("  End:  ", weekend_end)
