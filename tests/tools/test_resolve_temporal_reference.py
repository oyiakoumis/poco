import calendar
from datetime import datetime
from typing import Dict, Optional, Tuple, Union

import pytz
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field
from pytz import timezone

from tools.resolve_temporal_reference import resolve_temporal_reference

# (Assume the resolve_temporal_reference code from above is defined here)
# For example, it includes the functions:
# - adjust_datetime_boundary
# - check_offset_keys
# - adjust_to_weekday
# - adjust_to_month
# - resolve_temporal_reference
# ... and the Pydantic model CalculateDateRangeArgs.
# (Copy the complete implementation provided earlier here.)

# ----------------------------
# Example Test Functions
# ----------------------------


def test_last_month_range():
    # Using a fixed reference datetime.
    ref_dt = datetime(2025, 2, 12, 15, 30, 0)
    params = {
        "start_offsets": {"months": -1},
        "end_offsets": {"months": -1},
        "start_boundary": "start_of_month",
        "end_boundary": "end_of_month",
        "reference_datetime": ref_dt,
        "timezone_str": "UTC",
    }
    start, end = resolve_temporal_reference.invoke(params)
    print("Test Last Month Range:")
    print("Start:", start)
    print("End:  ", end)
    # Expected: January 2025 from the 1st (start) to the 31st (end).
    expected_start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    expected_end = datetime(2025, 1, 31, 23, 59, 59, 999999, tzinfo=pytz.UTC)
    assert start == expected_start, f"Expected start {expected_start}, got {start}"
    assert end == expected_end, f"Expected end {expected_end}, got {end}"


def test_this_year_so_far():
    ref_dt = datetime(2025, 2, 12, 15, 30, 0)
    params = {
        "start_boundary": "start_of_year",
        "reference_datetime": ref_dt,
        "timezone_str": "UTC",
    }
    start, end = resolve_temporal_reference.invoke(params)
    print("\nTest This Year So Far:")
    print("Start:", start)
    print("End:  ", end)
    # Expected: Start is January 1, 2025; end is the reference datetime.
    expected_start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    # Since no end boundary was provided, end remains unchanged (but localized).
    expected_end = pytz.UTC.localize(ref_dt)
    assert start == expected_start, f"Expected start {expected_start}, got {start}"
    assert end == expected_end, f"Expected end {expected_end}, got {end}"


def test_day_before_yesterday():
    ref_dt = datetime(2025, 2, 12, 15, 30, 0)
    params = {
        "start_offsets": {"days": -2},
        "end_offsets": {"days": -2},
        "start_boundary": "start_of_day",
        "end_boundary": "end_of_day",
        "reference_datetime": ref_dt,
        "timezone_str": "UTC",
        "single_day_mode": True,
    }
    result = resolve_temporal_reference.invoke(params)
    print("\nTest Day Before Yesterday:")
    print("Result:", result)
    # Expected: Both start and end shift to the day before yesterday (2025-02-10).
    # In single_day_mode, a tuple (YYYY-MM-DD, None) is returned.
    assert result == ("2025-02-10", None), f"Expected ('2025-02-10', None), got {result}"


def test_last_tuesday():
    # Given reference datetime of Wednesday, February 12, 2025, 15:30 UTC,
    # "last Tuesday" should resolve to February 11, 2025.
    ref_dt = datetime(2025, 2, 12, 15, 30, 0)
    params = {
        "reference_datetime": ref_dt,
        "timezone_str": "UTC",
        "start_boundary": "start_of_day",
        "end_boundary": "end_of_day",
        "target_start_weekday": 1,  # 0=Monday, 1=Tuesday, etc.
        "weekday_modifier_start": "last",
        "target_end_weekday": 1,
        "weekday_modifier_end": "last",
        "single_day_mode": True,
    }
    result = resolve_temporal_reference.invoke(params)
    print("\nTest Last Tuesday:")
    print("Result:", result)
    # Expected: "2025-02-11" since last Tuesday relative to Wednesday is the previous day.
    assert result == ("2025-02-11", None), f"Expected ('2025-02-11', None), got {result}"


def test_next_february():
    # With the reference datetime in February 2025, "next February" should resolve to February 2026.
    ref_dt = datetime(2025, 2, 12, 15, 30, 0)
    params = {
        "reference_datetime": ref_dt,
        "timezone_str": "UTC",
        "target_start_month": 2,  # February
        "month_modifier_start": "next",  # Next occurrence of February
        "target_end_month": 2,
        "month_modifier_end": "next",
        "start_boundary": "start_of_month",
        "end_boundary": "end_of_month",
    }
    start, end = resolve_temporal_reference.invoke(params)
    print("\nTest Next February:")
    print("Start:", start)
    print("End:  ", end)
    # Expected: Next February (2026) with start at Feb 1, 2026 and end at Feb 28, 2026.
    expected_start = datetime(2026, 2, 1, 0, 0, 0, tzinfo=pytz.UTC)
    expected_end = datetime(2026, 2, 28, 23, 59, 59, 999999, tzinfo=pytz.UTC)
    assert start == expected_start, f"Expected start {expected_start}, got {start}"
    assert end == expected_end, f"Expected end {expected_end}, got {end}"
