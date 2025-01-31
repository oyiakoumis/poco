from datetime import datetime

import pytest
from pytz import timezone

from tools.resolve_temporal_reference import adjust_datetime_boundary, check_offset_keys, resolve_temporal_reference


# Test adjust_datetime_boundary
def test_adjust_datetime_boundary():
    dt = datetime(2023, 1, 15, 10, 30)

    assert adjust_datetime_boundary(dt, "start_of_year") == datetime(2023, 1, 1, 0, 0, 0)
    assert adjust_datetime_boundary(dt, "end_of_year") == datetime(2023, 12, 31, 23, 59, 59, 999999)
    assert adjust_datetime_boundary(dt, "start_of_month") == datetime(2023, 1, 1, 0, 0, 0)
    assert adjust_datetime_boundary(dt, "end_of_month") == datetime(2023, 1, 31, 23, 59, 59, 999999)
    assert adjust_datetime_boundary(dt, "start_of_day") == datetime(2023, 1, 15, 0, 0, 0)
    assert adjust_datetime_boundary(dt, "end_of_day") == datetime(2023, 1, 15, 23, 59, 59, 999999)
    assert adjust_datetime_boundary(dt, "start_of_week", 0) == datetime(2023, 1, 9, 0, 0, 0)
    assert adjust_datetime_boundary(dt, "end_of_week", 0) == datetime(2023, 1, 15, 23, 59, 59, 999999)
    assert adjust_datetime_boundary(dt, "start_of_week", 6) == datetime(2023, 1, 15, 0, 0, 0)
    assert adjust_datetime_boundary(dt, "end_of_week", 6) == datetime(2023, 1, 21, 23, 59, 59, 999999)

    with pytest.raises(ValueError):
        adjust_datetime_boundary(dt, "invalid_boundary")


# Test check_offset_keys
def test_check_offset_keys():
    valid_offsets = {"years": 1, "months": -1, "weeks": 2, "days": 10}
    invalid_offsets = {"invalid_key": 1}

    # No exception for valid offsets
    check_offset_keys(valid_offsets)

    # Raise exception for invalid offsets
    with pytest.raises(ValueError):
        check_offset_keys(invalid_offsets)


# Test resolve_temporal_reference
def test_resolve_temporal_reference():
    # Reference datetime and timezone setup
    tz = timezone("UTC")
    ref_datetime = tz.localize(datetime(2023, 1, 15, 12, 0))

    # Test 1: Last month range
    params = {
        "start_offsets": {"months": -1},
        "end_offsets": {"months": -1},
        "start_boundary": "start_of_month",
        "end_boundary": "end_of_month",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "UTC",
    }
    start, end = resolve_temporal_reference.invoke(params)
    assert start.isoformat() == "2022-12-01T00:00:00+00:00"
    assert end.isoformat() == "2022-12-31T23:59:59.999999+00:00"

    # Test 2: Next week range
    params = {
        "start_offsets": {"weeks": 1},
        "end_offsets": {"weeks": 1},
        "start_boundary": "start_of_week",
        "end_boundary": "end_of_week",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "UTC",
    }
    start, end = resolve_temporal_reference.invoke(params)
    assert start.isoformat() == "2023-01-16T00:00:00+00:00"
    assert end.isoformat() == "2023-01-22T23:59:59.999999+00:00"

    # Test 3: This year so far
    params = {
        "start_boundary": "start_of_year",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "UTC",
    }
    start, end = resolve_temporal_reference.invoke(params)
    assert start.isoformat() == "2023-01-01T00:00:00+00:00"
    assert end.isoformat() == ref_datetime.isoformat()

    # Test 4: Custom timezone and week start
    params = {
        "start_offsets": {"weeks": 0},
        "end_offsets": {"weeks": 0},
        "start_boundary": "start_of_week",
        "end_boundary": "end_of_week",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "US/Eastern",
        "first_day_of_week": 6,  # Sunday as the first day of the week
    }
    start, end = resolve_temporal_reference.invoke(params)
    eastern = timezone("US/Eastern")
    start_expected = eastern.localize(datetime(2023, 1, 15, 0, 0))  # Week starts on Saturday midnight
    end_expected = eastern.localize(datetime(2023, 1, 21, 23, 59, 59, 999999))  # Week ends Friday midnight
    assert start.isoformat() == start_expected.isoformat()
    assert end.isoformat() == end_expected.isoformat()

    # Test 5: Single day mode - "Today"
    params = {
        "start_offsets": {"days": 0},
        "end_offsets": {"days": 0},
        "start_boundary": "start_of_day",
        "end_boundary": "end_of_day",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "UTC",
        "single_day_mode": True,
    }
    single_date, end = resolve_temporal_reference.invoke(params)
    assert single_date == "2023-01-15"
    assert end is None

    # Test 6: Single day mode - "Tomorrow"
    params = {
        "start_offsets": {"days": 1},
        "end_offsets": {"days": 1},
        "start_boundary": "start_of_day",
        "end_boundary": "end_of_day",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "UTC",
        "single_day_mode": True,
    }
    single_date, end = resolve_temporal_reference.invoke(params)
    assert single_date == "2023-01-16"
    assert end is None

    # Test 7: Single day mode - "The day before yesterday"
    params = {
        "start_offsets": {"days": -2},
        "end_offsets": {"days": -2},
        "start_boundary": "start_of_day",
        "end_boundary": "end_of_day",
        "reference_datetime": ref_datetime.isoformat(),
        "timezone_str": "UTC",
        "single_day_mode": True,
    }
    single_date, end = resolve_temporal_reference.invoke(params)
    assert single_date == "2023-01-13"
    assert end is None
