import calendar
from datetime import datetime
from typing import Dict, Optional, Tuple, Union

import pytz
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field
from pytz import timezone

from langchain_core.tools import tool


def adjust_datetime_boundary(dt: datetime, boundary_type: Optional[str], first_day_of_week: int = 0) -> datetime:
    """
    Adjust a datetime object to a specific boundary.

    Supported boundary types:
      - "start_of_year", "end_of_year"
      - "start_of_month", "end_of_month"
      - "start_of_day", "end_of_day"
      - "start_of_week", "end_of_week"

    Parameters:
        dt: The datetime object to adjust.
        boundary_type: The boundary type to adjust to. If None or empty, returns the original datetime.
        first_day_of_week: The starting day of the week (0 = Monday, 6 = Sunday).

    Returns:
        The datetime object adjusted to the specified boundary.

    Raises:
        ValueError: If the boundary_type is not recognized.
    """
    if not boundary_type:
        return dt

    boundary_type = boundary_type.lower()

    if boundary_type == "start_of_year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif boundary_type == "end_of_year":
        return dt.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
    elif boundary_type == "start_of_month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif boundary_type == "end_of_month":
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)
    elif boundary_type == "start_of_day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif boundary_type == "end_of_day":
        return dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif boundary_type == "start_of_week":
        # Calculate the number of days to subtract to reach the first day of the week.
        days_to_subtract = (dt.weekday() - first_day_of_week) % 7
        adjusted_date = dt - relativedelta(days=days_to_subtract)
        return adjusted_date.replace(hour=0, minute=0, second=0, microsecond=0)
    elif boundary_type == "end_of_week":
        # Calculate the number of days to add to reach the last day of the week.
        days_to_add = 6 - (dt.weekday() - first_day_of_week) % 7
        adjusted_date = dt + relativedelta(days=days_to_add)
        return adjusted_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        raise ValueError(f"Unrecognized boundary_type: {boundary_type}")


def check_offset_keys(offsets: Optional[Dict[str, int]]) -> None:
    """
    Validate the keys in an offsets dictionary.

    Parameters:
        offsets: A dictionary containing offset keys such as 'years', 'months', etc.
                 If None, no validation is performed.

    Raises:
        ValueError: If any offset key is invalid.
    """
    valid_keys = {"years", "months", "weeks", "days", "hours", "minutes", "seconds"}
    if offsets:
        invalid_keys = set(offsets.keys()) - valid_keys
        if invalid_keys:
            raise ValueError(f"Invalid offset keys: {invalid_keys}")


class CalculateDateRangeArgs(BaseModel):
    start_offsets: Optional[Dict[str, int]] = Field(None, description="Offsets for adjusting the start datetime. Example: {'months': -1}")
    end_offsets: Optional[Dict[str, int]] = Field(None, description="Offsets for adjusting the end datetime. Example: {'weeks': 1}")
    reference_offsets: Optional[Dict[str, int]] = Field(None, description="Offsets for adjusting the reference datetime. Example: {'years': -1}")
    start_boundary: Optional[str] = Field(None, description="Boundary type for the start datetime. Example: 'start_of_month'")
    end_boundary: Optional[str] = Field(None, description="Boundary type for the end datetime. Example: 'end_of_week'")
    reference_datetime: Optional[datetime] = Field(None, description="The base datetime for calculations. Defaults to the current datetime if not provided.")
    timezone_str: Optional[str] = Field("UTC", description="Timezone for localization. Defaults to 'UTC'.")
    first_day_of_week: Optional[int] = Field(0, description="The starting day of the week (0 = Monday, 6 = Sunday). Defaults to 0.")
    single_day_mode: bool = Field(False, description="If true, return only a single date (YYYY-MM-DD) and None for the end date.")


@tool(args_schema=CalculateDateRangeArgs)
def resolve_temporal_reference(
    start_offsets: Optional[Dict[str, int]] = None,
    end_offsets: Optional[Dict[str, int]] = None,
    reference_offsets: Optional[Dict[str, int]] = None,
    start_boundary: Optional[str] = None,
    end_boundary: Optional[str] = None,
    reference_datetime: Optional[datetime] = None,
    timezone_str: str = "UTC",
    first_day_of_week: int = 0,
    single_day_mode: bool = False,
) -> Tuple[Union[datetime, str], Optional[datetime]]:
    """
    Calculate a date range based on provided offsets, boundaries, and a reference datetime.

    The function applies offsets to a reference datetime (which defaults to the current datetime in the
    specified timezone), adjusts the result to the specified boundaries, and returns a tuple of
    (start_datetime, end_datetime). If single_day_mode is True, it ensures that both datetimes fall on the
    same calendar day and returns the date string (YYYY-MM-DD) along with None for the end date.

    Parameters:
        start_offsets: Offsets for adjusting the start datetime. Example: {'months': -1}
        end_offsets: Offsets for adjusting the end datetime. Example: {'weeks': 1}
        reference_offsets: Offsets for adjusting the reference datetime. Example: {'years': -1}
        start_boundary: Boundary type for the start datetime. Example: 'start_of_month'
        end_boundary: Boundary type for the end datetime. Example: 'end_of_week'
        reference_datetime: The base datetime for calculations. If not provided, defaults to the current datetime.
        timezone_str: Timezone identifier for localizing the datetime. Defaults to 'UTC'.
        first_day_of_week: The starting day of the week (0 = Monday, 6 = Sunday). Defaults to 0.
        single_day_mode: If True, verifies that the start and end datetimes are on the same day and returns a single date.

    Returns:
        A tuple where:
          - In normal mode: (start_datetime, end_datetime)
          - In single_day_mode: (date_string, None) if both datetimes fall on the same day.

    Raises:
        ValueError: If single_day_mode is True but the start and end datetimes are on different days,
                    or if invalid offset keys are provided.
    """
    # 1) Validate offsets.
    check_offset_keys(start_offsets)
    check_offset_keys(end_offsets)
    check_offset_keys(reference_offsets)

    # 2) Determine timezone and reference datetime.
    tz = timezone(timezone_str)
    if reference_datetime is not None:
        # If the provided datetime is naive, localize it; otherwise, convert it.
        if reference_datetime.tzinfo is None:
            localized_ref_dt = tz.localize(reference_datetime)
        else:
            localized_ref_dt = reference_datetime.astimezone(tz)
    else:
        localized_ref_dt = datetime.now(tz)

    # 3) Apply reference offsets.
    ref_delta = relativedelta(**(reference_offsets or {}))
    adjusted_ref_dt = localized_ref_dt + ref_delta

    # 4) Calculate start datetime using offsets and boundaries.
    start_delta = relativedelta(**(start_offsets or {}))
    raw_start = adjusted_ref_dt + start_delta
    start_datetime = adjust_datetime_boundary(raw_start, start_boundary, first_day_of_week)

    # 5) Calculate end datetime using offsets and boundaries.
    end_delta = relativedelta(**(end_offsets or {}))
    raw_end = adjusted_ref_dt + end_delta
    end_datetime = adjust_datetime_boundary(raw_end, end_boundary, first_day_of_week)

    # 6) Handle single-day mode.
    if single_day_mode:
        if start_datetime.date() == end_datetime.date():
            return (start_datetime.strftime("%Y-%m-%d"), None)
        else:
            raise ValueError(f"single_day_mode=True, but start and end fall on different days: {start_datetime} vs {end_datetime}")

    return (start_datetime, end_datetime)
