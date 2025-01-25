from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
from pytz import timezone
import pytz
from langchain_core.tools import tool


def adjust_datetime_boundary(dt: datetime, boundary_type: str, first_day_of_week: int = 0) -> datetime:
    """
    Adjust a datetime object to a specific boundary.
    Boundary types:
      - start_of_month, end_of_month, start_of_day, end_of_day
      - start_of_week, end_of_week, start_of_year, end_of_year

    Parameters:
      - dt: The datetime object to adjust.
      - boundary_type: The type of boundary to adjust to.
      - first_day_of_week: The starting day of the week (0 = Monday, 6 = Sunday).

    Returns:
      - A datetime object after applying the boundary adjustment.
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
        weekday = (dt.weekday() - first_day_of_week) % 7
        return (dt - relativedelta(days=weekday)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif boundary_type == "end_of_week":
        weekday = (dt.weekday() - first_day_of_week) % 7
        return (dt + relativedelta(days=(6 - weekday))).replace(hour=23, minute=59, second=59, microsecond=999999)

    raise ValueError(f"Unrecognized boundary_type: {boundary_type}")


def check_offset_keys(offsets: dict) -> None:
    """
    Check the validity of keys in the offsets dictionary.

    Parameters:
      - offsets: A dictionary containing offset keys like 'years', 'months', etc.

    Raises:
      - ValueError if any key is invalid.
    """
    valid_keys = {"years", "months", "weeks", "days", "hours", "minutes", "seconds"}
    if offsets:
        invalid_keys = set(offsets.keys()) - valid_keys
        if invalid_keys:
            raise ValueError(f"Invalid offset keys: {invalid_keys}")


from pydantic import BaseModel, Field
from typing import Optional, Dict
from datetime import datetime


class CalculateDateRangeArgs(BaseModel):
    start_offsets: Optional[Dict[str, int]] = Field(None, description="Offsets for adjusting the start datetime. Example: {'months': -1}")
    end_offsets: Optional[Dict[str, int]] = Field(None, description="Offsets for adjusting the end datetime. Example: {'weeks': 1}")
    reference_offsets: Optional[Dict[str, int]] = Field(None, description="Offsets for adjusting the reference datetime. Example: {'years': -1}")
    start_boundary: Optional[str] = Field(None, description="Boundary type for the start datetime. Example: 'start_of_month'")
    end_boundary: Optional[str] = Field(None, description="Boundary type for the end datetime. Example: 'end_of_week'")
    reference_datetime: Optional[datetime] = Field(None, description="The base datetime for calculations. Defaults to now if not provided.")
    timezone_str: Optional[str] = Field("UTC", description="Timezone for localization. Defaults to 'UTC'.")
    first_day_of_week: Optional[int] = Field(0, description="The starting day of the week (0 = Monday, 6 = Sunday). Defaults to 0.")


@tool(args_schema=CalculateDateRangeArgs)
def calculate_date_range(
    start_offsets: dict = None,
    end_offsets: dict = None,
    reference_offsets: dict = None,
    start_boundary: str = None,
    end_boundary: str = None,
    reference_datetime: datetime = None,
    timezone_str: str = "UTC",
    first_day_of_week: int = 0,
):
    """
    Calculate a start and end datetime range based on offsets, boundaries, and a reference datetime.

    This function is designed to resolve temporal references like "last month" or "this week"
    by calculating precise date ranges using the provided offsets, boundaries, and reference datetime.
    """
    # Validate offsets
    check_offset_keys(start_offsets)
    check_offset_keys(end_offsets)
    check_offset_keys(reference_offsets)

    # Get the timezone
    tz = timezone(timezone_str)

    # Convert reference_datetime to the target timezone
    if reference_datetime is not None:
        if reference_datetime.tzinfo is None:  # Localize naive datetime
            now = tz.localize(reference_datetime)
        else:  # Convert timezone-aware datetime to the target timezone
            now = reference_datetime.astimezone(tz)
    else:
        now = datetime.now(tz)

    # Step 1: Apply reference offsets to now
    reference_delta = relativedelta(**(reference_offsets or {}))
    reference_datetime = now + reference_delta

    # Step 2: Derive start_datetime from reference_datetime
    start_delta = relativedelta(**(start_offsets or {}))
    start_datetime = adjust_datetime_boundary(reference_datetime + start_delta, start_boundary, first_day_of_week=first_day_of_week)

    # Step 3: Derive end_datetime from reference_datetime
    end_delta = relativedelta(**(end_offsets or {}))
    end_datetime = adjust_datetime_boundary(reference_datetime + end_delta, end_boundary, first_day_of_week=first_day_of_week)

    return start_datetime, end_datetime
