import calendar
from datetime import datetime
from typing import Dict, Literal, Optional, Union

from dateutil.relativedelta import relativedelta
from langchain.tools import BaseTool
from langchain_core.runnables.config import RunnableConfig
from pydantic import BaseModel, Field
from pytz import timezone


class SnapConfig(BaseModel):
    type: Literal["weekday", "month"] = Field(description="Type of snap adjustment to apply - either 'weekday' or 'month'")
    target: int = Field(description="Target value to snap to - for weekday: 0-6 (Monday=0), for month: 1-12")
    modifier: str = Field(default="this", description="How to apply the snap - 'last', 'this', or 'next'")


class DateConfig(BaseModel):
    offset: Optional[Dict] = Field(default=None, description="Dictionary of offsets to apply (e.g., {'days': 1, 'months': -1})")
    boundary: Optional[str] = Field(default=None, description="Boundary adjustment ('start_of_year', 'end_of_year', 'start_of_month', etc.)")
    snap: Optional[SnapConfig] = Field(default=None, description="Configuration for snapping to specific weekdays or months")


class TemporalReferenceInput(BaseModel):
    reference: Optional[datetime] = Field(default=None, description="Reference datetime to calculate from. Defaults to current time if not provided")
    start: Optional[DateConfig] = Field(default=None, description="Configuration for start datetime calculations")
    end: Optional[DateConfig] = Field(default=None, description="Configuration for end datetime calculations")
    single_day_mode: bool = Field(default=False, description="If True, ensures start and end fall on same day and returns date string")


class TemporalReferenceTool(BaseTool):
    name: str = "temporal_reference_resolver"
    description: str = """
    Resolves temporal references into concrete datetime ranges based on a reference time.
    Useful for converting relative time expressions (e.g., 'next week', 'last month') into specific datetime ranges.

    Examples:
    {"start": {"offset": {"weeks": -1}, "boundary": "start_of_week"}, "end": {"offset": {"weeks": -1}, "boundary": "end_of_week"}},  # last week
    {"start": {"snap": {"type": "weekday", "target": 0, "modifier": "next"}, "boundary": "start_of_day"}, "end": {"snap": {"type": "weekday", "target": 0, "modifier": "next"}, "boundary": "end_of_day"}},  # next Monday
    {"start": {"boundary": "start_of_day"}, "end": {"offset": {"days": 3}, "boundary": "end_of_day"}},  # next 3 days
    {"start": {"offset": {"days": 3}, "boundary": "start_of_day"}, "end": {"offset": {"days": 3}, "boundary": "end_of_day"}, "single_day_mode": True}]  # in 3 days
    """
    args_schema: type[BaseModel] = TemporalReferenceInput

    def _adjust_datetime_boundary(self, dt: datetime, boundary: Optional[str], first_day_of_week: int = 0) -> datetime:
        """Adjust a datetime to a specified boundary."""
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

    def _adjust_to_weekday(self, dt: datetime, target: int, modifier: str = "this") -> datetime:
        """Snap dt to a target weekday."""
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

    def _adjust_to_month(self, dt: datetime, target: int, modifier: str = "this") -> datetime:
        """Snap dt to a target month."""
        year = dt.year
        if modifier == "next" and dt.month >= target:
            year += 1
        if modifier == "last" and dt.month <= target:
            year -= 1

        last_day = calendar.monthrange(year, target)[1]
        day = min(dt.day, last_day)
        return dt.replace(year=year, month=target, day=day)

    def _run(
        self,
        config: RunnableConfig,
        reference: Optional[datetime] = None,
        start: Optional[DateConfig] = None,
        end: Optional[DateConfig] = None,
        single_day_mode: bool = False,
    ) -> Union[tuple[str, None], tuple[datetime, datetime]]:
        """
        Core implementation of the temporal reference resolution tool.
        """
        # Extract configuration values
        configurable = config.get("configurable", {})
        tz = configurable.get("time_zone", "UTC")
        first_day_of_week = configurable.get("first_day_of_the_week", 0)

        tz_obj = timezone(tz)
        # Localize the reference datetime (or use now)
        if reference:
            ref = reference if reference.tzinfo else tz_obj.localize(reference)
            ref = ref.astimezone(tz_obj)
        else:
            ref = datetime.now(tz_obj)

        # Convert start/end configs to dicts for compatibility
        start_cfg = start.model_dump(exclude_none=True) if start else {}
        end_cfg = end.model_dump(exclude_none=True) if end else {}

        # Compute the raw dates with offsets
        start_dt = ref + relativedelta(**(start_cfg.get("offset", {}))) if start_cfg.get("offset") else ref
        end_dt = ref + relativedelta(**(end_cfg.get("offset", {}))) if end_cfg.get("offset") else ref

        # Apply boundary adjustments
        if "boundary" in start_cfg:
            start_dt = self._adjust_datetime_boundary(start_dt, start_cfg["boundary"], first_day_of_week)
        if "boundary" in end_cfg:
            end_dt = self._adjust_datetime_boundary(end_dt, end_cfg["boundary"], first_day_of_week)

        # Apply "snap" adjustments if provided
        if "snap" in start_cfg:
            snap = start_cfg["snap"]
            modifier = snap.get("modifier", "this")
            if snap["type"] == "weekday":
                start_dt = self._adjust_to_weekday(start_dt, snap["target"], modifier)
            elif snap["type"] == "month":
                start_dt = self._adjust_to_month(start_dt, snap["target"], modifier)

        if "snap" in end_cfg:
            snap = end_cfg["snap"]
            modifier = snap.get("modifier", "this")
            if snap["type"] == "weekday":
                end_dt = self._adjust_to_weekday(end_dt, snap["target"], modifier)
            elif snap["type"] == "month":
                end_dt = self._adjust_to_month(end_dt, snap["target"], modifier)

        if single_day_mode:
            if start_dt.date() != end_dt.date():
                raise ValueError("single_day_mode is True, but start and end dates differ.")
            return (start_dt.strftime("%Y-%m-%d"), None)

        return (start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S"))
