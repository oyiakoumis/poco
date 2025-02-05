from typing import Dict, Optional
import logging
from datetime import datetime, timedelta
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TemporalBoundary(BaseModel):
    """Boundary for temporal references"""

    boundary: str = Field(description="Type of boundary (start_of_day, end_of_day, start_of_week, etc.)")
    offset: Optional[int] = Field(default=0, description="Offset from boundary in days")


class TemporalReferenceInput(BaseModel):
    """Input for resolving temporal references"""

    text: str = Field(description="Text containing temporal references to resolve")
    reference: Optional[datetime] = Field(default=None, description="Reference time (defaults to current time)")
    start: Optional[TemporalBoundary] = Field(default=None, description="Start boundary for time range")
    end: Optional[TemporalBoundary] = Field(default=None, description="End boundary for time range")
    single_day_mode: bool = Field(default=True, description="Whether to treat references as single days")


class TemporalReferenceTool(BaseTool):
    """Tool for resolving temporal references in text"""

    name: str = "temporal_reference_resolver"
    description: str = "Resolves temporal references like 'tomorrow', 'next week', etc."
    args_schema: type[BaseModel] = TemporalReferenceInput

    def _get_boundary(self, dt: datetime, boundary: TemporalBoundary) -> datetime:
        """Get datetime for a boundary"""
        logger.info(f"Getting boundary for {dt} with boundary {boundary.boundary}")
        try:
            if boundary.boundary == "start_of_day":
                result = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            elif boundary.boundary == "end_of_day":
                result = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif boundary.boundary == "start_of_week":
                result = (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            elif boundary.boundary == "end_of_week":
                result = (dt + timedelta(days=6 - dt.weekday())).replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                raise ValueError(f"Unknown boundary type: {boundary.boundary}")

            if boundary.offset:
                result += timedelta(days=boundary.offset)

            logger.info(f"Boundary result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error getting boundary: {e}")
            raise

    async def _arun(
        self,
        text: str,
        reference: Optional[datetime] = None,
        start: Optional[TemporalBoundary] = None,
        end: Optional[TemporalBoundary] = None,
        single_day_mode: bool = True,
    ) -> Dict[str, str]:
        """Resolve temporal references"""
        logger.info("Starting temporal reference resolution")
        try:
            now = reference or datetime.now()
            logger.info(f"Reference time: {now}")

            # Common temporal references
            references = {
                "today": now,
                "tomorrow": now + timedelta(days=1),
                "yesterday": now - timedelta(days=1),
                "next week": now + timedelta(weeks=1),
                "last week": now - timedelta(weeks=1),
                "next month": now.replace(month=now.month % 12 + 1),
                "last month": now.replace(month=(now.month - 2) % 12 + 1),
            }
            logger.info(f"Base references generated: {references}")

            # Add day names
            weekday = now.weekday()
            for i, day in enumerate(["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]):
                # Next occurrence
                days_ahead = i - weekday
                if days_ahead <= 0:
                    days_ahead += 7
                next_day = now + timedelta(days=days_ahead)
                references[f"next {day}"] = next_day
                references[day] = next_day

                # Last occurrence
                days_behind = weekday - i
                if days_behind <= 0:
                    days_behind += 7
                last_day = now - timedelta(days=days_behind)
                references[f"last {day}"] = last_day

            logger.info("Day references added")

            # Format references based on boundaries
            result = {}
            for ref_text, ref_date in references.items():
                try:
                    if single_day_mode:
                        # Format as single day
                        if start:
                            start_dt = self._get_boundary(ref_date, start)
                            result[ref_text] = start_dt.strftime("%Y-%m-%d")
                            logger.info(f"Added single day reference: {ref_text} -> {result[ref_text]}")
                    else:
                        # Format as range
                        if start and end:
                            start_dt = self._get_boundary(ref_date, start)
                            end_dt = self._get_boundary(ref_date, end)
                            result[ref_text] = f"{start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}"
                            logger.info(f"Added range reference: {ref_text} -> {result[ref_text]}")
                except Exception as e:
                    logger.error(f"Error formatting reference {ref_text}: {e}")

            logger.info(f"Final result: {result}")
            return result

        except Exception as e:
            logger.error(f"Error in temporal reference resolution: {e}")
            raise

    def _run(
        self,
        text: str,
        reference: Optional[datetime] = None,
        start: Optional[TemporalBoundary] = None,
        end: Optional[TemporalBoundary] = None,
        single_day_mode: bool = True,
    ) -> Dict[str, str]:
        """Synchronous version not supported"""
        raise NotImplementedError("This tool only supports async operations")
