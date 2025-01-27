from datetime import datetime, timezone


def get_utc_now() -> datetime:
    """Get current timezone-aware UTC datetime"""
    return datetime.now(timezone.utc)
