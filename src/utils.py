from datetime import datetime, timezone


def get_utc_now() -> datetime:
    """Get current timezone-aware UTC datetime"""
    return datetime.now(timezone.utc)


def try_convert_to_int(str):
    try:
        return int(str)
    except ValueError:
        return str
