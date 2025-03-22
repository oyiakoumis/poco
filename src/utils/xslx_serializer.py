import base64
import io
from typing import Any, Dict, List, Union
from uuid import uuid4

import pandas as pd

from utils.logging import logger


def serialize_to_xlsx(data: List[Dict[str, Any]], base_name: str, max_file_size_mb: int = 16) -> Dict[str, Union[str, int]]:
    """Serialize data to XLSX format."""
    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Create BytesIO object to store XLSX file
    xlsx_buffer = io.BytesIO()

    # Write DataFrame to XLSX file
    df.to_excel(xlsx_buffer, index=False)

    # Get file size
    file_size = xlsx_buffer.tell()

    # Check if file size exceeds limit
    if file_size > max_file_size_mb * 1024 * 1024:  # Convert MB to bytes
        logger.error(f"XLSX file size ({file_size} bytes) exceeds {max_file_size_mb}MB limit")
        raise ValueError(f"XLSX file size exceeds {max_file_size_mb}MB limit. Please refine your query to return fewer records.")

    # Reset buffer position
    xlsx_buffer.seek(0)

    # Process filename
    # Replace spaces and special characters with underscores
    processed_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in base_name)
    # Truncate to 20 characters or less
    processed_name = processed_name[:20]

    # Add short UUID to filename
    short_uuid = str(uuid4())[:8]
    filename = f"{processed_name}_{short_uuid}.xlsx"

    return {
        "filename": filename,
        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "content": base64.b64encode(xlsx_buffer.getvalue()).decode("utf-8"),
        "size": file_size,
    }
