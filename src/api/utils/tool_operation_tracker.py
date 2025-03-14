"""Tool operation tracking and summary generation."""

import json
from typing import List


class ToolOperationTracker:
    """Tracks database tool operations and generates summary strings."""

    # Tool name prefixes for categorization
    DATASET_TOOLS = ["create_dataset", "update_dataset", "delete_dataset"]
    RECORD_TOOLS = ["create_record", "batch_create_records", "update_record", "batch_update_records", "delete_record", "batch_delete_records"]
    FIELD_TOOLS = ["add_field", "update_field", "delete_field"]

    # Icons for different operation types
    ICONS = {"datasets": "📊", "records": "📝", "fields": "🔧"}

    def __init__(self):
        """Initialize the tracker with empty operation counts."""
        self.operations = {
            "datasets": {"created": 0, "updated": 0, "deleted": 0},
            "records": {"added": 0, "updated": 0, "deleted": 0},
            "fields": {"added": 0, "updated": 0, "deleted": 0},
        }

    def get_supported_tools(self) -> List[str]:
        """Return a list of all supported tool names."""
        return self.DATASET_TOOLS + self.RECORD_TOOLS + self.FIELD_TOOLS

    def track_tool_message(self, tool_name: str, content: str) -> None:
        """Process a tool message and update operation counts.

        Args:
            tool_name: The name of the tool that was used
            content: The content of the tool message (JSON string or dict)
        """
        # Parse content if it's a string
        if isinstance(content, str):
            try:
                content_data = json.loads(content)
            except json.JSONDecodeError:
                # If content is not valid JSON, skip tracking
                return
        else:
            content_data = content

        # Track dataset operations
        if tool_name == "create_dataset":
            self.operations["datasets"]["created"] += 1
        elif tool_name == "update_dataset":
            self.operations["datasets"]["updated"] += 1
        elif tool_name == "delete_dataset":
            self.operations["datasets"]["deleted"] += 1

        # Track record operations
        elif tool_name == "create_record":
            self.operations["records"]["added"] += 1
        elif tool_name == "batch_create_records":
            # Count the number of records in the result
            if isinstance(content_data, dict) and "record_ids" in content_data:
                self.operations["records"]["added"] += len(content_data["record_ids"])
            else:
                self.operations["records"]["added"] += 1
        elif tool_name == "update_record":
            self.operations["records"]["updated"] += 1
        elif tool_name == "batch_update_records":
            # Count the number of records in the result
            if isinstance(content_data, dict) and "updated_record_ids" in content_data:
                self.operations["records"]["updated"] += len(content_data["updated_record_ids"])
            else:
                self.operations["records"]["updated"] += 1
        elif tool_name == "delete_record":
            self.operations["records"]["deleted"] += 1
        elif tool_name == "batch_delete_records":
            # Count the number of records in the result
            if isinstance(content_data, dict) and "deleted_record_ids" in content_data:
                self.operations["records"]["deleted"] += len(content_data["deleted_record_ids"])
            else:
                self.operations["records"]["deleted"] += 1

        # Track field operations
        elif tool_name == "add_field":
            self.operations["fields"]["added"] += 1
        elif tool_name == "update_field":
            self.operations["fields"]["updated"] += 1
        elif tool_name == "delete_field":
            self.operations["fields"]["deleted"] += 1

    def build_tool_summary_string(self) -> str:
        """Generate a formatted summary of all tracked operations.

        Returns:
            A concise summary string with icons, or empty string if no operations
        """
        parts = []

        # Add dataset operations
        dataset_parts = []
        if self.operations["datasets"]["created"] > 0:
            count = self.operations["datasets"]["created"]
            dataset_parts.append(f"{count} dataset{'s' if count > 1 else ''} created")
        if self.operations["datasets"]["updated"] > 0:
            count = self.operations["datasets"]["updated"]
            dataset_parts.append(f"{count} dataset{'s' if count > 1 else ''} updated")
        if self.operations["datasets"]["deleted"] > 0:
            count = self.operations["datasets"]["deleted"]
            dataset_parts.append(f"{count} dataset{'s' if count > 1 else ''} deleted")

        if dataset_parts:
            parts.append(f"{self.ICONS['datasets']} {', '.join(dataset_parts)}")

        # Add record operations
        record_parts = []
        if self.operations["records"]["added"] > 0:
            count = self.operations["records"]["added"]
            record_parts.append(f"{count} record{'s' if count > 1 else ''} added")
        if self.operations["records"]["updated"] > 0:
            count = self.operations["records"]["updated"]
            record_parts.append(f"{count} record{'s' if count > 1 else ''} updated")
        if self.operations["records"]["deleted"] > 0:
            count = self.operations["records"]["deleted"]
            record_parts.append(f"{count} record{'s' if count > 1 else ''} deleted")

        if record_parts:
            parts.append(f"{self.ICONS['records']} {', '.join(record_parts)}")

        # Add field operations
        field_parts = []
        if self.operations["fields"]["added"] > 0:
            count = self.operations["fields"]["added"]
            field_parts.append(f"{count} field{'s' if count > 1 else ''} added")
        if self.operations["fields"]["updated"] > 0:
            count = self.operations["fields"]["updated"]
            field_parts.append(f"{count} field{'s' if count > 1 else ''} updated")
        if self.operations["fields"]["deleted"] > 0:
            count = self.operations["fields"]["deleted"]
            field_parts.append(f"{count} field{'s' if count > 1 else ''} deleted")

        if field_parts:
            parts.append(f"{self.ICONS['fields']} {', '.join(field_parts)}")

        # Join all parts with separator
        return " | ".join(parts) if parts else ""
