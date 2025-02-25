from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Literal


class CheckboxItem(BaseModel):
    """Model for a single checkbox item with its state"""

    label: str = Field(description="Label for the checkbox item")
    checked: bool = Field(description="Whether the item is checked")

    model_config = {"json_schema_extra": {"example": {"label": "Task 1", "checked": True}}}


class ChartDataset(BaseModel):
    """Model for chart dataset configuration"""

    label: str = Field(description="Label for the dataset")
    data: List[float] = Field(description="Numerical data points")

    model_config = {"json_schema_extra": {"example": {"label": "Sales", "data": [65, 59, 80]}}}


class Markdown(BaseModel):
    type: Literal["markdown"] = "markdown"
    content: str = Field(min_length=1, description="Markdown formatted text content")

    model_config = {"json_schema_extra": {"example": {"type": "markdown", "content": "# Title\nThis is a **formatted** text with _markdown_ syntax"}}}


class Checkbox(BaseModel):
    type: Literal["checkbox"] = "checkbox"
    items: List[CheckboxItem] = Field(description="Items with their checked state")

    model_config = {
        "json_schema_extra": {
            "example": {
                "type": "checkbox",
                "items": [{"label": "Task 1", "checked": True}, {"label": "Task 2", "checked": False}, {"label": "Task 3", "checked": True}],
            }
        }
    }


class Table(BaseModel):
    type: Literal["table"] = "table"
    headers: List[str] = Field(description="Column headers")
    rows: List[List[Any]] = Field(description="Table data rows")

    @model_validator(mode="after")
    def validate_row_lengths(self) -> "Table":
        if self.headers:
            expected_length = len(self.headers)
            for i, row in enumerate(self.rows):
                if len(row) != expected_length:
                    raise ValueError(f"Row {i} has {len(row)} columns but should have {expected_length} to match headers")
        return self

    model_config = {
        "json_schema_extra": {
            "example": {
                "type": "table",
                "headers": ["Name", "Age", "City"],
                "rows": [["John Doe", 30, "New York"], ["Jane Smith", 25, "London"], ["Bob Johnson", 35, "Paris"]],
            }
        }
    }


class Chart(BaseModel):
    type: Literal["chart"] = "chart"
    chart_type: Literal["bar", "line", "pie"] = Field(description="Type of chart")
    labels: List[str] = Field(description="Data labels")
    datasets: List[ChartDataset] = Field(description="Chart datasets")

    @model_validator(mode="after")
    def validate_dataset_lengths(self) -> "Chart":
        if self.labels:
            expected_length = len(self.labels)
            for i, dataset in enumerate(self.datasets):
                if len(dataset.data) != expected_length:
                    raise ValueError(f"Dataset {i} has {len(dataset.data)} points but should have {expected_length} to match labels")
        return self

    model_config = {
        "json_schema_extra": {
            "example": {
                "type": "chart",
                "chart_type": "bar",
                "labels": ["January", "February", "March"],
                "datasets": [{"label": "Sales", "data": [65, 59, 80]}],
            }
        }
    }


# Union of all components
Component = Union[Markdown, Checkbox, Table, Chart]
