import re
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ParsedJob(BaseModel):
    title: str
    company: str
    url: str
    location: Optional[str] = None
    salary: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    skills: List[str] = Field(default_factory=list)
    description: Optional[str] = None

    @model_validator(mode="after")
    def parse_salary_string(self) -> "ParsedJob":
        """Parse raw salary string into salary_min and salary_max."""
        if not self.salary:
            return self

        # Skip if LLM already extracted numeric values.
        if self.salary_min is not None or self.salary_max is not None:
            return self

        clean_salary = self.salary.replace(",", "").lower()

        # Find numbers with optional 'k' suffix (e.g. "5k" → 5000).
        matches = re.findall(r"(\d+)(k)?", clean_salary)
        if not matches:
            return self

        parsed_nums: list[int] = []
        for num_str, k_suffix in matches:
            val = int(num_str)
            if k_suffix:
                val *= 1000
            parsed_nums.append(val)

        if len(parsed_nums) == 1:
            self.salary_min = parsed_nums[0]
        elif len(parsed_nums) >= 2:
            self.salary_min = min(parsed_nums[:2])
            self.salary_max = max(parsed_nums[:2])

        return self


class JobCreate(ParsedJob):
    """Schema for creating a DB record. Inherits validated fields from ParsedJob."""

    pass


class JobResponse(BaseModel):
    """Schema for returning job data via API from an ORM model."""

    id: int
    title: str
    company: str
    location: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    skills: List[str]
    source_url: str
    notified: bool
    created_at: datetime

    # Pydantic v2: enable ORM mode for SQLAlchemy model compatibility.
    model_config = ConfigDict(from_attributes=True)
