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
        """Парсить сирий рядок зарплати у salary_min та salary_max."""
        if not self.salary:
            return self

        # Якщо LLM вже успішно розпарсив числа, пропускаємо
        if self.salary_min is not None or self.salary_max is not None:
            return self

        # Очищення від ком та переведення в нижній регістр (10,000 -> 10000)
        clean_salary = self.salary.replace(",", "").lower()

        # Пошук чисел з опціональним суфіксом 'k'
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
    """Схема для створення запису в БД. Успадковує валідовані поля з ParsedJob."""

    pass


class JobResponse(BaseModel):
    """Схема для віддачі даних через API з ORM-моделі."""

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

    # Pydantic v2 налаштування для сумісності з SQLAlchemy моделями
    model_config = ConfigDict(from_attributes=True)
