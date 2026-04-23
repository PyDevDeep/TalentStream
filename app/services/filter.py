from typing import List, Optional

from app.schemas.job import ParsedJob


class FilterEngine:
    def __init__(
        self,
        keywords: Optional[List[str]] = None,
        location: Optional[str] = None,
        salary_min: Optional[int] = None,
    ):
        """Initialize the filter engine with optional keyword, location, and salary rules."""
        self.keywords = [k.lower() for k in keywords] if keywords else None
        self.location = location.lower() if location else None
        self.salary_min = salary_min

    def passes(self, job: ParsedJob) -> bool:
        """Return True if the job satisfies all active filter rules."""
        if not self._matches_keywords(job):
            return False
        if not self._matches_location(job):
            return False
        if not self._meets_salary(job):
            return False
        return True

    def _matches_keywords(self, job: ParsedJob) -> bool:
        if not self.keywords:
            return True

        search_text = (job.title + " " + " ".join(job.skills)).lower()
        return any(keyword in search_text for keyword in self.keywords)

    def _matches_location(self, job: ParsedJob) -> bool:
        if not self.location:
            return True
        if not job.location:
            return False
        return self.location in job.location.lower()

    def _meets_salary(self, job: ParsedJob) -> bool:
        if not self.salary_min:
            return True
        if not job.salary_min:
            return False
        return job.salary_min >= self.salary_min
