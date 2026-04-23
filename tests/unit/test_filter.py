"""
Unit tests for app/services/filter.py.

Coverage targets:
- FilterEngine.passes(): no filters, keyword match/no match, location, salary, combined
"""

from app.schemas.job import ParsedJob
from app.services.filter import FilterEngine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _job(**kwargs: object) -> ParsedJob:
    """Build a minimal ParsedJob with sensible defaults, overridable via kwargs."""
    defaults: dict[str, object] = {
        "title": "Python Developer",
        "company": "Acme",
        "url": "https://example.com/job/1",
        "location": "Kyiv, Ukraine",
        "skills": ["Python", "FastAPI"],
        "salary_min": 4000,
        "salary_max": 6000,
    }
    defaults.update(kwargs)
    return ParsedJob.model_validate(defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFilterEngineNoFilters:
    """FilterEngine with no rules configured."""

    def test_no_filters_passes_all(self) -> None:
        """Engine with no filters → every job passes."""
        engine = FilterEngine()
        assert engine.passes(_job()) is True

    def test_no_filters_passes_job_with_no_salary(self) -> None:
        """No salary filter → job without salary still passes."""
        engine = FilterEngine()
        assert engine.passes(_job(salary_min=None, salary_max=None)) is True


class TestFilterEngineKeywords:
    """Keyword matching against title + skills."""

    def test_keyword_match_in_title(self) -> None:
        """Keyword found in job title → passes."""
        engine = FilterEngine(keywords=["python"])
        assert engine.passes(_job(title="Senior Python Developer")) is True

    def test_keyword_match_in_skills(self) -> None:
        """Keyword found in skills list → passes."""
        engine = FilterEngine(keywords=["fastapi"])
        assert engine.passes(_job(title="Backend Engineer", skills=["FastAPI", "Redis"])) is True

    def test_keyword_no_match_fails(self) -> None:
        """Keyword not in title or skills → fails."""
        engine = FilterEngine(keywords=["golang"])
        assert engine.passes(_job(title="Python Dev", skills=["asyncio"])) is False

    def test_keyword_match_is_case_insensitive(self) -> None:
        """Keyword matching ignores case."""
        engine = FilterEngine(keywords=["PYTHON"])
        assert engine.passes(_job(title="python developer")) is True

    def test_multiple_keywords_any_match_passes(self) -> None:
        """Any keyword matching is sufficient to pass."""
        engine = FilterEngine(keywords=["golang", "python"])
        assert engine.passes(_job(title="Python Developer")) is True

    def test_multiple_keywords_none_match_fails(self) -> None:
        """No keyword matches → fails."""
        engine = FilterEngine(keywords=["golang", "rust"])
        assert engine.passes(_job(title="Python Developer", skills=["asyncio"])) is False


class TestFilterEngineLocation:
    """Location filtering."""

    def test_location_match_passes(self) -> None:
        """Job location contains filter location → passes."""
        engine = FilterEngine(location="ukraine")
        assert engine.passes(_job(location="Kyiv, Ukraine")) is True

    def test_location_match_case_insensitive(self) -> None:
        """Location comparison is case-insensitive."""
        engine = FilterEngine(location="Ukraine")
        assert engine.passes(_job(location="kyiv, ukraine")) is True

    def test_location_no_match_fails(self) -> None:
        """Job location does not contain filter location → fails."""
        engine = FilterEngine(location="poland")
        assert engine.passes(_job(location="Kyiv, Ukraine")) is False

    def test_location_job_has_none_fails(self) -> None:
        """Job with no location and location filter set → fails."""
        engine = FilterEngine(location="ukraine")
        assert engine.passes(_job(location=None)) is False

    def test_location_filter_none_passes_any(self) -> None:
        """No location filter → jobs with any location pass."""
        engine = FilterEngine(location=None)
        assert engine.passes(_job(location=None)) is True


class TestFilterEngineSalary:
    """Salary minimum threshold filtering."""

    def test_salary_above_min_passes(self) -> None:
        """Job salary_min >= filter salary_min → passes."""
        engine = FilterEngine(salary_min=3000)
        assert engine.passes(_job(salary_min=4000)) is True

    def test_salary_equal_to_min_passes(self) -> None:
        """Job salary_min == filter salary_min → passes (inclusive)."""
        engine = FilterEngine(salary_min=4000)
        assert engine.passes(_job(salary_min=4000)) is True

    def test_salary_below_min_fails(self) -> None:
        """Job salary_min < filter salary_min → fails."""
        engine = FilterEngine(salary_min=5000)
        assert engine.passes(_job(salary_min=3000)) is False

    def test_salary_job_has_no_salary_fails(self) -> None:
        """Salary filter active but job has no salary_min → fails."""
        engine = FilterEngine(salary_min=3000)
        assert engine.passes(_job(salary_min=None)) is False

    def test_salary_filter_none_passes_any(self) -> None:
        """No salary filter → jobs with any salary pass."""
        engine = FilterEngine(salary_min=None)
        assert engine.passes(_job(salary_min=None)) is True


class TestFilterEngineCombined:
    """Combined filter rules — all must pass."""

    def test_combined_filters_all_pass(self) -> None:
        """Job satisfies all filters → passes."""
        engine = FilterEngine(
            keywords=["python"],
            location="ukraine",
            salary_min=3000,
        )
        job = _job(
            title="Python Developer",
            location="Kyiv, Ukraine",
            salary_min=5000,
        )
        assert engine.passes(job) is True

    def test_combined_filters_one_fails(self) -> None:
        """Job fails one filter → overall fails."""
        engine = FilterEngine(
            keywords=["python"],
            location="ukraine",
            salary_min=6000,
        )
        job = _job(
            title="Python Developer",
            location="Kyiv, Ukraine",
            salary_min=3000,  # below threshold
        )
        assert engine.passes(job) is False

    def test_combined_filters_keyword_fails(self) -> None:
        """Job matches location + salary but not keyword → fails."""
        engine = FilterEngine(
            keywords=["golang"],
            location="ukraine",
            salary_min=3000,
        )
        job = _job(
            title="Python Developer",
            location="Kyiv, Ukraine",
            salary_min=5000,
        )
        assert engine.passes(job) is False
