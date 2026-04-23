"""
Unit tests for app/schemas/job.py.

Coverage targets:
- ParsedJob: valid full/minimal input, missing required fields
- parse_salary_string validator: range, single, currency prefix, k-suffix, no match
"""

import pytest
from pydantic import ValidationError

from app.schemas.job import ParsedJob

# ---------------------------------------------------------------------------
# ParsedJob validation
# ---------------------------------------------------------------------------


class TestParsedJobValidation:
    """Tests for ParsedJob Pydantic model validation."""

    def test_parsed_job_valid_all_fields(self) -> None:
        """All fields provided → model instantiates without error."""
        job = ParsedJob(
            title="Senior Python Developer",
            company="Acme Corp",
            url="https://example.com/job/1",
            location="Kyiv, Ukraine",
            salary="5000-7000",
            skills=["Python", "FastAPI", "PostgreSQL"],
            description="Full-stack role with async Python.",
        )
        assert job.title == "Senior Python Developer"
        assert job.company == "Acme Corp"
        assert job.location == "Kyiv, Ukraine"
        assert job.skills == ["Python", "FastAPI", "PostgreSQL"]

    def test_parsed_job_minimal_required_only(self) -> None:
        """Only required fields (title, company, url) → valid model."""
        job = ParsedJob(
            title="Backend Engineer",
            company="StartupXYZ",
            url="https://example.com/job/2",
        )
        assert job.title == "Backend Engineer"
        assert job.location is None
        assert job.salary is None
        assert job.skills == []
        assert job.salary_min is None
        assert job.salary_max is None

    def test_parsed_job_missing_title_raises_validation_error(self) -> None:
        """Missing required 'title' field → ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ParsedJob.model_validate({"company": "Acme", "url": "https://example.com/job/3"})
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("title",) for e in errors)

    def test_parsed_job_missing_company_raises_validation_error(self) -> None:
        """Missing required 'company' field → ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ParsedJob.model_validate({"title": "Dev", "url": "https://example.com/job/4"})
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("company",) for e in errors)

    def test_parsed_job_missing_url_raises_validation_error(self) -> None:
        """Missing required 'url' field → ValidationError."""
        with pytest.raises(ValidationError):
            ParsedJob.model_validate({"title": "Dev", "company": "Acme"})

    def test_parsed_job_skills_defaults_to_empty_list(self) -> None:
        """skills field omitted → defaults to empty list, not None."""
        job = ParsedJob(title="Dev", company="Acme", url="https://example.com/")
        assert isinstance(job.skills, list)
        assert len(job.skills) == 0


# ---------------------------------------------------------------------------
# Salary parsing validator
# ---------------------------------------------------------------------------


class TestParsedJobSalaryParsing:
    """Tests for the parse_salary_string model validator."""

    def test_salary_range_sets_min_and_max(self) -> None:
        """'5000-7000' → salary_min=5000, salary_max=7000."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="5000-7000",
        )
        assert job.salary_min == 5000
        assert job.salary_max == 7000

    def test_salary_single_value_sets_min_only(self) -> None:
        """'5000/month' → salary_min=5000, salary_max=None."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="5000/month",
        )
        assert job.salary_min == 5000
        assert job.salary_max is None

    def test_salary_with_dollar_prefix_parses_correctly(self) -> None:
        """'$5000-$7000' → salary_min=5000, salary_max=7000."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="$5000-$7000",
        )
        assert job.salary_min == 5000
        assert job.salary_max == 7000

    def test_salary_k_suffix_multiplies_by_1000(self) -> None:
        """'5k-10k' → salary_min=5000, salary_max=10000."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="5k-10k",
        )
        assert job.salary_min == 5000
        assert job.salary_max == 10000

    def test_salary_dollar_k_suffix(self) -> None:
        """'$5k-$10k' → salary_min=5000, salary_max=10000."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="$5k-$10k",
        )
        assert job.salary_min == 5000
        assert job.salary_max == 10000

    def test_salary_with_comma_separator(self) -> None:
        """'10,000-15,000' → salary_min=10000, salary_max=15000."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="10,000-15,000",
        )
        assert job.salary_min == 10000
        assert job.salary_max == 15000

    def test_salary_none_leaves_min_max_none(self) -> None:
        """salary=None → salary_min and salary_max remain None."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary=None,
        )
        assert job.salary_min is None
        assert job.salary_max is None

    def test_salary_no_numbers_leaves_min_max_none(self) -> None:
        """salary string with no digits → salary_min and salary_max remain None."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="competitive",
        )
        assert job.salary_min is None
        assert job.salary_max is None

    def test_salary_already_parsed_skips_validator(self) -> None:
        """If salary_min already set by caller → validator skips re-parsing."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="9999-99999",
            salary_min=1000,
            salary_max=2000,
        )
        # validator must not overwrite the explicitly provided values
        assert job.salary_min == 1000
        assert job.salary_max == 2000

    def test_salary_range_min_is_lower_value(self) -> None:
        """'7000-5000' (reversed) → salary_min=5000, salary_max=7000."""
        job = ParsedJob(
            title="Dev",
            company="Acme",
            url="https://x.com/",
            salary="7000-5000",
        )
        assert job.salary_min == 5000
        assert job.salary_max == 7000
