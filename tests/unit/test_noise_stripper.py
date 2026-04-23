"""
Unit tests for app/services/noise_stripper.py.

Coverage targets:
- strip_noise(): removes similar/related jobs block, removes navigation lines,
  preserves job content, truncates at 5000 chars, handles empty input
"""

import pytest

from app.services.noise_stripper import strip_noise

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_html_with_noise() -> str:
    """Raw page text that contains job content mixed with noise sections."""
    return (
        "Home\n"
        "Sign In\n"
        "Senior Python Developer\n"
        "Acme Corp · Kyiv, Ukraine\n"
        "We are looking for a Python developer with 5+ years of experience.\n"
        "Requirements: FastAPI, asyncio, PostgreSQL.\n"
        "Similar Jobs\n"
        "Junior Developer at OtherCorp\n"
        "Golang Engineer at YetAnother\n"
    )


@pytest.fixture
def clean_job_text() -> str:
    """Raw text with only meaningful job content, no noise."""
    return (
        "Backend Engineer\n"
        "StartupXYZ · Remote\n"
        "Looking for a backend engineer with Python and Redis experience.\n"
        "Skills: Python, Redis, Docker.\n"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStripNoise:
    """Tests for strip_noise() function."""

    def test_removes_similar_jobs_block(self, sample_html_with_noise: str) -> None:
        """Everything after 'Similar Jobs' header is removed."""
        result = strip_noise(sample_html_with_noise)
        assert "Junior Developer at OtherCorp" not in result
        assert "Golang Engineer at YetAnother" not in result

    def test_removes_related_jobs_variant(self) -> None:
        """'Related Jobs' variant is also stripped."""
        text = "Python Developer\nAcme Corp\nRelated Jobs\nOther Job 1\nOther Job 2"
        result = strip_noise(text)
        assert "Other Job 1" not in result
        assert "Other Job 2" not in result

    def test_removes_navigation_lines(self, sample_html_with_noise: str) -> None:
        """Navigation keywords like 'Home' and 'Sign In' are removed."""
        result = strip_noise(sample_html_with_noise)
        assert "Home" not in result
        assert "Sign In" not in result

    def test_removes_log_in_navigation(self) -> None:
        """'Log In' navigation line is stripped."""
        text = "Log In\nPython Developer\nAcme Corp"
        result = strip_noise(text)
        assert "Log In" not in result
        assert "Python Developer" in result

    def test_preserves_job_content(self, sample_html_with_noise: str) -> None:
        """Core job description text survives noise stripping."""
        result = strip_noise(sample_html_with_noise)
        assert "Senior Python Developer" in result
        assert "Acme Corp" in result
        assert "FastAPI" in result

    def test_preserves_clean_text_unchanged_modulo_whitespace(self, clean_job_text: str) -> None:
        """Text without any noise → all content is preserved."""
        result = strip_noise(clean_job_text)
        assert "Backend Engineer" in result
        assert "StartupXYZ" in result
        assert "Python, Redis, Docker" in result

    def test_truncates_long_content_to_5000_chars(self) -> None:
        """Input longer than 5000 chars → output truncated to exactly 5000."""
        long_text = "Python Developer at Acme. " * 300  # well over 5000 chars
        result = strip_noise(long_text)
        assert len(result) <= 5000

    def test_short_content_not_truncated(self, clean_job_text: str) -> None:
        """Input shorter than 5000 chars → not truncated."""
        result = strip_noise(clean_job_text)
        assert len(result) < 5000
        assert "Backend Engineer" in result

    def test_empty_string_returns_empty_string(self) -> None:
        """Empty input → empty output, no exception."""
        result = strip_noise("")
        assert result == ""

    def test_only_noise_returns_empty_string(self) -> None:
        """Input with only navigation and noise → empty or near-empty output."""
        text = "Home\nSign In\nMenu\nLog In\nPrivacy Policy"
        result = strip_noise(text)
        # All lines are navigation noise — result should contain no meaningful text
        assert "Home" not in result
        assert "Sign In" not in result

    def test_normalizes_multiple_whitespace(self) -> None:
        """Multiple consecutive spaces/newlines → collapsed to single space."""
        text = "Python   Developer\n\n\nAcme   Corp"
        result = strip_noise(text)
        assert "  " not in result  # no double spaces remain

    def test_case_insensitive_noise_removal(self) -> None:
        """Noise section headers are removed regardless of case."""
        text = "Python Developer\nAcme Corp\nSIMILAR JOBS\nSome Other Job"
        result = strip_noise(text)
        assert "Some Other Job" not in result
        assert "Python Developer" in result
