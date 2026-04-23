"""
Shared pytest configuration and fixtures for all test suites.

Scope:
- asyncio mode is set to "auto" via pyproject.toml (asyncio_mode = "auto")
- Shared environment patching to prevent real .env loading during tests
"""

import os
from collections.abc import Generator
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True, scope="session")
def patch_settings_env() -> Generator[None, None, None]:
    """
    Override critical environment variables for the entire test session
    so that app.config.get_settings() never requires real credentials.

    Tests that need specific values should override these via their own patches.
    """
    env_overrides = {
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost:5432/test_db",
        "REDIS_URL": "redis://localhost:6379/1",
        "SERPER_API_KEY": "test-serper-key",
        "OPENAI_API_KEY": "test-openai-key",
        "GEMINI_API_KEY": "test-gemini-key",
        "SLACK_BOT_TOKEN": "xoxb-test-token",
        "SLACK_CHANNEL_ID": "C0TEST12345",
        "SCRAPE_QUERY": "Python Developer Test",
    }
    with patch.dict(os.environ, env_overrides):
        yield
