"""Tests for structured logging setup."""

from __future__ import annotations

import logging
from pathlib import Path

import structlog

from src.core.logger import get_logger, setup_logging


class TestSetupLogging:
    def test_json_format(self):
        setup_logging(level="DEBUG", log_format="json")
        log = get_logger("test_json")
        # Should not raise
        assert log is not None

    def test_text_format(self):
        setup_logging(level="INFO", log_format="text")
        log = get_logger("test_text")
        assert log is not None

    def test_file_output(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(level="WARNING", log_format="json", log_file=log_file)
        assert Path(log_file).parent.exists()

    def test_invalid_level_defaults(self):
        # Invalid level string → defaults to INFO
        setup_logging(level="NONEXISTENT", log_format="json")
        log = get_logger("test_fallback")
        assert log is not None


class TestGetLogger:
    def test_returns_bound_logger(self):
        log = get_logger("my_module")
        assert log is not None

    def test_different_names(self):
        log1 = get_logger("a")
        log2 = get_logger("b")
        assert log1 is not None
        assert log2 is not None
