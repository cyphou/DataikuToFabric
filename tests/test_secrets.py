"""Tests for Phase 13 — Log redaction and secrets policy enforcement."""

from __future__ import annotations

import pytest

from src.core.secrets import (
    PolicyViolation,
    SecretPolicyChecker,
    _SENSITIVE_KEYS,
    redact_dict,
    redact_value,
    redaction_log_processor,
)


# ── redact_value ──────────────────────────────────────────────────────────────


class TestRedactValue:
    def test_plain_string_unchanged(self):
        assert redact_value("hello world") == "hello world"

    def test_api_key_assignment(self):
        result = redact_value("api_key=supersecret123")
        assert "supersecret123" not in result
        assert "***REDACTED***" in result

    def test_password_colon(self):
        result = redact_value("password: myP@ssw0rd")
        assert "myP@ssw0rd" not in result
        assert "***REDACTED***" in result

    def test_token_in_json_string(self):
        result = redact_value('"access_token": "eyJhbGciOiJIUzI1NiJ9.payload"')
        assert "eyJhbGciOiJIUzI1NiJ9" not in result

    def test_bearer_authorization_header(self):
        result = redact_value("Authorization: Bearer my-secret-token-value")
        assert "my-secret-token-value" not in result

    def test_github_pat(self):
        token = "ghp_" + "a" * 40
        result = redact_value(f"token={token}")
        assert token not in result

    def test_client_secret(self):
        result = redact_value("client_secret=abc123xyz")
        assert "abc123xyz" not in result

    def test_sas_token_pattern(self):
        sas = "sv=2023-01-03&ss=b&srt=sco&sp=rwdlacupitfx&se=2025-01-01&sig=ABCDEF"
        result = redact_value(sas)
        assert "ABCDEF" not in result

    def test_multiple_secrets_same_string(self):
        text = "api_key=key123 password=pass456"
        result = redact_value(text)
        assert "key123" not in result
        assert "pass456" not in result

    def test_empty_string_unchanged(self):
        assert redact_value("") == ""

    def test_no_false_positives(self):
        clean = "SELECT id, name FROM orders WHERE status = 'active'"
        assert redact_value(clean) == clean

    def test_idempotent(self):
        """Redacting already-redacted text should be stable."""
        once = redact_value("api_key=secret")
        twice = redact_value(once)
        assert once == twice


# ── redact_dict ───────────────────────────────────────────────────────────────


class TestRedactDict:
    def test_sensitive_key_value_replaced(self):
        d = {"api_key": "my-secret", "other": "safe"}
        result = redact_dict(d)
        assert result["api_key"] == "***REDACTED***"
        assert result["other"] == "safe"

    def test_password_key(self):
        d = {"password": "hunter2"}
        assert redact_dict(d)["password"] == "***REDACTED***"

    def test_nested_dict(self):
        d = {"dataiku": {"api_key": "secret", "url": "https://example.com"}}
        result = redact_dict(d)
        assert result["dataiku"]["api_key"] == "***REDACTED***"
        assert result["dataiku"]["url"] == "https://example.com"

    def test_list_of_dicts(self):
        d = [{"token": "tok1"}, {"token": "tok2"}]
        result = redact_dict(d)
        assert result[0]["token"] == "***REDACTED***"
        assert result[1]["token"] == "***REDACTED***"

    def test_string_value_runs_redact_value(self):
        d = {"log_msg": "api_key=secret inside a log"}
        result = redact_dict(d)
        assert "secret" not in result["log_msg"]

    def test_non_sensitive_keys_untouched(self):
        d = {"url": "https://example.com", "timeout": 30}
        assert redact_dict(d) == {"url": "https://example.com", "timeout": 30}

    def test_integer_value_unchanged(self):
        d = {"port": 5432}
        assert redact_dict(d)["port"] == 5432

    def test_none_value(self):
        d = {"api_key": None}
        # None is not a string, so sensitive key check bypasses
        result = redact_dict(d)
        assert result["api_key"] == "***REDACTED***"

    def test_case_insensitive_key_matching(self):
        for key in ["API_KEY", "Api_Key", "PASSWORD", "Token"]:
            d = {key: "secret"}
            result = redact_dict(d)
            assert result[key] == "***REDACTED***", f"Key {key!r} not redacted"

    def test_all_sensitive_keys_redacted(self):
        d = {k: "secret_value" for k in _SENSITIVE_KEYS}
        result = redact_dict(d)
        for k in _SENSITIVE_KEYS:
            assert result[k] == "***REDACTED***", f"Key {k!r} not redacted"

    def test_deeply_nested(self):
        d = {"a": {"b": {"c": {"d": {"token": "deep_secret"}}}}}
        result = redact_dict(d)
        assert result["a"]["b"]["c"]["d"]["token"] == "***REDACTED***"

    def test_non_dict_passthrough(self):
        assert redact_dict(42) == 42
        assert redact_dict(3.14) == 3.14
        assert redact_dict(True) is True

    def test_original_not_mutated(self):
        original = {"api_key": "secret", "url": "https://example.com"}
        _ = redact_dict(original)
        assert original["api_key"] == "secret"  # unchanged


# ── redaction_log_processor ───────────────────────────────────────────────────


class TestRedactionLogProcessor:
    def test_redacts_event_dict(self):
        event = {"event": "user_login", "api_key": "supersecret", "user": "alice"}
        result = redaction_log_processor(None, "info", event)
        assert result["api_key"] == "***REDACTED***"
        assert result["user"] == "alice"

    def test_nested_log_event(self):
        event = {"event": "config_loaded", "config": {"password": "letmein"}}
        result = redaction_log_processor(None, "info", event)
        assert result["config"]["password"] == "***REDACTED***"

    def test_clean_event_unchanged(self):
        event = {"event": "startup", "version": "1.0.0"}
        result = redaction_log_processor(None, "info", event)
        assert result == event

    def test_processor_accepts_any_method_string(self):
        for method in ["debug", "info", "warning", "error", "critical"]:
            event = {"token": "tok"}
            result = redaction_log_processor(None, method, event)
            assert result["token"] == "***REDACTED***"


# ── SecretPolicyChecker ───────────────────────────────────────────────────────


class TestSecretPolicyChecker:
    def _checker(self):
        return SecretPolicyChecker()

    # ── check() returns violations list ──────────────────────

    def test_clean_config_no_violations(self):
        config = {
            "dataiku": {
                "url": "https://dss.example.com",
                "api_key": "${DATAIKU_API_KEY}",
                "verify_ssl": True,
            }
        }
        assert self._checker().check(config) == []

    def test_env_ref_dollar_brace_allowed(self):
        config = {"password": "${DB_PASSWORD}"}
        assert self._checker().check(config) == []

    def test_env_ref_env_colon_allowed(self):
        config = {"secret": "env:MY_SECRET_VAR"}
        assert self._checker().check(config) == []

    def test_vault_ref_allowed(self):
        config = {"token": "vault:secret/data/myapp#token"}
        assert self._checker().check(config) == []

    def test_plain_api_key_is_violation(self):
        config = {"api_key": "plaintext-secret-value"}
        violations = self._checker().check(config)
        assert len(violations) == 1
        assert "api_key" in violations[0]

    def test_plain_password_is_violation(self):
        config = {"password": "hunter2"}
        violations = self._checker().check(config)
        assert len(violations) == 1

    def test_nested_violation(self):
        config = {"dataiku": {"api_key": "hardcoded-secret"}}
        violations = self._checker().check(config)
        assert len(violations) == 1
        assert "dataiku.api_key" in violations[0]

    def test_multiple_violations(self):
        config = {
            "dataiku": {"api_key": "secret1"},
            "fabric": {"client_secret": "secret2"},
        }
        violations = self._checker().check(config)
        assert len(violations) == 2

    def test_empty_string_not_a_violation(self):
        config = {"password": ""}
        assert self._checker().check(config) == []

    def test_none_value_not_a_violation(self):
        config = {"api_key": None}
        assert self._checker().check(config) == []

    def test_integer_value_not_a_violation(self):
        config = {"token": 42}
        assert self._checker().check(config) == []

    def test_deeply_nested_violation(self):
        config = {"a": {"b": {"c": {"password": "deep_secret"}}}}
        violations = self._checker().check(config)
        assert len(violations) == 1
        assert "a.b.c.password" in violations[0]

    def test_list_of_dicts(self):
        config = {"connections": [{"api_key": "secret"}]}
        violations = self._checker().check(config)
        assert len(violations) == 1

    # ── assert_clean() raises PolicyViolation ─────────────────

    def test_assert_clean_raises_on_violation(self):
        config = {"api_key": "plaintext"}
        with pytest.raises(PolicyViolation) as exc_info:
            self._checker().assert_clean(config)
        assert "api_key" in str(exc_info.value)

    def test_assert_clean_no_error_on_clean(self):
        config = {"api_key": "${DATAIKU_API_KEY}"}
        self._checker().assert_clean(config)  # should not raise

    def test_policy_violation_message_contains_all_violations(self):
        config = {"password": "p1", "token": "t1"}
        with pytest.raises(PolicyViolation) as exc_info:
            self._checker().assert_clean(config)
        msg = str(exc_info.value)
        assert "password" in msg
        assert "token" in msg

    # ── Real-world config patterns ─────────────────────────────

    def test_sample_config_with_env_vars_passes(self):
        """Simulates a typical config.yaml with all env-var references — must pass."""
        config = {
            "dataiku": {
                "url": "https://dss.company.com",
                "api_key": "${DATAIKU_API_KEY}",
                "verify_ssl": True,
                "ca_bundle_path": None,
            },
            "fabric": {
                "workspace_id": "some-guid-here",
                "tenant_id": "${AZURE_TENANT_ID}",
                "client_id": "${AZURE_CLIENT_ID}",
                "client_secret": "${AZURE_CLIENT_SECRET}",
            },
            "migration": {
                "output_dir": "./output",
                "max_concurrent_agents": 4,
            },
        }
        assert self._checker().check(config) == []

    def test_sample_config_with_hardcoded_secrets_fails(self):
        """Config with hardcoded credentials must produce violations."""
        config = {
            "dataiku": {"api_key": "dss-abc123def456"},
            "fabric": {"client_secret": "mySuperSecretAzureValue"},
        }
        violations = self._checker().check(config)
        assert len(violations) >= 2

    def test_custom_sensitive_keys(self):
        """Users can extend the sensitive key set."""
        checker = SecretPolicyChecker(sensitive_keys=frozenset({"my_custom_key"}))
        violations = checker.check({"my_custom_key": "some-value"})
        assert len(violations) == 1

    def test_path_reported_correctly_with_nesting(self):
        config = {
            "level1": {
                "level2": {
                    "api_key": "hard-coded-value"
                }
            }
        }
        violations = self._checker().check(config)
        assert violations[0].startswith("config.level1.level2.api_key")
