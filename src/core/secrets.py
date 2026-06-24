"""Phase 13 — Secrets redaction and credential policy enforcement.

Provides:
- ``REDACT_PATTERNS`` — compiled regexes for common secret patterns.
- ``redact_value(text)`` — replace secrets in a string with ``***REDACTED***``.
- ``redact_dict(obj)`` — deep-redact secrets from log event dicts.
- ``SecretPolicyChecker`` — fail-fast check that a config payload carries no plain secrets.
- ``install_redaction_processor()`` — plug the redactor into the structlog pipeline.
"""

from __future__ import annotations

import re
from typing import Any

from src.core.logger import get_logger

logger = get_logger(__name__)

# ── Redaction patterns ────────────────────────────────────────────────────────

#: Compiled patterns that match common secret values.
#: Each pattern targets a ``key=<secret>`` or ``"key": "<secret>"`` form, or a
#: bare token with enough entropy to be considered a secret.
REDACT_PATTERNS: list[re.Pattern[str]] = [
    # Generic key=value assignments (e.g. api_key=abc123, token=xxx)
    re.compile(
        r'(?i)(api[_\-]?key|api[_\-]?secret|auth[_\-]?token|access[_\-]?token'
        r'|client[_\-]?secret|bearer[_\-]?token|private[_\-]?key'
        r'|secret[_\-]?key|password|passwd|pwd|credential|sas[_\-]?token'
        r'|refresh[_\-]?token|id[_\-]?token)["\']?\s*[=:]\s*["\']?([^\s"\'\\,;}{>\]]+)',
        re.IGNORECASE,
    ),
    # Authorization header values
    re.compile(r'(?i)(authorization\s*[=:]\s*)(bearer\s+\S+)', re.IGNORECASE),
    # AWS keys
    re.compile(r'AKIA[0-9A-Z]{16}'),
    # GitHub personal access tokens
    re.compile(r'ghp_[0-9A-Za-z]{36,}'),
    # Azure SAS tokens
    re.compile(r'sv=\d{4}-\d{2}-\d{2}&[^\s"\']+sig=[^\s"\']+'),
]

_REDACTED = "***REDACTED***"

# Keys whose values should always be redacted (case-insensitive)
_SENSITIVE_KEYS: frozenset[str] = frozenset({
    "api_key", "apikey", "api_secret", "apisecret",
    "password", "passwd", "pwd",
    "secret", "client_secret", "token",
    "access_token", "auth_token", "bearer_token", "id_token",
    "refresh_token", "sas_token", "private_key",
    "credential", "credentials", "authorization",
})


# ── Core redaction helpers ────────────────────────────────────────────────────


def redact_value(text: str) -> str:
    """Return *text* with all recognised secret patterns replaced by ``***REDACTED***``.

    >>> redact_value("api_key=supersecret")
    'api_key=***REDACTED***'
    >>> redact_value("no secrets here")
    'no secrets here'
    """
    result = text
    for pattern in REDACT_PATTERNS:
        result = pattern.sub(_replace_match, result)
    return result


def _replace_match(m: re.Match[str]) -> str:
    """Substitution callback: keep the key part, redact the value."""
    groups = m.groups()
    if groups:
        # Keep the first group (key/prefix), replace the rest
        return groups[0] + _REDACTED
    return _REDACTED


def redact_dict(obj: Any, *, _depth: int = 0) -> Any:
    """Recursively redact secrets from a dict, list, or scalar.

    - Dict keys in ``_SENSITIVE_KEYS`` have their values replaced.
    - String values are passed through ``redact_value()``.
    - Nesting is limited to 20 levels to avoid infinite recursion.
    """
    if _depth > 20:
        return obj

    if isinstance(obj, dict):
        result: dict[str, Any] = {}
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() in _SENSITIVE_KEYS:
                result[k] = _REDACTED
            else:
                result[k] = redact_dict(v, _depth=_depth + 1)
        return result

    if isinstance(obj, list):
        return [redact_dict(item, _depth=_depth + 1) for item in obj]

    if isinstance(obj, str):
        return redact_value(obj)

    return obj


# ── Structlog processor ───────────────────────────────────────────────────────


def redaction_log_processor(
    logger_instance: Any,  # noqa: ARG001
    method: str,           # noqa: ARG001
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Structlog processor: redact all sensitive values from the event dict.

    Install via::

        structlog.configure(processors=[..., redaction_log_processor, ...])
    """
    return redact_dict(event_dict)  # type: ignore[return-value]


# ── Policy checker ────────────────────────────────────────────────────────────


class PolicyViolation(Exception):
    """Raised when the config payload contains a plain secret value."""


class SecretPolicyChecker:
    """Enforce that a config payload does not carry plain secret values.

    The checker flags any dict that contains a sensitive key whose value is
    a non-empty string (and not a recognised env-var reference pattern like
    ``${VAR_NAME}`` or ``env:VAR_NAME``).

    Usage::

        checker = SecretPolicyChecker()
        violations = checker.check(config_dict)
        if violations:
            raise PolicyViolation(...)
    """

    #: Pattern for valid env-var references — these are *not* plain secrets.
    _ENV_REF_PATTERN = re.compile(
        r'^\$\{[A-Z_][A-Z0-9_]*\}$'   # ${VAR_NAME}
        r'|^env:[A-Z_][A-Z0-9_]*$'    # env:VAR_NAME
        r'|^vault:[^\s]+$',            # vault:path/to/secret
        re.IGNORECASE,
    )

    def __init__(self, sensitive_keys: frozenset[str] | None = None) -> None:
        self._sensitive_keys = sensitive_keys or _SENSITIVE_KEYS

    def check(self, payload: dict[str, Any], *, path: str = "") -> list[str]:
        """Return a list of violation messages (empty if clean).

        Args:
            payload: Config dict to inspect (may be nested).
            path: Internal key-path used for error messages (leave blank on first call).

        Returns:
            List of violation strings, e.g.
            ``["config.dataiku.api_key: plain secret value detected"]``.
        """
        violations: list[str] = []
        self._scan(payload, path=path or "config", violations=violations)
        return violations

    def assert_clean(self, payload: dict[str, Any]) -> None:
        """Raise ``PolicyViolation`` if any plain secrets are found."""
        violations = self.check(payload)
        if violations:
            msg = "Secret policy violation — plain credentials found in config:\n" + "\n".join(
                f"  • {v}" for v in violations
            )
            logger.error("secret_policy_violation", violations=violations)
            raise PolicyViolation(msg)

    # ── Internals ─────────────────────────────────────────────

    def _scan(
        self, obj: Any, *, path: str, violations: list[str], _depth: int = 0
    ) -> None:
        if _depth > 20:
            return

        if isinstance(obj, dict):
            for k, v in obj.items():
                child_path = f"{path}.{k}" if path else k
                if isinstance(k, str) and k.lower() in self._sensitive_keys:
                    if self._is_plain_secret(v):
                        violations.append(f"{child_path}: plain secret value detected")
                else:
                    self._scan(v, path=child_path, violations=violations, _depth=_depth + 1)

        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                self._scan(item, path=f"{path}[{i}]", violations=violations, _depth=_depth + 1)

    @staticmethod
    def _is_plain_secret(value: Any) -> bool:
        """Return True if *value* looks like a literal secret (not an env-var ref)."""
        if not isinstance(value, str):
            return False
        stripped = value.strip()
        if not stripped:
            return False  # empty string is fine
        return not SecretPolicyChecker._ENV_REF_PATTERN.match(stripped)
