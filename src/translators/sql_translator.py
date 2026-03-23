"""SQL translation engine — core sqlglot-based dialect translation."""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot

from src.core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TranslationResult:
    """Result of a SQL translation operation."""

    source_sql: str
    translated_sql: str
    source_dialect: str
    target_dialect: str
    success: bool = True
    review_flags: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def translate_sql(
    sql: str,
    source: str = "oracle",
    target: str = "tsql",
    pretty: bool = True,
) -> TranslationResult:
    """
    Translate SQL from one dialect to another using sqlglot.

    Args:
        sql: Source SQL statement.
        source: Source dialect (oracle, postgres, mysql, hive, spark).
        target: Target dialect (tsql, spark).
        pretty: Whether to pretty-print the output.

    Returns:
        TranslationResult with translated SQL and any flags/errors.
    """
    review_flags: list[str] = []
    errors: list[str] = []

    try:
        results = sqlglot.transpile(
            sql,
            read=source,
            write=target,
            pretty=pretty,
        )

        if not results:
            return TranslationResult(
                source_sql=sql,
                translated_sql="",
                source_dialect=source,
                target_dialect=target,
                success=False,
                errors=["sqlglot returned empty result"],
            )

        translated = results[0]

        # Validate the output parses cleanly
        try:
            sqlglot.parse(translated, dialect=target)
        except sqlglot.errors.ParseError as e:
            review_flags.append(f"Output SQL may have syntax issues: {e}")

        return TranslationResult(
            source_sql=sql,
            translated_sql=translated,
            source_dialect=source,
            target_dialect=target,
            success=True,
            review_flags=review_flags,
        )

    except sqlglot.errors.ParseError as e:
        logger.warning("sql_parse_error", dialect=source, error=str(e))
        return TranslationResult(
            source_sql=sql,
            translated_sql=sql,  # Return original on failure
            source_dialect=source,
            target_dialect=target,
            success=False,
            errors=[f"Parse error: {e}"],
            review_flags=["MANUAL_REVIEW: SQL could not be parsed automatically"],
        )

    except Exception as e:
        logger.error("sql_translation_error", error=str(e))
        return TranslationResult(
            source_sql=sql,
            translated_sql=sql,
            source_dialect=source,
            target_dialect=target,
            success=False,
            errors=[str(e)],
        )


def validate_sql(sql: str, dialect: str = "tsql") -> list[str]:
    """
    Validate SQL syntax by parsing with sqlglot.

    Args:
        sql: The SQL statement to validate.
        dialect: The SQL dialect to validate against.

    Returns:
        List of error messages. Empty list means valid SQL.
    """
    errors: list[str] = []
    try:
        parsed = sqlglot.parse(sql, dialect=dialect)
        if not parsed or all(p is None for p in parsed):
            errors.append("SQL parsed to empty result")
    except sqlglot.errors.ParseError as e:
        errors.append(f"Syntax error: {e}")
    except Exception as e:
        errors.append(f"Validation error: {e}")
    return errors


def translate_multi_statement(
    sql: str,
    source: str = "oracle",
    target: str = "tsql",
    pretty: bool = True,
) -> TranslationResult:
    """Translate multi-statement SQL by splitting on semicolons.

    Handles:
    - Multiple ;-separated statements
    - BEGIN…END blocks (kept intact)
    - CTE (WITH … AS) queries
    """
    review_flags: list[str] = []
    errors: list[str] = []

    # Split on semicolons but preserve BEGIN…END blocks
    statements = _split_statements(sql)

    if len(statements) <= 1:
        return translate_sql(sql, source=source, target=target, pretty=pretty)

    translated_parts: list[str] = []
    for i, stmt in enumerate(statements):
        stmt = stmt.strip()
        if not stmt:
            continue

        result = translate_sql(stmt, source=source, target=target, pretty=pretty)
        translated_parts.append(result.translated_sql)
        review_flags.extend(result.review_flags)
        errors.extend(result.errors)

    combined = ";\n\n".join(translated_parts)
    all_success = len(errors) == 0

    return TranslationResult(
        source_sql=sql,
        translated_sql=combined,
        source_dialect=source,
        target_dialect=target,
        success=all_success,
        review_flags=review_flags,
        errors=errors,
    )


def _split_statements(sql: str) -> list[str]:
    """Split SQL into individual statements on semicolons.

    Keeps BEGIN…END blocks intact (doesn't split inside them).
    """
    statements: list[str] = []
    current: list[str] = []
    depth = 0  # Track BEGIN/END nesting

    for line in sql.split("\n"):
        stripped = line.strip().upper()

        # Track BEGIN/END nesting
        if stripped.startswith("BEGIN"):
            depth += 1
        if stripped.startswith("END"):
            depth = max(0, depth - 1)

        # Only split on ; when not inside a BEGIN…END block
        if ";" in line and depth == 0:
            parts = line.split(";")
            current.append(parts[0])
            statements.append("\n".join(current))
            current = []
            # Handle remaining parts after the semicolon
            for part in parts[1:]:
                if part.strip():
                    current.append(part)
        else:
            current.append(line)

    if current:
        remaining = "\n".join(current).strip()
        if remaining:
            statements.append(remaining)

    return statements


def validate_sql(sql: str, dialect: str = "tsql") -> list[str]:
    """Validate SQL syntax by parsing it. Returns list of errors (empty if valid)."""
    errors: list[str] = []
    try:
        sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        errors.append(str(e))
    return errors
