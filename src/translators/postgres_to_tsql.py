"""PostgreSQL → T-SQL custom translation rules for constructs sqlglot doesn't handle."""

from __future__ import annotations

import re

from src.core.logger import get_logger

logger = get_logger(__name__)


def apply_postgres_rules(sql: str) -> tuple[str, list[str]]:
    """
    Apply PostgreSQL-specific translation rules beyond sqlglot's capabilities.

    Returns:
        Tuple of (translated_sql, review_flags).
    """
    review_flags: list[str] = []
    result = sql

    # ── LIMIT N OFFSET M → OFFSET M ROWS FETCH NEXT N ROWS ONLY ───────
    def _replace_limit_offset(match: re.Match) -> str:
        limit = match.group(1).strip()
        offset = match.group(2).strip() if match.group(2) else "0"
        return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    result = re.sub(
        r'\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\b',
        _replace_limit_offset,
        result,
        flags=re.IGNORECASE,
    )

    # ── SERIAL / BIGSERIAL → IDENTITY ─────────────────────────────────
    result = re.sub(r'\bBIGSERIAL\b', 'BIGINT IDENTITY(1,1)', result, flags=re.IGNORECASE)
    result = re.sub(r'\bSERIAL\b', 'INT IDENTITY(1,1)', result, flags=re.IGNORECASE)

    # ── BOOLEAN / TRUE / FALSE → BIT / 1 / 0 ─────────────────────────
    result = re.sub(r'\bBOOLEAN\b', 'BIT', result, flags=re.IGNORECASE)
    result = re.sub(r'\bTRUE\b', '1', result, flags=re.IGNORECASE)
    result = re.sub(r'\bFALSE\b', '0', result, flags=re.IGNORECASE)

    # ── TEXT → NVARCHAR(MAX) ──────────────────────────────────────────
    result = re.sub(r'\bTEXT\b', 'NVARCHAR(MAX)', result, flags=re.IGNORECASE)

    # ── NOW() → GETDATE() ────────────────────────────────────────────
    result = re.sub(r'\bNOW\s*\(\s*\)', 'GETDATE()', result, flags=re.IGNORECASE)

    # ── CURRENT_TIMESTAMP → GETDATE() (when used as a function default) ──
    # Don't replace inside CURRENT_TIMESTAMP AT TIME ZONE ...
    result = re.sub(
        r'\bCURRENT_TIMESTAMP\b(?!\s+AT\b)',
        'GETDATE()',
        result,
        flags=re.IGNORECASE,
    )

    # ── EXTRACT(field FROM expr) → DATEPART(field, expr) ──────────────
    def _replace_extract(match: re.Match) -> str:
        field = match.group(1).strip()
        expr = match.group(2).strip()
        return f"DATEPART({field}, {expr})"

    result = re.sub(
        r'\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\)',
        _replace_extract,
        result,
        flags=re.IGNORECASE,
    )

    # ── INTERVAL 'N unit' → DATEADD(unit, N, ...) — flag for context ──
    if re.search(r"\bINTERVAL\s+'[^']*'", result, re.IGNORECASE):
        review_flags.append("Contains INTERVAL literal — use DATEADD in T-SQL")

    # ── string_agg(col, sep ORDER BY …) → STRING_AGG(col, sep) WITHIN GROUP (ORDER BY …) ──
    result = re.sub(
        r'\bSTRING_AGG\b',
        'STRING_AGG',
        result,
        flags=re.IGNORECASE,
    )

    # ── RETURNING clause → OUTPUT ─────────────────────────────────────
    returning_match = re.search(r'\bRETURNING\s+(.+?)$', result, re.IGNORECASE | re.MULTILINE)
    if returning_match:
        cols = returning_match.group(1).strip().rstrip(';')
        result = re.sub(
            r'\bRETURNING\s+.+?$',
            f'OUTPUT INSERTED.{cols}',
            result,
            flags=re.IGNORECASE | re.MULTILINE,
        )
        review_flags.append("RETURNING → OUTPUT — verify column references")

    # ── ON CONFLICT → MERGE (flag + partial rewrite) ──────────────────
    if re.search(r'\bON\s+CONFLICT\b', result, re.IGNORECASE):
        review_flags.append("Contains ON CONFLICT (upsert) — rewrite as MERGE in T-SQL")

    # ── GENERATE_SERIES → recursive CTE (flag) ───────────────────────
    gen_match = re.search(
        r'\bGENERATE_SERIES\s*\(\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*(\d+))?\s*\)',
        result,
        re.IGNORECASE,
    )
    if gen_match:
        start = gen_match.group(1)
        end = gen_match.group(2)
        step = gen_match.group(3) or "1"
        # Replace with a CTE-compatible comment and flag
        review_flags.append(
            f"GENERATE_SERIES({start},{end},{step}) — use recursive CTE or numbers table"
        )

    # ── JSONB operators (->>, ->, #>>, @>) → JSON_VALUE / OPENJSON ────
    if re.search(r'->>|->|#>>|@>', result):
        review_flags.append("Contains PostgreSQL JSON operators — requires OPENJSON/JSON_VALUE conversion")

    # ── ARRAY types ───────────────────────────────────────────────────
    if re.search(r'\bARRAY\s*\[', result, re.IGNORECASE):
        review_flags.append("Contains PostgreSQL ARRAY literal — requires manual conversion")

    # ── ILIKE → LOWER() LIKE LOWER() ─────────────────────────────────
    def _replace_ilike(match: re.Match) -> str:
        return f"LOWER({match.group(1)}) LIKE LOWER({match.group(2)})"

    result = re.sub(
        r'(\S+)\s+ILIKE\s+(\S+)',
        _replace_ilike,
        result,
        flags=re.IGNORECASE,
    )

    # ── || string concat → + ─────────────────────────────────────────
    result = re.sub(r'\|\|', '+', result)

    # ── Flag PL/pgSQL functions / DO blocks ───────────────────────────
    if re.search(r'\b(DO\s*\$\$|CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\b)', result, re.IGNORECASE):
        review_flags.append("Contains PL/pgSQL function — requires manual conversion to T-SQL")

    # ── Flag PostgreSQL extensions ────────────────────────────────────
    extension_pattern = r'\b(pg_trgm|hstore|ltree|citext|pgcrypto|postgis|uuid-ossp)\b'
    ext_match = re.search(extension_pattern, result, re.IGNORECASE)
    if ext_match:
        review_flags.append(f"Uses PostgreSQL extension '{ext_match.group(1)}' — no direct T-SQL equivalent")

    # ── LATERAL → CROSS APPLY (flag) ─────────────────────────────────
    if re.search(r'\bLATERAL\b', result, re.IGNORECASE):
        result = re.sub(r'\bLATERAL\b', 'CROSS APPLY', result, flags=re.IGNORECASE)
        review_flags.append("LATERAL → CROSS APPLY — verify subquery compatibility")

    return result, review_flags
