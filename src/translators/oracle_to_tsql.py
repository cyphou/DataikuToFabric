"""Oracle → T-SQL custom translation rules for constructs sqlglot doesn't handle."""

from __future__ import annotations

import re

from src.core.logger import get_logger

logger = get_logger(__name__)


def apply_oracle_rules(sql: str) -> tuple[str, list[str]]:
    """
    Apply Oracle-specific translation rules beyond sqlglot's capabilities.

    Returns:
        Tuple of (translated_sql, review_flags).
    """
    review_flags: list[str] = []
    result = sql

    # ── DECODE → CASE ──────────────────────────────────────────────────
    result = _translate_decode(result)

    # ── NVL2(expr, val_if_not_null, val_if_null) → IIF(expr IS NOT NULL, ...) ──
    result = re.sub(
        r'\bNVL2\s*\(\s*([^,]+),\s*([^,]+),\s*([^)]+)\)',
        r'IIF(\1 IS NOT NULL, \2, \3)',
        result,
        flags=re.IGNORECASE,
    )

    # ── TO_DATE('str', 'fmt') → CONVERT(DATE, 'str', style) ───────────
    def _replace_to_date(match: re.Match) -> str:
        value = match.group(1).strip()
        fmt = match.group(2).strip().strip("'\"")
        simple_formats = {
            "YYYY-MM-DD": "23",
            "DD-MON-YYYY": "106",
            "MM/DD/YYYY": "101",
            "YYYY/MM/DD": "111",
        }
        style = simple_formats.get(fmt.upper())
        if style:
            return f"CONVERT(DATE, {value}, {style})"
        review_flags.append(f"Complex Oracle date format '{fmt}' — verify conversion")
        return f"CONVERT(DATE, {value}) /* Oracle format: {fmt} */"

    result = re.sub(
        r'\bTO_DATE\s*\(\s*([^,]+),\s*([^)]+)\)',
        _replace_to_date,
        result,
        flags=re.IGNORECASE,
    )

    # ── TO_CHAR(date, fmt) → FORMAT(date, fmt) ────────────────────────
    result = re.sub(
        r'\bTO_CHAR\s*\(\s*([^,]+),\s*([^)]+)\)',
        r'FORMAT(\1, \2)',
        result,
        flags=re.IGNORECASE,
    )

    # ── TO_NUMBER(expr) → CAST(expr AS FLOAT) ─────────────────────────
    result = re.sub(
        r'\bTO_NUMBER\s*\(\s*([^)]+)\)',
        r'CAST(\1 AS FLOAT)',
        result,
        flags=re.IGNORECASE,
    )

    # ── MINUS → EXCEPT ────────────────────────────────────────────────
    result = re.sub(r'\bMINUS\b', 'EXCEPT', result, flags=re.IGNORECASE)

    # ── SEQUENCES: seq.NEXTVAL → NEXT VALUE FOR seq ────────────────────
    result = re.sub(
        r'\b(\w+)\.NEXTVAL\b',
        r'NEXT VALUE FOR \1',
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(
        r'\b(\w+)\.CURRVAL\b',
        r"(SELECT current_value FROM sys.sequences WHERE name = '\1')",
        result,
        flags=re.IGNORECASE,
    )

    # ── LISTAGG → STRING_AGG ──────────────────────────────────────────
    result = re.sub(
        r'\bLISTAGG\s*\(\s*([^,]+),\s*([^)]+)\)',
        r'STRING_AGG(\1, \2)',
        result,
        flags=re.IGNORECASE,
    )

    # ── FROM DUAL → (removed) ─────────────────────────────────────────
    result = re.sub(r'\s+FROM\s+DUAL\b', '', result, flags=re.IGNORECASE)

    # ── RETURNING … INTO → OUTPUT … (flag for manual review) ─────────
    returning_match = re.search(
        r'\bRETURNING\s+(.+?)\s+INTO\s+(.+)',
        result,
        re.IGNORECASE | re.DOTALL,
    )
    if returning_match:
        cols = returning_match.group(1).strip()
        result = re.sub(
            r'\bRETURNING\s+.+?\s+INTO\s+.+',
            f'OUTPUT INSERTED.{cols}',
            result,
            flags=re.IGNORECASE | re.DOTALL,
        )
        review_flags.append("RETURNING…INTO → OUTPUT — verify column references")

    # ── (+) outer join syntax → flag ──────────────────────────────────
    if re.search(r'\(\+\)', result):
        review_flags.append("Contains Oracle (+) outer join syntax — rewrite as LEFT/RIGHT JOIN")

    # ── CONNECT BY → flag for recursive CTE ───────────────────────────
    if re.search(r'\bCONNECT\s+BY\b', result, re.IGNORECASE):
        review_flags.append(
            "Contains CONNECT BY hierarchical query — rewrite as recursive CTE"
        )

    # ── MERGE statement adjustments ────────────────────────────────────
    # Oracle MERGE uses "ON (condition)" — T-SQL uses "ON condition" (no extra parens required but valid)
    # No regex needed; just flag complex MERGE for review
    if re.search(r'\bMERGE\s+INTO\b', result, re.IGNORECASE):
        review_flags.append("Contains MERGE statement — verify T-SQL MERGE syntax compatibility")

    # ── ROWID → flag ──────────────────────────────────────────────────
    if re.search(r'\bROWID\b', result, re.IGNORECASE):
        review_flags.append("Uses Oracle ROWID — no direct T-SQL equivalent, consider %%physloc%%")

    # ── Flag PL/SQL blocks ────────────────────────────────────────────
    if re.search(r'\b(BEGIN|DECLARE|EXCEPTION)\b', result, re.IGNORECASE):
        review_flags.append("Contains PL/SQL block — requires manual conversion to T-SQL")

    # ── Flag DBMS_* packages ──────────────────────────────────────────
    if re.search(r'\bDBMS_\w+', result, re.IGNORECASE):
        review_flags.append("Uses Oracle DBMS_* package — no direct T-SQL equivalent")

    # ── Flag database links ───────────────────────────────────────────
    if re.search(r'@\w+', result):
        review_flags.append("Contains database link (@dblink) — requires Fabric linked service")

    # ── Flag materialized views ───────────────────────────────────────
    if re.search(r'\bMATERIALIZED\s+VIEW\b', result, re.IGNORECASE):
        review_flags.append("Contains materialized view — use indexed view or Spark table")

    # ── Flag Oracle hints ─────────────────────────────────────────────
    if re.search(r'/\*\+\s*\w+', result):
        review_flags.append("Contains Oracle optimizer hints — remove or adapt for Fabric")

    return result, review_flags


# ── Private helpers ────────────────────────────────────────────────────────


def _translate_decode(sql: str) -> str:
    """Translate Oracle DECODE(expr, search, result, …, default) → CASE.

    Handles nested DECODE calls recursively.
    """
    pattern = re.compile(r'\bDECODE\s*\(', re.IGNORECASE)

    while pattern.search(sql):
        match = pattern.search(sql)
        if not match:
            break

        start = match.start()
        # Find the matching closing paren
        paren_start = match.end() - 1  # position of '('
        end = _find_matching_paren(sql, paren_start)
        if end == -1:
            break

        inner = sql[paren_start + 1 : end]
        args = _split_top_level(inner, ',')

        if len(args) < 3:
            break  # malformed DECODE, skip

        expr = args[0].strip()
        case_parts = ["CASE"]
        i = 1
        while i + 1 < len(args):
            search_val = args[i].strip()
            result_val = args[i + 1].strip()
            case_parts.append(f"WHEN {expr} = {search_val} THEN {result_val}")
            i += 2

        # Remaining arg is the default
        if i < len(args):
            default_val = args[i].strip()
            case_parts.append(f"ELSE {default_val}")

        case_parts.append("END")
        replacement = " ".join(case_parts)

        sql = sql[:start] + replacement + sql[end + 1 :]

    return sql


def _find_matching_paren(sql: str, start: int) -> int:
    """Return index of closing paren matching the open paren at *start*."""
    depth = 0
    in_string = False
    quote_char = ""
    for i in range(start, len(sql)):
        ch = sql[i]
        if in_string:
            if ch == quote_char:
                in_string = False
            continue
        if ch in ("'", '"'):
            in_string = True
            quote_char = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _split_top_level(text: str, sep: str) -> list[str]:
    """Split *text* on *sep* only at the top level of parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_string = False
    quote_char = ""

    for ch in text:
        if in_string:
            current.append(ch)
            if ch == quote_char:
                in_string = False
            continue
        if ch in ("'", '"'):
            in_string = True
            quote_char = ch
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)

    parts.append("".join(current))
    return parts
