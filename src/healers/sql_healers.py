"""SQL healers — auto-fix common SQL migration issues."""

from __future__ import annotations

import re
from typing import Any

from src.healers.base_healer import BaseHealer, HealerCategory, HealResult


class NvlToIsNullHealer(BaseHealer):
    """Convert Oracle NVL() to T-SQL ISNULL()."""

    @property
    def name(self) -> str:
        return "nvl_to_isnull"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace Oracle NVL() with T-SQL ISNULL()"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"\bNVL\s*\(", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        fixed = re.sub(r"\bNVL\s*\(", "ISNULL(", content, flags=re.IGNORECASE)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced NVL() with ISNULL()",
            before=content,
            after=fixed,
        )


class SysdateToGetdateHealer(BaseHealer):
    """Convert Oracle SYSDATE to T-SQL GETDATE()."""

    @property
    def name(self) -> str:
        return "sysdate_to_getdate"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace Oracle SYSDATE with T-SQL GETDATE()"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"\bSYSDATE\b", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        fixed = re.sub(r"\bSYSDATE\b", "GETDATE()", content, flags=re.IGNORECASE)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced SYSDATE with GETDATE()",
            before=content,
            after=fixed,
        )


class DecodeToCaseHealer(BaseHealer):
    """Convert simple DECODE(x, a, b, c) to CASE WHEN."""

    @property
    def name(self) -> str:
        return "decode_to_case"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace Oracle DECODE() with T-SQL CASE expression"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"\bDECODE\s*\(", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        # Simple 4-arg DECODE: DECODE(x, a, b, c) -> CASE x WHEN a THEN b ELSE c END
        pattern = r"\bDECODE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)"

        def replacer(m: re.Match) -> str:
            expr, val, then_val, else_val = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
            return f"CASE {expr} WHEN {val} THEN {then_val} ELSE {else_val} END"

        fixed = re.sub(pattern, replacer, content, flags=re.IGNORECASE)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced DECODE() with CASE expression",
            before=content,
            after=fixed,
        )


class PgCastHealer(BaseHealer):
    """Convert PostgreSQL :: cast to CAST(x AS type)."""

    @property
    def name(self) -> str:
        return "pg_cast_to_cast"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace PostgreSQL ::type with CAST(x AS type)"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "::" in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        # Match word::type patterns
        pattern = r"(\b\w+)::([\w]+)"
        fixed = re.sub(pattern, r"CAST(\1 AS \2)", content)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced ::type with CAST(x AS type)",
            before=content,
            after=fixed,
        )


class IlikeHealer(BaseHealer):
    """Convert PostgreSQL ILIKE to LOWER() LIKE LOWER()."""

    @property
    def name(self) -> str:
        return "ilike_to_lower_like"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace PostgreSQL ILIKE with LOWER(x) LIKE LOWER(y)"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"\bILIKE\b", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        pattern = r"(\S+)\s+ILIKE\s+(\S+)"
        fixed = re.sub(
            pattern,
            lambda m: f"LOWER({m.group(1)}) LIKE LOWER({m.group(2)})",
            content,
            flags=re.IGNORECASE,
        )
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced ILIKE with LOWER() LIKE LOWER()",
            before=content,
            after=fixed,
        )


class StringConcatHealer(BaseHealer):
    """Convert PostgreSQL || concat to CONCAT()."""

    @property
    def name(self) -> str:
        return "concat_operator_to_concat"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace PostgreSQL || string concat with CONCAT()"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "||" in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        # Simple two-operand || replacement
        pattern = r"(\S+)\s*\|\|\s*(\S+)"
        fixed = re.sub(pattern, r"CONCAT(\1, \2)", content)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced || with CONCAT()",
            before=content,
            after=fixed,
        )


class RownumToRowNumberHealer(BaseHealer):
    """Convert Oracle ROWNUM to ROW_NUMBER() OVER (ORDER BY ...)."""

    @property
    def name(self) -> str:
        return "rownum_to_row_number"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace Oracle ROWNUM with ROW_NUMBER() OVER (ORDER BY 1)"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"\bROWNUM\b", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        fixed = re.sub(
            r"\bROWNUM\b",
            "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))",
            content,
            flags=re.IGNORECASE,
        )
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced ROWNUM with ROW_NUMBER()",
            before=content,
            after=fixed,
        )


class Nvl2ToCaseHealer(BaseHealer):
    """Convert Oracle NVL2(x, a, b) to CASE WHEN x IS NOT NULL THEN a ELSE b END."""

    @property
    def name(self) -> str:
        return "nvl2_to_case"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SQL

    @property
    def description(self) -> str:
        return "Replace Oracle NVL2() with CASE expression"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"\bNVL2\s*\(", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        pattern = r"\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)"

        def replacer(m: re.Match) -> str:
            expr, not_null_val, null_val = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            return f"CASE WHEN {expr} IS NOT NULL THEN {not_null_val} ELSE {null_val} END"

        fixed = re.sub(pattern, replacer, content, flags=re.IGNORECASE)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced NVL2() with CASE expression",
            before=content,
            after=fixed,
        )


def get_sql_healers() -> list[BaseHealer]:
    """Return all SQL healers."""
    return [
        NvlToIsNullHealer(),
        SysdateToGetdateHealer(),
        DecodeToCaseHealer(),
        PgCastHealer(),
        IlikeHealer(),
        StringConcatHealer(),
        RownumToRowNumberHealer(),
        Nvl2ToCaseHealer(),
    ]
