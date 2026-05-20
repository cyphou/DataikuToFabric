"""Schema healers — auto-fix common schema migration issues."""

from __future__ import annotations

import re
from typing import Any

from src.healers.base_healer import BaseHealer, HealerCategory, HealResult

# Oracle → T-SQL type mapping
_ORACLE_TYPE_MAP = {
    "NUMBER": "DECIMAL",
    "VARCHAR2": "NVARCHAR",
    "CLOB": "NVARCHAR(MAX)",
    "BLOB": "VARBINARY(MAX)",
    "DATE": "DATETIME2",
    "TIMESTAMP": "DATETIME2",
    "RAW": "VARBINARY",
    "LONG": "NVARCHAR(MAX)",
    "LONG RAW": "VARBINARY(MAX)",
}

# PostgreSQL → T-SQL type mapping
_PG_TYPE_MAP = {
    "SERIAL": "INT IDENTITY(1,1)",
    "BIGSERIAL": "BIGINT IDENTITY(1,1)",
    "BOOLEAN": "BIT",
    "TEXT": "NVARCHAR(MAX)",
    "JSONB": "NVARCHAR(MAX)",
    "JSON": "NVARCHAR(MAX)",
    "UUID": "UNIQUEIDENTIFIER",
    "BYTEA": "VARBINARY(MAX)",
    "TIMESTAMPTZ": "DATETIMEOFFSET",
    "INTERVAL": "NVARCHAR(100)",
}


class OracleTypeHealer(BaseHealer):
    """Convert Oracle data types in DDL to T-SQL types."""

    @property
    def name(self) -> str:
        return "oracle_type_to_tsql"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SCHEMA

    @property
    def description(self) -> str:
        return "Convert Oracle data types to T-SQL equivalents in DDL"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        upper = content.upper()
        return any(f" {t} " in upper or f" {t}(" in upper for t in _ORACLE_TYPE_MAP)

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        fixed = content
        for oracle_type, tsql_type in _ORACLE_TYPE_MAP.items():
            pattern = rf"\b{oracle_type}\b"
            fixed = re.sub(pattern, tsql_type, fixed, flags=re.IGNORECASE)

        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Converted Oracle data types to T-SQL",
            before=content,
            after=fixed,
        )


class PgTypeHealer(BaseHealer):
    """Convert PostgreSQL data types in DDL to T-SQL types."""

    @property
    def name(self) -> str:
        return "pg_type_to_tsql"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SCHEMA

    @property
    def description(self) -> str:
        return "Convert PostgreSQL data types to T-SQL equivalents in DDL"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        upper = content.upper()
        return any(f" {t} " in upper or f" {t}(" in upper or f" {t}," in upper for t in _PG_TYPE_MAP)

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        fixed = content
        for pg_type, tsql_type in _PG_TYPE_MAP.items():
            pattern = rf"\b{pg_type}\b"
            fixed = re.sub(pattern, tsql_type, fixed, flags=re.IGNORECASE)

        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Converted PostgreSQL data types to T-SQL",
            before=content,
            after=fixed,
        )


class NullableDefaultHealer(BaseHealer):
    """Add NOT NULL defaults where missing in CREATE TABLE."""

    @property
    def name(self) -> str:
        return "nullable_defaults"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.SCHEMA

    @property
    def description(self) -> str:
        return "Ensure columns have explicit NULL/NOT NULL in DDL"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return bool(re.search(r"CREATE\s+TABLE", content, re.IGNORECASE))

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        # This is informational — flag columns without explicit nullability
        lines = content.split("\n")
        fixed_lines = []
        changed = False
        for line in lines:
            stripped = line.strip().rstrip(",")
            if (
                stripped
                and not stripped.upper().startswith(("CREATE", "PRIMARY", "CONSTRAINT", ")", "(", "--", "/*"))
                and "NULL" not in stripped.upper()
                and re.match(r"\s*\w+\s+\w+", stripped)
            ):
                line = line.rstrip(",") + " NULL" + ("," if line.rstrip().endswith(",") else "")
                changed = True
            fixed_lines.append(line)

        fixed = "\n".join(fixed_lines)
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=changed,
            description="Added explicit NULL to columns without nullability",
            before=content,
            after=fixed,
        )


def get_schema_healers() -> list[BaseHealer]:
    """Return all schema healers."""
    return [
        OracleTypeHealer(),
        PgTypeHealer(),
        NullableDefaultHealer(),
    ]
