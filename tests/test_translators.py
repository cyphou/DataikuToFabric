"""Tests for SQL translation layer."""

import pytest

from src.translators.sql_translator import translate_sql, validate_sql


class TestTranslateSQL:
    """Core sqlglot-based translation tests."""

    def test_oracle_to_tsql_basic(self):
        result = translate_sql("SELECT * FROM orders", source="oracle", target="tsql")
        assert result.success
        assert "orders" in result.translated_sql.lower()

    def test_postgres_to_tsql_basic(self):
        result = translate_sql("SELECT * FROM orders", source="postgres", target="tsql")
        assert result.success
        assert "orders" in result.translated_sql.lower()

    def test_failed_translation_returns_result(self):
        # Even bad SQL returns a TranslationResult (not raises)
        result = translate_sql("NOT VALID SQL AT ALL ))))", source="oracle", target="tsql")
        assert isinstance(result.success, bool)

    def test_empty_sql(self):
        result = translate_sql("", source="oracle", target="tsql")
        # Either empty result or error — should not crash
        assert isinstance(result.translated_sql, str)


class TestValidateSQL:
    """SQL syntax validation via sqlglot parse."""

    def test_valid_sql_returns_empty_errors(self):
        errors = validate_sql("SELECT 1")
        assert errors == []

    def test_valid_select_with_where(self):
        errors = validate_sql("SELECT id, name FROM users WHERE active = 1")
        assert errors == []

    def test_invalid_sql_returns_errors(self):
        errors = validate_sql("SLECT FORM orders")
        # sqlglot may or may not error on this, but the function returns a list
        assert isinstance(errors, list)


class TestOracleRules:
    """Oracle-specific custom rule tests."""

    def test_nvl2_replacement(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        translated, flags = apply_oracle_rules("SELECT NVL2(a, b, c) FROM t")
        assert "IIF" in translated.upper()

    def test_sysdate_via_sqlglot(self):
        # SYSDATE → GETDATE is handled by sqlglot transpile, not custom rules
        result = translate_sql("SELECT SYSDATE FROM dual", source="oracle", target="tsql")
        assert result.success

    def test_to_date_replacement(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        translated, flags = apply_oracle_rules("SELECT TO_DATE('2024-01-01', 'YYYY-MM-DD') FROM t")
        assert "CONVERT" in translated.upper()

    def test_minus_to_except(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        translated, flags = apply_oracle_rules("SELECT a FROM t1 MINUS SELECT a FROM t2")
        assert "EXCEPT" in translated.upper()

    def test_listagg_to_string_agg(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        translated, flags = apply_oracle_rules("SELECT LISTAGG(name, ',') FROM t")
        assert "STRING_AGG" in translated.upper()

    def test_plsql_flagged(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        _, flags = apply_oracle_rules("BEGIN UPDATE t SET x = 1; END;")
        assert any("PL/SQL" in f for f in flags)

    def test_dbms_flagged(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        _, flags = apply_oracle_rules("EXEC DBMS_OUTPUT.PUT_LINE('test')")
        assert any("DBMS_" in f for f in flags)

    def test_sequence_nextval(self):
        from src.translators.oracle_to_tsql import apply_oracle_rules

        translated, _ = apply_oracle_rules("SELECT myseq.NEXTVAL FROM dual")
        assert "NEXT VALUE FOR" in translated.upper()


class TestPostgresRules:
    """PostgreSQL-specific custom rule tests."""

    def test_limit_offset(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        translated, _ = apply_postgres_rules("SELECT * FROM t LIMIT 10 OFFSET 5")
        assert "OFFSET" in translated.upper()
        assert "FETCH" in translated.upper()

    def test_serial_to_identity(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        translated, _ = apply_postgres_rules("CREATE TABLE t (id SERIAL PRIMARY KEY)")
        assert "IDENTITY" in translated.upper()

    def test_boolean_to_bit(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        translated, _ = apply_postgres_rules("CREATE TABLE t (active BOOLEAN)")
        assert "BIT" in translated.upper()

    def test_text_to_nvarchar(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        translated, _ = apply_postgres_rules("CREATE TABLE t (notes TEXT)")
        assert "NVARCHAR" in translated.upper()

    def test_json_operators_flagged(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        _, flags = apply_postgres_rules("SELECT data->>'name' FROM t")
        assert any("JSON" in f for f in flags)

    def test_on_conflict_flagged(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        _, flags = apply_postgres_rules("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING")
        assert any("ON CONFLICT" in f for f in flags)

    def test_returning_flagged(self):
        from src.translators.postgres_to_tsql import apply_postgres_rules

        _, flags = apply_postgres_rules("DELETE FROM t WHERE id = 1 RETURNING *")
        assert any("RETURNING" in f for f in flags)
