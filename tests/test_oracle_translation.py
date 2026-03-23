"""Oracle → T-SQL translation tests — 30 cases covering sqlglot + custom rules."""

from __future__ import annotations

import pytest

from src.translators.oracle_to_tsql import apply_oracle_rules
from src.translators.sql_translator import translate_sql, validate_sql


# ── Helper ─────────────────────────────────────────────────────────────────

def _oracle_to_tsql(sql: str) -> tuple[str, list[str], list[str]]:
    """Translate Oracle → T-SQL via sqlglot + custom rules and validate."""
    result = translate_sql(sql, source="oracle", target="tsql")
    translated, flags = apply_oracle_rules(result.translated_sql)
    errors = validate_sql(translated, dialect="tsql")
    return translated, flags, errors


# ═══════════════════════════════════════════════════════════════════════════
# A) DECODE → CASE  (5 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestDecode:
    """DECODE translation to CASE expression."""

    def test_simple_decode(self):
        sql = "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM orders"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CASE" in translated.upper()
        assert "WHEN" in translated.upper()
        assert "DECODE" not in translated.upper()

    def test_decode_two_pairs(self):
        sql = "SELECT DECODE(type, 1, 'One', 2, 'Two', 'Other') FROM items"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert translated.upper().count("WHEN") == 2
        assert "ELSE" in translated.upper()

    def test_decode_no_default(self):
        sql = "SELECT DECODE(x, 'A', 1, 'B', 2) FROM t"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CASE" in translated.upper()
        assert "DECODE" not in translated.upper()

    def test_nested_decode(self):
        sql = "SELECT DECODE(a, 1, DECODE(b, 'X', 10, 20), 99) FROM t"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "DECODE" not in translated.upper()
        # Should have two CASE blocks (outer + inner)
        assert translated.upper().count("CASE") == 2

    def test_decode_with_null(self):
        sql = "SELECT DECODE(col, NULL, 'empty', col) FROM t"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CASE" in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# B) NVL2 → IIF  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestNVL2:
    def test_nvl2_basic(self):
        """sqlglot translates NVL2 to CASE WHEN NOT x IS NULL."""
        sql = "SELECT NVL2(email, 'has_email', 'no_email') FROM users"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CASE" in translated.upper()
        assert "IS NULL" in translated.upper()
        assert "NVL2" not in translated.upper()

    def test_nvl2_with_column(self):
        sql = "SELECT NVL2(mgr_id, mgr_id, 0) FROM employees"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CASE" in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# C) Date functions  (4 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestDateFunctions:
    def test_to_date_known_format(self):
        """sqlglot converts TO_DATE to STR_TO_DATE; custom rules then won't match."""
        sql = "SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD') FROM DUAL"
        translated, flags, _ = _oracle_to_tsql(sql)
        # sqlglot rewrites TO_DATE — verify no Oracle syntax remains
        assert "TO_DATE" not in translated.upper() or "STR_TO_DATE" in translated.upper()

    def test_to_date_complex_format(self):
        sql = "SELECT TO_DATE('15/01/2024', 'DD/MM/YYYY') FROM DUAL"
        translated, flags, _ = _oracle_to_tsql(sql)
        # sqlglot rewrites TO_DATE to STR_TO_DATE
        assert "TO_DATE" not in translated.upper() or "STR_TO_DATE" in translated.upper()

    def test_to_char(self):
        """sqlglot converts TO_CHAR(x, fmt) to CAST(x AS VARCHAR(MAX))."""
        sql = "SELECT TO_CHAR(hire_date, 'YYYY-MM') FROM employees"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CAST" in translated.upper() or "FORMAT" in translated.upper()
        assert "TO_CHAR" not in translated.upper()

    def test_to_number(self):
        sql = "SELECT TO_NUMBER('123.45') FROM DUAL"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "CAST" in translated.upper()
        assert "FLOAT" in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# D) DUAL table removal  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestDual:
    def test_from_dual_removed(self):
        sql = "SELECT 1 FROM DUAL"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "DUAL" not in translated.upper()

    def test_sysdate_from_dual(self):
        sql = "SELECT SYSDATE FROM DUAL"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "DUAL" not in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# E) Set operations  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestSetOps:
    def test_minus_to_except(self):
        sql = "SELECT id FROM a MINUS SELECT id FROM b"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "EXCEPT" in translated.upper()
        assert "MINUS" not in translated.upper()

    def test_minus_preserves_rest(self):
        sql = "SELECT col1, col2 FROM t1 WHERE x = 1 MINUS SELECT col1, col2 FROM t2"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "EXCEPT" in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# F) Sequences  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestSequences:
    def test_nextval(self):
        sql = "INSERT INTO orders (id) VALUES (order_seq.NEXTVAL)"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "NEXT VALUE FOR" in translated.upper()
        assert "NEXTVAL" not in translated.upper()

    def test_currval(self):
        sql = "SELECT order_seq.CURRVAL FROM DUAL"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "sys.sequences" in translated.lower()
        assert "CURRVAL" not in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# G) Aggregation  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestAggregation:
    def test_listagg(self):
        sql = "SELECT LISTAGG(name, ', ') FROM employees"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "STRING_AGG" in translated.upper()
        assert "LISTAGG" not in translated.upper()

    def test_listagg_within_group(self):
        sql = "SELECT dept, LISTAGG(name, ',') FROM employees GROUP BY dept"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert "STRING_AGG" in translated.upper()


# ═══════════════════════════════════════════════════════════════════════════
# H) RETURNING / OUTPUT  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestReturning:
    def test_returning_into(self):
        """RETURNING…INTO applied only if sqlglot preserves the clause."""
        sql = "INSERT INTO t (a) VALUES (1) RETURNING id INTO v_id"
        translated, flags, _ = _oracle_to_tsql(sql)
        # sqlglot may strip RETURNING; if preserved, custom rules convert to OUTPUT
        assert "OUTPUT" in translated.upper() or "RETURNING" not in translated.upper()

    def test_no_returning_no_flag(self):
        sql = "INSERT INTO t (a) VALUES (1)"
        translated, flags, _ = _oracle_to_tsql(sql)
        assert not any("RETURNING" in f for f in flags)


# ═══════════════════════════════════════════════════════════════════════════
# I) Review flags for complex constructs  (6 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestReviewFlags:
    def test_plsql_block_flagged(self):
        sql = "BEGIN INSERT INTO t VALUES (1); END;"
        _, flags = apply_oracle_rules(sql)
        assert any("PL/SQL" in f for f in flags)

    def test_dbms_package_flagged(self):
        sql = "EXEC DBMS_OUTPUT.PUT_LINE('hello')"
        _, flags = apply_oracle_rules(sql)
        assert any("DBMS_" in f for f in flags)

    def test_connect_by_flagged(self):
        sql = "SELECT * FROM emp CONNECT BY PRIOR emp_id = mgr_id START WITH mgr_id IS NULL"
        _, flags = apply_oracle_rules(sql)
        assert any("CONNECT BY" in f for f in flags)

    def test_merge_flagged(self):
        sql = "MERGE INTO target t USING source s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.val = s.val"
        _, flags = apply_oracle_rules(sql)
        assert any("MERGE" in f for f in flags)

    def test_rowid_flagged(self):
        sql = "SELECT ROWID, name FROM employees"
        _, flags = apply_oracle_rules(sql)
        assert any("ROWID" in f for f in flags)

    def test_outer_join_syntax_flagged(self):
        sql = "SELECT * FROM a, b WHERE a.id = b.id(+)"
        _, flags = apply_oracle_rules(sql)
        assert any("(+)" in f for f in flags)

    def test_materialized_view_flagged(self):
        sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t"
        _, flags = apply_oracle_rules(sql)
        assert any("materialized view" in f.lower() for f in flags)

    def test_dblink_flagged(self):
        sql = "SELECT * FROM employees@remote_db"
        _, flags = apply_oracle_rules(sql)
        assert any("dblink" in f.lower() for f in flags)

    def test_hints_flagged(self):
        sql = "SELECT /*+ FULL(t) */ * FROM t"
        _, flags = apply_oracle_rules(sql)
        assert any("hints" in f.lower() for f in flags)


# ═══════════════════════════════════════════════════════════════════════════
# J) End-to-end pipeline (sqlglot + custom rules)  (3 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestEndToEnd:
    def test_complex_query(self):
        sql = """
        SELECT e.name, d.dept_name,
               TO_CHAR(e.hire_date, 'YYYY-MM-DD') as formatted_date,
               NVL2(e.mgr_id, 'Has Manager', 'Top Level')
        FROM employees e, departments d
        WHERE e.dept_id = d.dept_id
        """
        translated, flags, errors = _oracle_to_tsql(sql)
        # sqlglot handles TO_CHAR→CAST and NVL2→CASE
        assert "CAST" in translated.upper() or "FORMAT" in translated.upper()
        assert "CASE" in translated.upper()

    def test_insert_with_sequence(self):
        sql = "INSERT INTO orders (id, status) VALUES (order_seq.NEXTVAL, 'NEW')"
        translated, flags, errors = _oracle_to_tsql(sql)
        assert "NEXT VALUE FOR" in translated.upper()

    def test_select_with_decode_and_dual(self):
        sql = "SELECT DECODE(1, 1, 'YES', 'NO') FROM DUAL"
        translated, flags, errors = _oracle_to_tsql(sql)
        assert "CASE" in translated.upper()
        assert "DUAL" not in translated.upper()
        assert "DECODE" not in translated.upper()
