"""Unit tests for logic that needs no database. Run: python -m pytest tests"""
import decimal
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake_source as sf  # noqa: E402
import sync  # noqa: E402
import targets  # noqa: E402
from change_capture import _saved_watermark, state_key  # noqa: E402


def args(*argv):
    a = sync.parse_args(["--source", "DB.S.T", "--target", "T", *argv])
    return a, sync.validate(a)


# --- option validation ---------------------------------------------------------

def test_valid_defaults():
    a, err = args()
    assert err is None and a.commit_rows == sync.DEFAULT_COMMIT_ROWS


@pytest.mark.parametrize("argv, message", [
    (["--mode", "upsert"], "requires --key-cols"),
    (["--change-capture", "hwm", "--mode", "upsert", "--key-cols", "ID"], "requires --hwm-col"),
    (["--change-capture", "hwm", "--hwm-col", "U"], "upsert or --mode append"),
    (["--change-capture", "stream", "--mode", "upsert", "--key-cols", "ID"], "--stream and --outbox"),
    (["--stage", "@S"], "only to --transport unload"),
    (["--transport", "unload", "--commit-rows", "5"], "only to --transport pull"),
    (["--outbox-retention-days", "-1"], "0 or more"),
])
def test_invalid_combinations(argv, message):
    assert message in args(*argv)[1]


@pytest.mark.parametrize("argv", [
    ["--target", "T; DROP TABLE X"],
    ["--key-cols", "ID)--"],
    ["--change-capture", "hwm", "--mode", "upsert", "--key-cols", "ID", "--hwm-col", "U OR 1=1"],
    ["--transport", "unload", "--stage", "@S'; REMOVE @x"],
])
def test_identifiers_rejected(argv):
    assert "not a valid" in args(*argv)[1]


def test_quoted_snowflake_names_and_stages_accepted():
    a = sync.parse_args(["--source", 'DB."My Schema".T', "--target", "dbo.T",
                         "--transport", "unload", "--stage", "@DB.S.STG/sub-dir/x"])
    assert sync.validate(a) is None
    a = sync.parse_args(["--source", "DB.S.T", "--target", "T", "--transport", "unload",
                         "--stage", "@~/simple_reverse_etl"])
    assert sync.validate(a) is None


# --- query builders -----------------------------------------------------------------

def test_full_and_hwm_queries_ordered_by_key():
    assert sf.build_full_query("S", ["A", "B"]) == "SELECT * FROM S ORDER BY A, B"
    q = sf.build_hwm_query("S", "U", has_watermark=True, key_columns=["ID"])
    assert q == "SELECT * FROM S WHERE U > %(watermark)s AND U <= %(ceiling)s ORDER BY ID"
    assert "watermark" not in sf.build_hwm_query("S", "U", has_watermark=False)


def test_outbox_reduction_keeps_update_delete_half():
    q = sf.build_outbox_changes_query("OB", ["ID"])
    # The DELETE half of an update must not be filtered out: it is the only row
    # for the old key when an UPDATE changed a key column.
    assert "_CDC_ISUPDATE" not in q
    assert "PARTITION BY ID ORDER BY _CDC_LOADED_AT DESC, IFF(_CDC_ACTION = 'INSERT', 0, 1)" in q


# --- state and helpers --------------------------------------------------------------

def test_state_key_and_legacy_lookup():
    assert state_key("S", "T") == "S -> T"
    assert _saved_watermark({"S -> T": {"watermark": 5}}, "S", "T") == 5
    assert _saved_watermark({"S": {"watermark": 4}}, "S", "T") == 4
    assert _saved_watermark({}, "S", "T") is None


def test_lock_name_limits():
    assert targets._lock_name("T", 64) == "simple_reverse_etl:T"
    long = targets._lock_name("x" * 100, 64)
    assert len(long) <= 64 and long.startswith("simple_reverse_etl:")


def test_mssql_decimals_sent_as_exact_text():
    d = decimal.Decimal("12345678901234567890123456789012345678")
    rows = targets.MSSQLTarget._exact_decimals([(1, d, None), (2, decimal.Decimal("1.50"), "x")])
    assert rows == [(1, "12345678901234567890123456789012345678", None), (2, "1.50", "x")]
    plain = [(1, "a")]
    assert targets.MSSQLTarget._exact_decimals(plain) is plain


def test_odbc_value_quoting():
    assert targets._odbc_value("p;w}d") == "{p;w}}d}"
    assert targets._q("a]b") == "[a]]b]"
