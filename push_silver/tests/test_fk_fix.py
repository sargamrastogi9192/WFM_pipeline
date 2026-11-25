"""dhms_push/fk_fix.py with a fake database (no Postgres needed):  cd push_silver && python -m pytest -q tests"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dhms_push import config, fk_fix                         # noqa: E402
from dhms_push.compare import compare                        # noqa: E402
from dhms_push.schema import TableDef                        # noqa: E402

FK = {"name": "fk_rutting_c10_parent", "columns": ["parent_id"], "ref_schema": "public",
      "ref_table": "master_rutting_parent", "ref_columns": ["defect_id"], "definition": ""}


class FakeConn:
    """Answers the 'SELECT DISTINCT ref FROM table WHERE ref = ANY(...)' queries from a dict of table values."""
    def __init__(self, db):
        self.db = db

    def execute(self, sql, params):
        table = str(sql).split('FROM "public"."')[1].split('"')[0]
        have = self.db.get(table, set())
        rows = [(v,) for v in params["fk_values"] if v in have]
        return type("R", (), {"__iter__": lambda s: iter(rows), "scalar": lambda s: 0})()


def col(name, nullable):
    return {"position": 1, "name": name, "type": "text", "data_type": "text", "udt_name": "text", "max_length": None,
            "numeric_precision": None, "numeric_scale": None, "nullable": nullable, "default": None,
            "identity": None, "generated": None, "enum_values": None, "comment": None}


def tables(nullable=True):
    parent = TableDef(schema="public", table="master_rutting_parent", exists=True,
                      columns=[col("id", False), col("defect_id", True)])
    child = TableDef(schema="public", table="master_rutting_child_10m", exists=True,
                     columns=[col("id", False), col("parent_id", nullable)], primary_key=["id"],
                     foreign_keys=[FK])
    return {"master_rutting_parent": parent, "master_rutting_child_10m": child}


def frames():
    return {"master_rutting_parent": pd.DataFrame({"id": ["p1", "p2", "p3"], "defect_id": ["U_LHS_0_1000", None, None]}),
            "master_rutting_child_10m": pd.DataFrame({"id": ["c1", "c2", "c3", "c4"],
                                                      "parent_id": ["U_LHS_0_1000", "U_LHS_1000_2000",
                                                                    "U_RHS_0_1000", "OLD_IN_DB"]})}


def test_unmatched_nullable_fk_set_to_null():
    prepared = frames()
    issues = fk_fix.resolve(FakeConn({"master_rutting_parent": {"OLD_IN_DB"}}), "public", tables(), prepared,
                            {}, "append")
    child = prepared["master_rutting_child_10m"]
    # in this push (breaching parent) / in the database: kept; the two non-breach blocks: NULL
    assert child["parent_id"].tolist() == ["U_LHS_0_1000", None, None, "OLD_IN_DB"]
    assert len(issues) == 1 and issues[0]["severity"] == "WARNING" and issues[0]["rows"] == 2


def test_not_null_column_stays_and_is_a_blocker():
    prepared = frames()
    defs = tables(nullable=False)
    assert fk_fix.resolve(FakeConn({}), "public", defs, prepared, {}, "append") == []
    assert prepared["master_rutting_child_10m"]["parent_id"].isna().sum() == 0


def test_switch_off_keeps_values(monkeypatch):
    monkeypatch.setattr(config, "NULL_UNMATCHED_FK", False)
    prepared = frames()
    assert fk_fix.resolve(FakeConn({}), "public", tables(), prepared, {}, "append") == []
    assert prepared["master_rutting_child_10m"]["parent_id"].isna().sum() == 0


def test_compare_counts_parents_inserted_by_the_push():
    """A child pointing at a parent row of the same push is not a blocker (only DB values were checked before)."""
    prepared = frames()
    fk_fix.resolve(FakeConn({"master_rutting_parent": {"OLD_IN_DB"}}), "public", tables(), prepared, {}, "append")
    db = FakeConn({"master_rutting_parent": {"OLD_IN_DB"}})
    db.execute = lambda sql, params: type("R", (), {"__iter__": lambda s: iter(
        [(v,) for v in params.get("v", []) if v == "OLD_IN_DB"]), "scalar": lambda s: 0})()
    _, issues = compare(tables()["master_rutting_child_10m"], prepared["master_rutting_child_10m"], db,
                        scope=None, mode="append", pushed=prepared)
    assert not [i for i in issues if i["severity"] == "BLOCKER" and "foreign key" in i["issue"]]
