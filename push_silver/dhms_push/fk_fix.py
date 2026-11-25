"""Foreign keys of the pushed tables, resolved before the check.

A child row's foreign key value is fine when the referenced row
  - is inserted by this same push (the parents are inserted first, config.TARGETS order), or
  - is already in the database and stays there (in 'replace' mode the survey's own rows of a pushed table are
    deleted first, so those do not count).
A value found in neither:
  - nullable column and config.NULL_UNMATCHED_FK  -> set to NULL, reported as a WARNING (row count + examples)
  - otherwise                                     -> left as it is; compare() reports the BLOCKER
Example: master_rutting_child_10m.parent_id -> master_rutting_parent.defect_id. A parent gets a defect_id only when
its 1 km block breaches, so the children of a non-breaching block point at nothing -> parent_id NULL (the block is
still on the child row: parent_start_chainage / parent_end_chainage / side).
"""

from sqlalchemy import text

from . import config
from .compare import _is_null as _null

KEY_CHUNK = 5000
MAX_EXAMPLES = 3


def _txt(v) -> str:
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return str(v)


def pushed_values(prepared: dict, schema: str, fk: dict) -> set:
    """Values of the referenced column inserted by this push (as text)."""
    ref_table, ref = fk["ref_table"], fk["ref_columns"][0]
    df = prepared.get(ref_table)
    if fk["ref_schema"] != schema or df is None or ref not in df.columns:
        return set()
    return {_txt(v) for v in df[ref] if not _null(v)}


def db_values(conn, fk: dict, wanted: list, exclude=None) -> set:
    """The wanted values that the referenced table has (exclude = (SQL condition, params) of rows that the push
    deletes first)."""
    found, ref = set(), fk["ref_columns"][0]
    extra, params = (f" AND NOT ({exclude[0]})", dict(exclude[1])) if exclude else ("", {})
    for i in range(0, len(wanted), KEY_CHUNK):
        rows = conn.execute(text(f'SELECT DISTINCT "{ref}"::text FROM "{fk["ref_schema"]}"."{fk["ref_table"]}" '
                                 f'WHERE "{ref}"::text = ANY(:fk_values){extra}'),
                            {**params, "fk_values": wanted[i:i + KEY_CHUNK]})
        found |= {r[0] for r in rows}
    return found


def resolve(conn, schema: str, defs: dict, prepared: dict, scopes: dict, mode: str) -> list:
    """Sets unmatched nullable foreign key values to NULL in the prepared frames (in place).
    Returns issue rows (WARNING per foreign key that was changed)."""
    issues = []
    for table, df in prepared.items():
        t = defs[table]
        for fk in t.foreign_keys:
            if len(fk["columns"]) != 1 or fk["columns"][0] not in df.columns:
                continue
            col = fk["columns"][0]
            vals = sorted({_txt(v) for v in df[col] if not _null(v)})
            if not vals:
                continue
            have = pushed_values(prepared, schema, fk)
            rest = [v for v in vals if v not in have]
            if rest:
                exclude = scopes.get(fk["ref_table"]) if (mode == "replace" and fk["ref_schema"] == schema
                                                          and fk["ref_table"] in prepared) else None
                have |= db_values(conn, fk, rest, exclude)
            missing = {v for v in vals if v not in have}
            column = t.column(col) or {}
            if not missing or not config.NULL_UNMATCHED_FK or not column.get("nullable"):
                continue
            mask = df[col].map(lambda v: not _null(v) and _txt(v) in missing)
            n = int(mask.sum())
            df[col] = df[col].astype(object)
            df.loc[mask, col] = None
            issues.append({"table": table, "severity": "WARNING", "column": col,
                           "issue": f"foreign key {fk['name']}: {len(missing):,} value(s) in neither "
                                    f"{fk['ref_table']}.{fk['ref_columns'][0]} nor this push → set to NULL "
                                    f"(config.NULL_UNMATCHED_FK)",
                           "rows": n, "examples": "; ".join(sorted(missing)[:MAX_EXAMPLES])})
            print(f"  {table:28s} {col}: {n:,} row(s) → NULL (foreign key {fk['name']}: referenced "
                  f"{fk['ref_table']}.{fk['ref_columns'][0]} not found)")
    return issues
