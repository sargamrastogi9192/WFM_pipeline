"""
metadata.py - the WFM surveys in Postgres: which are ready, and their zone / phase / cycle / UPC.

    python -m wfm_flow.metadata <survey id>        the schedule row, its reports, the pipeline values, ready or not

The S3 folder of an upload is the survey id:
    <WFM_REPORTS_S3_ROOT>/<survey id>/D1..D4     <survey id> = wfm_survey_schedule.id (+ its wfm_report rows)

Values the pipeline needs (survey_details):
    upc    wfm_survey_schedule.upc                     -> N/04018/05002/AP
    zone   nsv_zone_id -> master_zone.zone_code        (no nsv_zone_id: upc_project_zone.project_id -> nsv_zone_id)
    phase  wfm_survey_schedule.survey_phase            -> '1'
    cycle  wfm_survey_schedule.survey_cycle            -> '2'
Ready (eligibility): current_status is one of WFM_FINAL_STATUSES (default 'Survey Final Approved'), active, not
deleted. The extra report / lock checks below are off (the status alone decides), as in the WLM-module trigger.
Connection: the repository .env (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD / DB_SECRET_NAME), database WFM_DB_NAME.
"""
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from .settings import WfmSettings

SCHEDULE_TABLE, REPORT_TABLE = "wfm_survey_schedule", "wfm_report"

# names behind the schedule's ids
LOOKUPS = {
    "zone": ("master_zone", "id", ("zone_code", "zone_name")),
    "ro": ("master_region_zone", "id", ("region_name",)),
    "state": ("master_state", "id", ("state_name",)),
    "piu": ("master_piu", "id", ("piu_name",)),
    "project_zone": ("upc_project_zone", "project_id", ("nsv_zone_id",)),
}

# extra checks, all OFF (the status alone decides); switch on if needed
CHECK_REPORTS = False                                   # True = D1..D4 wfm_report rows must also be approved
REQUIRED_REPORTS = ["D1", "D2", "D3", "D4"]
APPROVED_STATUSES = {"APPROVED", "REPORT_APPROVED"}     # wfm_report.current_status meaning approved ...
APPROVED_IF_APPROVED_AT = True                          # ... or approved_at is filled
CHECK_SCHEDULE_LOCK = False                             # True = wfm_survey_schedule.is_locked must be false
CHECK_REPORT_LOCK = False                               # True = wfm_report.is_locked must be false

UPC = re.compile(r"\b([A-Z])[/_\- ]?(\d{5})[/_\- ]?(\d{5})[/_\- ]?([A-Z]{2})\b", re.IGNORECASE)


# =============================================================================
# Connection + queries
# =============================================================================
def connect(cfg: WfmSettings):
    import psycopg2
    return psycopg2.connect(host=cfg.get("DB_HOST"), port=int(cfg.get("DB_PORT", "5432") or 5432),
                            user=cfg.get("DB_USER"), password=cfg.db_password(), dbname=cfg.wfm_db,
                            connect_timeout=15)


def _jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (Decimal, UUID)):
        return str(v)
    if isinstance(v, (bytes, memoryview)):
        return f"<{len(bytes(v))} bytes>"
    return v


def _rows(cur):
    cols = [d[0] for d in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, r)} for r in cur.fetchall()]


def find_table(cur, table, schema):
    """(schema, [column names]) of the table: the configured schema first, then any schema; (None, []) if absent."""
    cur.execute("""SELECT table_schema, column_name FROM information_schema.columns WHERE table_name = %s
                   ORDER BY (table_schema = %s) DESC, table_schema, ordinal_position""", (table, schema))
    found = cur.fetchall()
    if not found:
        return None, []
    first = found[0][0]
    return first, [c for s, c in found if s == first]


def load_lookups(cur, schema):
    """{name: {key: {column: value}}} of the small master tables in LOOKUPS (a missing table -> {})."""
    from psycopg2 import sql
    out = {}
    for name, (table, key, cols) in LOOKUPS.items():
        try:
            cur.execute(sql.SQL("SELECT {}, {} FROM {}.{}").format(
                sql.Identifier(key), sql.SQL(", ").join(map(sql.Identifier, cols)),
                sql.Identifier(schema), sql.Identifier(table)))
            out[name] = {str(r[0]): dict(zip(cols, r[1:])) for r in cur.fetchall()}
        except Exception:
            cur.connection.rollback()
            out[name] = {}
    return out


def enrich(schedule, lookups):
    """Adds _zone_code / _zone_name / _zone_source / _ro_name / _state_name / _piu_name to a schedule row."""
    zid = schedule.get("nsv_zone_id")
    if not zid and schedule.get("project_id"):
        zid = (lookups.get("project_zone", {}).get(str(schedule["project_id"])) or {}).get("nsv_zone_id")
    zone = lookups.get("zone", {}).get(str(zid)) if zid else None
    schedule["_zone_code"] = zone.get("zone_code") if zone else None
    schedule["_zone_name"] = zone.get("zone_name") if zone else None
    schedule["_zone_source"] = ("nsv_zone_id" if schedule.get("nsv_zone_id") else "upc_project_zone") if zone else None
    for name, col, idcol in (("ro", "region_name", "ro_id"), ("state", "state_name", "state_id"),
                             ("piu", "piu_name", "piu_id")):
        row = lookups.get(name, {}).get(str(schedule.get(idcol))) if schedule.get(idcol) else None
        schedule[f"_{name}_name"] = row.get(col) if row else None
    return schedule


def list_surveys(cfg: WfmSettings, survey_id=None):
    """Every survey (or one) with its wfm_report rows: [{'schedule': row, 'reports': [rows]}]."""
    from psycopg2 import sql
    with connect(cfg) as conn, conn.cursor() as cur:
        s_schema, s_cols = find_table(cur, SCHEDULE_TABLE, cfg.wfm_schema)
        r_schema, r_cols = find_table(cur, REPORT_TABLE, cfg.wfm_schema)
        if not s_schema or not r_schema:
            raise RuntimeError(f"{SCHEDULE_TABLE} / {REPORT_TABLE} not found in database {cfg.wfm_db} "
                               f"(set WFM_DB_NAME in wfm_flow/.env)")
        where = [sql.SQL("TRUE")]
        source = cfg.get("WFM_SOURCE_SYSTEM")
        if "source_system" in s_cols and source and not survey_id:
            where.append(sql.SQL("source_system = {}").format(sql.Literal(source)))
        if "deleted_at" in s_cols and not survey_id:
            where.append(sql.SQL("deleted_at IS NULL"))
        if survey_id:
            where.append(sql.SQL("id::text = {}").format(sql.Literal(survey_id)))
        cur.execute(sql.SQL("SELECT * FROM {}.{} WHERE ").format(sql.Identifier(s_schema), sql.Identifier(SCHEDULE_TABLE))
                    + sql.SQL(" AND ").join(where))
        schedules = _rows(cur)
        lookups = load_lookups(cur, s_schema)
        for row in schedules:
            enrich(row, lookups)
        link = "survey_schedule_id" if "survey_schedule_id" in r_cols else None
        reports = {}
        if link:
            q = sql.SQL("SELECT * FROM {}.{} WHERE {} IS NOT NULL").format(
                sql.Identifier(r_schema), sql.Identifier(REPORT_TABLE), sql.Identifier(link))
            if survey_id:
                q += sql.SQL(" AND {}::text = {}").format(sql.Identifier(link), sql.Literal(survey_id))
            cur.execute(q)
            for r in _rows(cur):
                reports.setdefault(str(r[link]), []).append(r)
    return [{"schedule": s, "reports": reports.get(str(s["id"]), [])} for s in schedules]


# =============================================================================
# Ready to process?
# =============================================================================
def _norm(text):
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()


def _true(v):
    return v is True or str(v).strip().lower() in ("true", "t", "1", "yes")


def report_approved(row):
    status = str(row.get("current_status") or "").strip().upper()
    return status in APPROVED_STATUSES or (APPROVED_IF_APPROVED_AT and bool(row.get("approved_at")))


def eligibility(schedule, reports, final_statuses):
    """(ok, [reasons it is not]) for one survey."""
    why = []
    if not schedule:
        return False, ["no wfm_survey_schedule row"]
    if _norm(schedule.get("current_status")) not in {_norm(s) for s in final_statuses}:
        why.append(f"status '{schedule.get('current_status')}' (needs {final_statuses})")
    if CHECK_SCHEDULE_LOCK and _true(schedule.get("is_locked")):
        why.append("survey is locked")
    if "is_active" in schedule and schedule.get("is_active") is not None and not _true(schedule.get("is_active")):
        why.append("survey is not active")
    if schedule.get("deleted_at"):
        why.append("survey is deleted")
    by_type = {}
    for r in reports:                                    # newest row per report type
        t = str(r.get("report_type") or "").upper()
        if t not in by_type or str(r.get("updated_at") or "") > str(by_type[t].get("updated_at") or ""):
            by_type[t] = r
    for t in (REQUIRED_REPORTS if CHECK_REPORTS else []):
        r = by_type.get(t)
        if r is None:
            why.append(f"{t}: no report")
        elif not report_approved(r):
            why.append(f"{t}: not approved ({r.get('current_status')})")
        elif CHECK_REPORT_LOCK and _true(r.get("is_locked")):
            why.append(f"{t}: report is locked")
    return not why, why


def fingerprint(schedule, reports):
    """Changes when the survey or one of its reports changes (a new upload / version)."""
    parts = [str(schedule.get("updated_at"))] + sorted(
        f"{r.get('report_type')}:{r.get('latest_version_id')}:{r.get('updated_at')}:{r.get('current_status')}"
        for r in reports)
    return "|".join(parts)


def _clean(field, value):
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    if field == "upc":
        m = UPC.search(text)
        return f"{m.group(1).upper()}/{m.group(2)}/{m.group(3)}/{m.group(4).upper()}" if m else None
    if field == "zone":
        m = re.search(r"\b(?:zone[\s_\-]*)?([A-Z])\b", text, re.I)      # 'Zone A', 'A', 'zone-a'
        return m.group(1).upper() if m else None
    m = re.search(r"(\d+)", text)                     # 'Phase 2', 'Cycle-2', 2 -> '2'
    return str(int(m.group(1))) if m else None


def survey_details(schedule):
    """The values the pipeline needs, from the (enriched) schedule row. A value not found is None."""
    return {"survey_id": str(schedule.get("id")), "upc": _clean("upc", schedule.get("upc")),
            "zone": _clean("zone", schedule.get("_zone_code")), "zone_name": schedule.get("_zone_name"),
            "zone_from": schedule.get("_zone_source"),
            "phase": _clean("phase", schedule.get("survey_phase")), "cycle": _clean("cycle", schedule.get("survey_cycle")),
            "project_name": schedule.get("project_name"), "schedule_name": schedule.get("schedule_name"),
            "ro": schedule.get("_ro_name"), "state": schedule.get("_state_name"), "piu": schedule.get("_piu_name"),
            "current_status": schedule.get("current_status")}


# =============================================================================
def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if not args:
        print(__doc__)
        return 1
    cfg = WfmSettings()
    sid = args[0].strip().strip("/").split("/")[-1]
    found = list_surveys(cfg, sid)
    if not found:
        print(f"{sid}: no {SCHEDULE_TABLE} row (database {cfg.wfm_db})")
        return 2
    item = found[0]
    print(f"{SCHEDULE_TABLE} (database {cfg.wfm_db}):")
    print(json.dumps(item["schedule"], indent=2, ensure_ascii=False, default=str))
    print(f"\n{REPORT_TABLE}: {len(item['reports'])} row(s)")
    for r in sorted(item["reports"], key=lambda r: str(r.get("report_type"))):
        print(f"  {r.get('report_type')}  {r.get('current_status')}  locked={r.get('is_locked')}  "
              f"approved_at={r.get('approved_at')}")
    ok, why = eligibility(item["schedule"], item["reports"], cfg.final_statuses)
    print(f"\npipeline values: {survey_details(item['schedule'])}")
    print("ready to process: " + ("YES" if ok else "NO - " + "; ".join(why)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
