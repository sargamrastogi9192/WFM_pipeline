"""
trigger.py - process every WFM survey that is 'Survey Final Approved' and not processed yet (run by cron).

    python -m wfm_flow.trigger                       every new final-approved survey
    python -m wfm_flow.trigger --dry-run             only show what would be processed (and why the others are not)
    python -m wfm_flow.trigger --survey <id>         only this survey (still checked against the rules)
    python -m wfm_flow.trigger --survey <id> --force process it again / although not final-approved
    python -m wfm_flow.trigger --reprocess           every final-approved survey again (e.g. after a logic change)
    passed on to run.py:  --no-write  --from <stage>  --to <stage>  --db <database>

For each survey:
  1. Postgres: wfm_survey_schedule + its wfm_report rows; ready = current_status in WFM_FINAL_STATUSES
     (metadata.py); zone (master_zone.zone_code), phase, cycle, UPC from the schedule row
  2. processed log (state.py): 'ok' is never processed again; 'stopped' only when the upload changed;
     'failed' is retried up to WFM_MAX_ATTEMPTS times
  3. S3: the upload must be there - <WFM_REPORTS_S3_ROOT>/<survey id>/ with D1..D4
  4. MIGRATE (migrate.py): unzipped, no videos -> <WFM_STAGE_S3>/Zone D/Phase 1/Cycle 2/<UPC>/Deliverable 1..4/
  5. run.py --scheduled --only <stage> --zone --phase --cycle --upc, one stage after another, RAW_S3_ROOT =
     <WFM_STAGE_S3>:  bronze -> silver -> images -> gold -> push_silver -> push_gold  (the database last)
     - a stage OK -> the next one
     - a WARNING of a stage in WFM_CONTINUE_ON_WARNING (default bronze, silver, images: data quality, e.g. a
       missing CRRI workbook or a silver DQ check vs D2) -> recorded, the next stage runs
     - any other WARNING, a FAILED step or a database check with blockers -> the survey stops there
     - the database stages write only after their own clean check (run.py)
  6. processed log updated (status, the warnings, the step it stopped at, the run log folders)
One survey after another, each in its own run.py (gold only changes the rows of the run's surveys).
Exit code: 0 nothing failed or stopped, 1 a survey failed / stopped or the trigger could not run.
"""
import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import metadata
from .migrate import migrate
from .report_storage import DELIVERABLE_FOLDER
from .settings import ROOT, WfmSettings
from .state import State

RUN_PY = Path(os.getenv("WFM_RUN_PY") or ROOT / "run.py")       # tests point this to a fake run.py
ZONES, PHASES, CYCLES = "ABCDE", ("1", "2", "3"), ("1", "2", "3")   # values bronze accepts (bronze/settings.py)
DONE = ("ok", "warnings")                    # processed: not picked again (--reprocess / --survey <id> --force)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(text=""):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {text}" if text else "", flush=True)


def _s3():
    import boto3
    return boto3.client("s3")


def s3_upload(cfg, survey_id):
    """{'location', 'deliverables', 'files', 'newest'} of the upload (None when nothing is there)."""
    location = f"{cfg.get('WFM_REPORTS_S3_ROOT').rstrip('/')}/{survey_id}/"
    if not location.startswith("s3://"):                         # a local folder of uploads (tests)
        base = Path(location)
        files = [p for p in base.rglob("*") if p.is_file()] if base.is_dir() else []
        found = {f"d{DELIVERABLE_FOLDER.match(p.relative_to(base).parts[0]).group(1)}" for p in files
                 if DELIVERABLE_FOLDER.match(p.relative_to(base).parts[0])}
        newest = max((p.stat().st_mtime for p in files), default=None)
        return {"location": location, "deliverables": sorted(found), "files": len(files), "newest": newest} if files else None
    bucket, _, prefix = location[5:].partition("/")
    files, newest, found = 0, None, set()
    for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            files += 1
            stamp = obj["LastModified"].isoformat()
            newest = max(newest or stamp, stamp)
            m = DELIVERABLE_FOLDER.match(obj["Key"][len(prefix):].split("/")[0])
            if m:
                found.add(f"d{m.group(1)}")
    return {"location": location, "deliverables": sorted(found), "files": files, "newest": newest} if files else None


def run_log_result(log_dir: Path, before: set):
    """(run log folder, the reason run.py stopped or None) of the run.py run that just finished."""
    new = sorted(p for p in log_dir.glob("run_*") if p.is_dir() and p.name not in before)
    if not new:
        return None, None
    folder, reason = new[-1], None
    f = folder / "pipeline.log"
    if f.is_file():
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            if "STOPPED:" in line:
                reason = line.split("STOPPED:", 1)[1].strip()
    if reason is None and (folder / "summary.csv").is_file():
        with open(folder / "summary.csv", encoding="utf-8") as fh:
            bad = [r for r in csv.DictReader(fh) if r.get("result") in ("FAILED", "WARNING")]
        reason = f"{bad[-1]['step']}: {bad[-1]['result']}" if bad else None
    return folder, reason


def stages_to_run(cfg, o):
    """The stages of the run, in pipeline order (pipeline/settings.py ALL_STAGES, STAGES of .env, --from / --to)."""
    from pipeline.settings import ALL_STAGES
    start, end = o.start or ALL_STAGES[0], o.end or ALL_STAGES[-1]
    return [x for x in ALL_STAGES[ALL_STAGES.index(start):ALL_STAGES.index(end) + 1] if x in cfg.s.stages]


def run_survey(cfg, d, o):
    """The stages for one survey, each its own 'run.py --scheduled --only <stage>'.
    Returns (exit code, [log folders], reason it stopped or None, [warnings that were passed])."""
    go_on = {x.strip() for x in cfg.get("WFM_CONTINUE_ON_WARNING").split(",") if x.strip()}
    log_dir = cfg.s.path("LOG_DIR", "logs")
    env = {**os.environ, "RAW_S3_ROOT": cfg.stage_root(), "PYTHONIOENCODING": "utf-8"}
    folders, warnings = [], []
    for stage in stages_to_run(cfg, o):
        cmd = [sys.executable, str(RUN_PY), "--scheduled", "--only", stage, "--zone", d["zone"], "--phase", d["phase"],
               "--cycle", d["cycle"], "--upc", d["upc"]]
        cmd += (["--no-write"] if o.no_write else []) + (["--db", o.db] if o.db else [])
        before = {p.name for p in log_dir.glob("run_*")} if log_dir.is_dir() else set()
        log(f"  RUN  RAW_S3_ROOT={cfg.stage_root()} {' '.join(cmd[1:])}")
        code = subprocess.call(cmd, cwd=str(ROOT), env=env)
        folder, why = run_log_result(log_dir, before)
        folders += [str(folder)] if folder else []
        if code == 0:
            continue
        warning = code == 1 and why and "warning (exit" in why
        if warning and stage in go_on:
            warnings.append(why)
            log(f"  WARNING passed ({stage} in WFM_CONTINUE_ON_WARNING): {why}")
            continue
        return code, folders, why or f"{stage}: exit code {code}", warnings
    return 0, folders, None, warnings


def pick(item, state, cfg, o):
    """(action, details or reason): action = run / skip-<why>."""
    sched, reports = item["schedule"], item["reports"]
    sid = str(sched.get("id"))
    ok, why = metadata.eligibility(sched, reports, cfg.final_statuses)
    if not ok and not o.force:
        return "not_ready", "; ".join(why)
    last = state.get(sid)
    fp = metadata.fingerprint(sched, reports)
    if not o.force and not o.reprocess:
        if last.get("status") in DONE:
            return "already_processed", last.get("status")
        if last.get("status") == "stopped" and last.get("fingerprint") == fp:
            return "stopped_before", f"stopped at '{last.get('stopped_at')}' - same upload, not run again " \
                                     f"(fix it, then: --survey {sid} --force)"
        if last.get("status") == "failed" and last.get("fingerprint") == fp and \
                int(last.get("attempts", 0)) >= int(cfg.get("WFM_MAX_ATTEMPTS")):
            return "gave_up", f"failed {last['attempts']} times (rerun: --survey {sid} --force)"
    d = metadata.survey_details(sched)
    missing = [k for k in ("upc", "zone", "phase", "cycle") if not d.get(k)]
    if missing:
        return "no_details", f"{missing} not found in wfm_survey_schedule / master_zone"
    bad = [f"{k}={d[k]}" for k, allowed in (("zone", ZONES), ("phase", PHASES), ("cycle", CYCLES)) if d[k] not in allowed]
    if bad:
        return "no_details", f"{bad} not accepted by bronze (bronze/settings.py ZONES / PHASES / CYCLES)"
    return "run", d


def main(args=None):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1], formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="\n".join(__doc__.split("\n")[2:]))
    ap.add_argument("--survey", help="only this survey id")
    ap.add_argument("--dry-run", action="store_true", help="show what would run, run nothing")
    ap.add_argument("--force", action="store_true", help="with --survey: process again / although not final-approved")
    ap.add_argument("--reprocess", action="store_true", help="every final-approved survey again (processed log ignored)")
    ap.add_argument("--no-write", action="store_true", help="run.py --no-write: database checks run, nothing written")
    ap.add_argument("--from", dest="start", help="run.py --from <stage>")
    ap.add_argument("--to", dest="end", help="run.py --to <stage>")
    ap.add_argument("--db", help="run.py --db <database> (the database of both pushes)")
    o = ap.parse_args(args)
    if o.force and not o.survey:
        ap.error("--force only together with --survey")
    from pipeline.settings import ALL_STAGES
    for flag in ("start", "end"):
        if getattr(o, flag) and getattr(o, flag) not in ALL_STAGES:
            ap.error(f"--{'from' if flag == 'start' else 'to'}: one of {ALL_STAGES}")

    cfg = WfmSettings(o.db)
    work = cfg.work_dir
    log(f"WFM trigger: {'DRY RUN' if o.dry_run else 'processing'} | WFM tables in {cfg.wfm_db} | "
        f"uploads {cfg.get('WFM_REPORTS_S3_ROOT')} | stage {cfg.stage_root()} | push database {cfg.s.db_name}"
        + (" | NO WRITE" if o.no_write else ""))
    try:
        surveys = metadata.list_surveys(cfg, o.survey)
    except Exception as e:                                       # noqa: BLE001
        log(f"cannot read the surveys from Postgres: {type(e).__name__}: {e}")
        return 1
    if o.survey and not surveys:
        log(f"survey {o.survey} not found in wfm_survey_schedule")
        return 1
    state = State(work / "state", cfg.get("WFM_STATE_S3"))
    log(f"{len(surveys)} survey(s) in Postgres")

    counts, bad = {}, 0
    for item in surveys:
        sid = str(item["schedule"].get("id"))
        action, info = pick(item, state, cfg, o)
        if action != "run":
            counts[action] = counts.get(action, 0) + 1
            if action != "not_ready" or o.survey or o.dry_run:
                log(f"  skip {sid}: {action} - {info}")
            continue
        d = info
        try:
            upload = s3_upload(cfg, sid)
        except Exception as e:                                   # noqa: BLE001
            counts["s3_error"] = counts.get("s3_error", 0) + 1
            log(f"  skip {sid}: cannot list the upload ({type(e).__name__}: {e})")
            continue
        if not upload:
            counts["not_in_s3"] = counts.get("not_in_s3", 0) + 1
            log(f"  skip {sid}: final approved, but nothing in {cfg.get('WFM_REPORTS_S3_ROOT')}{sid}/ yet")
            continue
        last = state.get(sid)
        fp = metadata.fingerprint(item["schedule"], item["reports"])
        reason = "forced" if o.force else "again" if last.get("status") in DONE else \
            "retry" if last.get("status") in ("failed", "stopped", "running") else "new"
        log(f"  {reason.upper():7} {sid}  UPC {d['upc']}  zone {d['zone']}  phase {d['phase']}  cycle {d['cycle']}  "
            f"upload {upload['files']} file(s) {upload['deliverables']}")
        if o.dry_run:
            counts["would_run"] = counts.get("would_run", 0) + 1
            continue

        meta = work / "metadata" / f"{sid}.json"                 # the Postgres rows this run used
        meta.parent.mkdir(parents=True, exist_ok=True)
        meta.write_text(json.dumps({**item, "details": d}, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        same = last.get("fingerprint") == fp and last.get("status") == "failed"
        attempts = int(last.get("attempts", 0)) + 1 if same and not (o.force or o.reprocess) else 1
        state.put(sid, status="running", started_at=now(), finished_at=None, fingerprint=fp, attempts=attempts,
                  upc=d["upc"], zone=d["zone"], phase=d["phase"], cycle=d["cycle"], project_name=d.get("project_name"),
                  ro=d.get("ro"), state=d.get("state"), piu=d.get("piu"), note=None, stopped_at=None, exit_code=None)
        started = datetime.now()
        try:
            manifest = migrate(upload["location"], sid, d["zone"], d["phase"], d["cycle"], d["upc"], cfg.stage_root(),
                               log=log)
            (work / "manifests").mkdir(parents=True, exist_ok=True)
            (work / "manifests" / f"{sid}.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        except Exception as e:                                   # noqa: BLE001
            state.put(sid, status="failed", finished_at=now(), note=f"migrate: {type(e).__name__}: {e}")
            counts["failed"] = counts.get("failed", 0) + 1
            bad += 1
            log(f"  FAILED {sid}: migrate: {type(e).__name__}: {e}")
            continue
        code, folders, why, warnings = run_survey(cfg, d, o)
        minutes = round((datetime.now() - started).total_seconds() / 60, 1)
        if code == 2:                                            # settings problem: same for every survey
            state.put(sid, status="failed", exit_code=code, finished_at=now(), minutes=minutes,
                      note="run.py: settings problem (see the output above)")
            log("  run.py reported a settings problem - trigger stopped (fix .env, then run again)")
            return 1
        status = ("warnings" if warnings else "ok") if code == 0 else \
            "stopped" if code == 1 and why and "exit code" not in why else "failed"
        note = "; ".join(([f"{manifest['unreadable_count']} file(s) of the upload could not be copied"]
                          if manifest["unreadable_count"] else []) + [f"warning passed: {w}" for w in warnings]) or None
        state.put(sid, status=status, exit_code=code, finished_at=now(), minutes=minutes, stopped_at=why,
                  run_log=", ".join(folders) or None, note=note)
        counts[status] = counts.get(status, 0) + 1
        bad += status not in DONE
        log(f"  DONE {sid}: {status.upper()} (exit {code})" + (f" - {why}" if why else "")
            + (f" | {len(warnings)} warning(s) passed" if warnings else ""))

    log(f"trigger finished: {counts}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
