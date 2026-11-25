"""WFM flow rules with local folders instead of S3 and a fake run.py instead of the pipeline (no AWS, no DB).

    python -m pytest -q wfm_flow/tests
"""
import io
import json
import os
import sys
import textwrap
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

SID = "57d89b35-0ceb-4bcc-b35a-fb947c0f8268"
STAGES = ["bronze", "silver", "images", "gold", "push_silver", "push_gold"]
UPC = "N/04018/05002/AP"


def schedule(sid=SID, status="Survey Final Approved", updated="2026-11-20T10:00:00"):
    return {"schedule": {"id": sid, "upc": UPC, "_zone_code": "D", "survey_phase": 1, "survey_cycle": "Cycle 2",
                         "current_status": status, "updated_at": updated, "project_name": "test"},
            "reports": [{"report_type": "D1", "current_status": "REPORT_APPROVED", "updated_at": updated}]}


def make_upload(base: Path, sid=SID):
    """A WFM upload: D1..D4/<timestamp>-<name>, an older + newer upload of the same file, a zip, a video."""
    up = base / sid
    (up / "D1").mkdir(parents=True)
    (up / "D1" / "1793260589326-LRMS_DataTemplate.xlsx").write_bytes(b"new")
    (up / "D1" / "1693260589326-LRMS_DataTemplate.xlsx").write_bytes(b"old")       # superseded
    (up / "D2").mkdir()
    (up / "D2" / "1793260589400-D2 Comparison File.xlsm").write_bytes(b"d2")
    (up / "D3" / "GIS Mapping -As Per CA" / "GIS Shape Files" / "Cracking").mkdir(parents=True)
    (up / "D3" / "GIS Mapping -As Per CA" / "GIS Shape Files" / "Cracking" / "1793260591234-Cracking.shp").write_bytes(b"shp")
    (up / "D4").mkdir()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("Crack Images_DHMS/L1/a.jpg", b"jpg")
        z.writestr("__MACOSX/._a.jpg", b"x")
    (up / "D4" / "1794207928512-Crack_Images_DHMS.zip").write_bytes(buf.getvalue())
    (up / "D4" / "1794207928600-drive.mp4").write_bytes(b"video")
    return up


FAKE_RUN = textwrap.dedent("""
    import json, os, sys
    from datetime import datetime
    from pathlib import Path
    log_dir = Path(os.environ["LOG_DIR"])
    run = log_dir / f"run_{datetime.now():%Y%m%d_%H%M%S_%f}"
    run.mkdir(parents=True)
    stage = sys.argv[sys.argv.index("--only") + 1]
    codes = dict(x.split("=") for x in os.environ.get("FAKE_RUN_CODES", "").split(",") if x)
    code = int(codes.get(stage, os.environ.get("FAKE_RUN_CODE", "0")))
    text = f"!! STOPPED: {stage} N/04018/05002/AP: warning (exit 2)\\n" if code == 1 else "Every step OK\\n"
    (run / "pipeline.log").write_text(text)
    rec = Path(os.environ["FAKE_RUN_RECORD"])
    calls = json.loads(rec.read_text()) if rec.exists() else []
    calls.append({"argv": sys.argv[1:], "raw": os.environ.get("RAW_S3_ROOT")})
    rec.write_text(json.dumps(calls))
    sys.exit(code)
""")


@pytest.fixture
def env(tmp_path, monkeypatch):
    (tmp_path / "repo.env").write_text("DB_NAME=test_db\nDB_HOST=h\nDB_USER=u\nDB_PASSWORD=p\n")
    uploads, stage = tmp_path / "uploads", tmp_path / "stage"
    monkeypatch.setenv("PIPELINE_ENV", str(tmp_path / "repo.env"))
    monkeypatch.setenv("WFM_ENV", str(tmp_path / "none.env"))
    monkeypatch.setenv("WFM_REPORTS_S3_ROOT", str(uploads))
    monkeypatch.setenv("WFM_STAGE_S3", str(stage))
    monkeypatch.setenv("WFM_STATE_S3", "")
    monkeypatch.setenv("WFM_WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("FAKE_RUN_RECORD", str(tmp_path / "calls.json"))
    for m in [m for m in sys.modules if m.startswith(("wfm_flow", "pipeline"))]:
        del sys.modules[m]
    from wfm_flow import metadata, trigger
    fake = tmp_path / "fake_run.py"
    fake.write_text(FAKE_RUN)
    monkeypatch.setattr(trigger, "RUN_PY", fake)
    items = [schedule()]
    monkeypatch.setattr(metadata, "list_surveys", lambda cfg, sid=None: [i for i in items if not sid or i["schedule"]["id"] == sid])
    make_upload(uploads)
    return {"tmp": tmp_path, "trigger": trigger, "items": items, "stage": stage,
            "calls": lambda: json.loads((tmp_path / "calls.json").read_text()) if (tmp_path / "calls.json").exists() else [],
            "state": lambda: json.loads((tmp_path / "work" / "state" / "wfm_trigger_state.json").read_text())}


def test_migrate_layout(env):
    from wfm_flow.migrate import migrate
    stale = env["stage"] / "Zone D/Phase 1/Cycle 2/N_04018_05002_AP/Deliverable 1 - RAMS Data/old_file.xlsx"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"from an earlier upload")
    m = migrate(str(env["tmp"] / "uploads" / SID), SID, "D", "1", "2", UPC, str(env["stage"]), log=lambda *_: None)
    upc = env["stage"] / "Zone D/Phase 1/Cycle 2/N_04018_05002_AP"
    files = sorted(p.relative_to(upc).as_posix() for p in upc.rglob("*") if p.is_file())
    assert files == ["Deliverable 1 - RAMS Data/LRMS_DataTemplate.xlsx",
                     "Deliverable 2 - Comparison/D2 Comparison File.xlsm",
                     "Deliverable 3 - GIS Mapping/GIS Mapping -As Per CA/GIS Shape Files/Cracking/Cracking.shp",
                     "Deliverable 4 - NHAI One & Inspection Sathi Format/Crack_Images_DHMS/L1/a.jpg"]
    assert (upc / "Deliverable 1 - RAMS Data/LRMS_DataTemplate.xlsx").read_bytes() == b"new"     # newest upload
    assert m["earlier_files_removed"] == 1 and m["videos_skipped"] == 1 and m["unreadable_count"] == 0
    assert m["deliverables"] == ["d1", "d2", "d3", "d4"]
    assert (env["stage"] / "_manifests" / f"{SID}.json").is_file()


def test_bronze_finds_the_stage_layout(env):
    """bronze/run_download.py (unchanged) maps the stage folders to d1..d4 of the right survey."""
    from wfm_flow.migrate import migrate
    migrate(str(env["tmp"] / "uploads" / SID), SID, "D", "1", "2", UPC, str(env["stage"]), log=lambda *_: None)
    sys.path.insert(0, str(ROOT / "bronze"))
    import run_download
    folders, rows, missing = run_download.find_deliverables(env["stage"], [UPC])
    assert not missing
    assert sorted(folders) == [f"nsv/zone=D/phase=1/cycle=2/N-04018-05002-AP/d{i}" for i in (1, 2, 3, 4)]


def test_trigger_runs_new_survey_once(env):
    t = env["trigger"]
    assert t.main([]) == 0
    calls = env["calls"]()
    assert [c["argv"][2] for c in calls] == STAGES                         # one run.py per stage, the DB last
    assert calls[0]["argv"] == ["--scheduled", "--only", "bronze", "--zone", "D", "--phase", "1", "--cycle", "2",
                                "--upc", UPC]
    assert calls[0]["raw"].rstrip("/") == str(env["stage"]).replace("\\", "/").rstrip("/") or \
        calls[0]["raw"].rstrip("/") == str(env["stage"]).rstrip("/")
    s = env["state"]()[SID]
    assert s["status"] == "ok" and s["upc"] == UPC and s["run_log"]
    assert t.main([]) == 0                       # processed: not again
    assert len(env["calls"]()) == 6
    assert t.main(["--survey", SID, "--force"]) == 0
    assert len(env["calls"]()) == 12


def test_not_ready_is_skipped(env):
    env["items"][0] = schedule(status="Survey Submitted")
    assert env["trigger"].main([]) == 0
    assert env["calls"]() == []


def test_stopped_runs_again_only_after_a_new_upload(env, monkeypatch):
    t = env["trigger"]
    monkeypatch.setenv("WFM_CONTINUE_ON_WARNING", "")                  # strict: every warning stops
    monkeypatch.setenv("FAKE_RUN_CODE", "1")
    assert t.main([]) == 1
    s = env["state"]()[SID]
    assert s["status"] == "stopped" and s["stopped_at"].startswith("bronze")
    assert t.main([]) == 0                       # same upload: left alone
    assert len(env["calls"]()) == 1
    env["items"][0] = schedule(updated="2026-11-22T09:00:00")      # vendor uploaded again
    monkeypatch.setenv("FAKE_RUN_CODE", "0")
    assert t.main([]) == 0
    assert len(env["calls"]()) == 7 and env["state"]()[SID]["status"] == "ok"


def test_failed_is_retried_up_to_max_attempts(env, monkeypatch):
    t = env["trigger"]
    monkeypatch.setenv("FAKE_RUN_CODE", "3")     # crash, no STOPPED line
    for _ in range(4):
        t.main([])
    assert len(env["calls"]()) == 3              # WFM_MAX_ATTEMPTS = 3
    assert env["state"]()[SID]["status"] == "failed"


def test_data_warnings_are_passed_and_recorded(env, monkeypatch):
    """bronze / silver warnings (missing CRRI workbook, DQ vs D2) do not stop the survey; the DB stages still run."""
    monkeypatch.setenv("FAKE_RUN_CODES", "bronze=1,silver=1")
    assert env["trigger"].main([]) == 0
    assert [c["argv"][2] for c in env["calls"]()] == STAGES
    s = env["state"]()[SID]
    assert s["status"] == "warnings" and s["note"].count("warning passed") == 2
    assert env["trigger"].main([]) == 0 and len(env["calls"]()) == 6       # processed: not again


def test_warning_outside_the_list_stops(env, monkeypatch):
    monkeypatch.setenv("FAKE_RUN_CODES", "gold=1")
    assert env["trigger"].main([]) == 1
    assert [c["argv"][2] for c in env["calls"]()] == ["bronze", "silver", "images", "gold"]   # no DB push
    s = env["state"]()[SID]
    assert s["status"] == "stopped" and s["stopped_at"].startswith("gold")


def test_dry_run_runs_nothing(env):
    assert env["trigger"].main(["--dry-run"]) == 0
    assert env["calls"]() == [] and not (env["tmp"] / "work" / "state" / "wfm_trigger_state.json").exists()


def test_details_from_schedule():
    from wfm_flow.metadata import eligibility, survey_details
    d = survey_details(schedule()["schedule"])
    assert (d["upc"], d["zone"], d["phase"], d["cycle"]) == (UPC, "D", "1", "2")
    ok, why = eligibility({"current_status": "survey  final approved"}, [], ["Survey Final Approved"])
    assert ok and not why
    ok, why = eligibility({"current_status": "Draft", "deleted_at": "x"}, [], ["Survey Final Approved"])
    assert not ok and len(why) == 2
