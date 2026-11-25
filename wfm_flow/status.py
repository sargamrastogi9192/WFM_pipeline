"""
status.py - what the WFM flow has done: one line per survey of the processed log (times in IST).

    python -m wfm_flow.status                   every survey in the processed log, newest first
    python -m wfm_flow.status --status stopped  only those with this status (ok / stopped / failed / running)
    python -m wfm_flow.status <survey id>       one survey, every field
The processed log is read from WFM_STATE_S3 (else the local copy in <WFM_WORK_DIR>/state/).
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from .settings import WfmSettings
from .state import State

IST = timezone(timedelta(hours=5, minutes=30))


def ist(stamp):
    if not stamp:
        return "-"
    try:
        return datetime.fromisoformat(str(stamp)).astimezone(IST).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return str(stamp)


def main(args=None):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("survey", nargs="?")
    ap.add_argument("--status")
    o = ap.parse_args(args)
    cfg = WfmSettings()
    state = State(cfg.work_dir / "state", cfg.get("WFM_STATE_S3"))
    if o.survey:
        print(json.dumps(state.get(o.survey) or {"error": "not in the processed log"}, indent=2, ensure_ascii=False))
        return 0
    rows = sorted(state.data.items(), key=lambda kv: str(kv[1].get("started_at") or ""), reverse=True)
    rows = [(k, v) for k, v in rows if not o.status or v.get("status") == o.status]
    print(f"{'survey id':38} {'status':8} {'UPC':18} {'z/p/c':7} {'started IST':16} {'finished IST':16} {'min':>5}  "
          f"stopped at / note")
    for sid, v in rows:
        zpc = f"{v.get('zone') or '-'}/{v.get('phase') or '-'}/{v.get('cycle') or '-'}"
        print(f"{sid:38} {str(v.get('status')):8} {str(v.get('upc')):18} {zpc:7} {ist(v.get('started_at')):16} "
              f"{ist(v.get('finished_at')):16} {str(v.get('minutes') or '-'):>5}  "
              f"{v.get('stopped_at') or v.get('note') or ''}")
    counts = {}
    for _, v in rows:
        counts[v.get("status")] = counts.get(v.get("status"), 0) + 1
    print(f"\n{len(rows)} survey(s): {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
