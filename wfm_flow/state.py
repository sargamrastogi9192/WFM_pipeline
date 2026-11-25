"""
state.py - the processed log of the WFM flow: one entry per survey id.

    <WFM_WORK_DIR>/state/wfm_trigger_state.json     {survey id: {status, upc, zone, phase, cycle, started_at, ...}}
    <WFM_WORK_DIR>/state/processed_surveys.csv      the same as a table
    both mirrored to <WFM_STATE_S3>/ after every change (the S3 copy is read first, so a new machine continues
    where the old one stopped)

Status of a survey:
    running   the trigger is processing it now (left behind only when the machine stopped mid-run)
    ok        run.py finished every step                                     -> never processed again
    stopped   run.py stopped at a step (warning / error / failed DB check)   -> again only when the upload changes
    failed    the copy (migrate) or the run could not finish                 -> retried, up to WFM_MAX_ATTEMPTS
"""
import csv
import io
import json
import os
from pathlib import Path

CSV_COLUMNS = ["survey_id", "status", "upc", "zone", "phase", "cycle", "project_name", "ro", "state", "piu",
               "started_at", "finished_at", "minutes", "exit_code", "attempts", "stopped_at", "note", "run_log"]


def _s3():
    import boto3
    return boto3.client("s3")


class State:
    def __init__(self, folder: Path, s3_url: str = ""):
        self.path = Path(folder) / "wfm_trigger_state.json"
        self.s3 = f"{s3_url.rstrip('/')}/wfm_trigger_state.json" if s3_url else None
        self.data = {}
        if self.s3:
            try:
                bucket, _, key = self.s3[5:].partition("/")
                self.data = json.loads(_s3().get_object(Bucket=bucket, Key=key)["Body"].read())
            except Exception:                                    # noqa: BLE001 - not there yet / no access
                self.data = {}
        if not self.data and self.path.is_file():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, survey_id):
        return self.data.get(survey_id, {})

    def put(self, survey_id, **values):
        self.data[survey_id] = {**self.data.get(survey_id, {}), **values}
        self.save()

    def csv_text(self):
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for sid, v in sorted(self.data.items(), key=lambda kv: str(kv[1].get("started_at") or "")):
            w.writerow({"survey_id": sid, **v})
        return buf.getvalue()

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)
        (self.path.parent / "processed_surveys.csv").write_text(self.csv_text(), encoding="utf-8")
        if self.s3:
            bucket, _, key = self.s3[5:].partition("/")
            _s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(self.data, indent=2).encode("utf-8"))
            _s3().put_object(Bucket=bucket, Key=key.rsplit("/", 1)[0] + "/processed_surveys.csv",
                             Body=self.csv_text().encode("utf-8"))
