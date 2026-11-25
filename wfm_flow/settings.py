"""
settings.py - the settings of the WFM flow.

Read in this order (the first that has the key wins):
    1. an environment variable of the same name
    2. wfm_flow/.env          the WFM keys (copy wfm_flow/wfm.env.example)
    3. <repo>/.env            the pipeline's own settings: DB_HOST, DB_USER, DB_PASSWORD / DB_SECRET_NAME, BUCKET, ...
    4. the defaults below

The database connection (host, user, password) is the pipeline's own (.env of the repository); the WFM tables
(wfm_survey_schedule, wfm_report) are read in WFM_DB_NAME (default: DB_NAME).
"""
import os
from pathlib import Path

from pipeline.settings import ROOT, Settings, load_settings, read_env_file, resolve_db_password

WFM_DIR = Path(__file__).resolve().parent
WFM_ENV = Path(os.getenv("WFM_ENV") or WFM_DIR / ".env")

DEFAULTS = {
    # the WFM uploads: <root>/<survey id>/D1..D4/<upload timestamp>-<file name>
    "WFM_REPORTS_S3_ROOT": "s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/",
    # the migrated copy (unzipped, no videos) in the layout bronze reads (bronze/run_download.py):
    # <root>/Zone D/Phase 1/Cycle 1/N_04018_05002_AP/Deliverable 1 - RAMS Data/...   -> RAW_S3_ROOT of the run
    "WFM_STAGE_S3": "s3://dhms-new-stgae/DHMS/NSV/WFM_Raw/",
    # processed log (wfm_trigger_state.json + processed_surveys.csv), mirrored here; empty = local only
    "WFM_STATE_S3": "s3://dhms-new-stgae/DHMS/NSV/Pipeline/wfm_state",
    # database / schema of wfm_survey_schedule + wfm_report (empty = DB_NAME / DB_SCHEMA of the repository .env)
    "WFM_DB_NAME": "",
    "WFM_DB_SCHEMA": "",
    # a survey is processed when wfm_survey_schedule.current_status is one of these (comma separated)
    "WFM_FINAL_STATUSES": "Survey Final Approved",
    # only schedules of this source_system (empty = all)
    "WFM_SOURCE_SYSTEM": "NSV",
    # a failed survey (copy error, crash) is retried on the next runs up to this many times
    "WFM_MAX_ATTEMPTS": "3",
    # stages whose WARNING is recorded and the survey goes on (comma separated; empty = stop at every warning,
    # as run.py --scheduled). A FAILED step always stops; the database stages write only after a clean check.
    "WFM_CONTINUE_ON_WARNING": "bronze,silver,images",
    # local folder of the processed log, the Postgres rows and the migration manifests (relative to the repo)
    "WFM_WORK_DIR": "logs/wfm",
}


class WfmSettings:
    """The repository Settings (pipeline/settings.py) + the WFM keys."""

    def __init__(self, db_name=None):
        self.s: Settings = load_settings(db_name)
        for key, value in read_env_file(WFM_ENV).items():        # wfm_flow/.env wins over the repository .env
            self.s.values[key] = value

    def get(self, key, default=None):
        """A key that is set but empty stays empty (WFM_STATE_S3= means: local only); a missing key -> default."""
        if key in os.environ:
            return os.environ[key].strip()
        if key in self.s.values:
            return str(self.s.values[key]).strip()
        return DEFAULTS.get(key, "") if default is None else default

    @property
    def wfm_db(self):
        return self.get("WFM_DB_NAME") or self.s.db_name

    @property
    def wfm_schema(self):
        return self.get("WFM_DB_SCHEMA") or self.get("DB_SCHEMA", "public")

    @property
    def final_statuses(self):
        return [x.strip() for x in self.get("WFM_FINAL_STATUSES").split(",") if x.strip()]

    @property
    def work_dir(self) -> Path:
        p = Path(self.get("WFM_WORK_DIR")).expanduser()
        return p if p.is_absolute() else ROOT / p

    def stage_root(self):
        return self.get("WFM_STAGE_S3").rstrip("/") + "/"

    def db_password(self):
        """The password of the repository .env (DB_PASSWORD, else the Secrets Manager secret DB_SECRET_NAME)."""
        problem = resolve_db_password(self.s)
        if problem:
            raise RuntimeError(f"database password: {problem}")
        return self.s.get("DB_PASSWORD")


__all__ = ["ROOT", "WFM_DIR", "WFM_ENV", "DEFAULTS", "WfmSettings"]
