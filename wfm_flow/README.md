# WFM flow

Runs the NSV pipeline of this repository (`run.py`) automatically for every WFM survey that is **Survey Final
Approved**. The surveys and their upload come from WFM; everything after the upload is the repository's own
pipeline, unchanged.

```
Postgres (WFM)                 S3 (WFM upload)                    S3 (stage)                         this repository
wfm_survey_schedule  ──ready?──► reports/<survey id>/D1..D4 ──MIGRATE──► WFM_Raw/Zone D/Phase 1/Cycle 2/ ──► run.py, stage by stage
+ wfm_report                     <timestamp>-<file>, zips              <UPC>/Deliverable 1..4/           bronze → silver → images → gold
zone / phase / cycle / UPC                                             (unzipped, no videos)             → push_silver → push_gold (DB last)
```

| File | What it does |
|---|---|
| `trigger.py` | the job: finds the ready surveys, migrates each upload, runs `run.py` for it, keeps the processed log |
| `metadata.py` | Postgres: `wfm_survey_schedule` + `wfm_report`, ready or not, zone (`master_zone`) / phase / cycle / UPC |
| `migrate.py` | WFM upload → the vendor layout bronze reads (`bronze/run_download.py`): unzipped, timestamps removed, no videos |
| `report_storage.py` | reads a WFM upload (D1..D4, timestamps, re-uploads, zips, damaged zip entries) |
| `state.py` | processed log: `logs/wfm/state/wfm_trigger_state.json` + `processed_surveys.csv`, mirrored to S3 |
| `status.py` | what the flow has done, one line per survey (IST) |
| `settings.py`, `wfm.env.example` | the WFM settings (the database login is the repository's `.env`) |
| `install_cron.sh` | the cron job (e.g. daily at 00:30 IST) |
| `tests/` | the rules, with local folders and a fake `run.py` (no AWS, no DB) |

## What happens to one survey

1. **Ready?** `wfm_survey_schedule.current_status` is one of `WFM_FINAL_STATUSES` (default `Survey Final Approved`),
   the survey is active and not deleted. Zone = `master_zone.zone_code` of `nsv_zone_id` (else of the project's
   `upc_project_zone`), phase = `survey_phase`, cycle = `survey_cycle`, UPC = `upc`.
2. **Processed log** (one entry per survey id):

   | status | meaning | next trigger run |
   |---|---|---|
   | `ok` | every stage OK | never again (`--reprocess` / `--survey <id> --force` to redo) |
   | `warnings` | every stage done; data warnings were passed (`note` lists them) | never again (as `ok`) |
   | `stopped` | stopped at a step: a warning outside `WFM_CONTINUE_ON_WARNING`, or a DB check with blockers (`stopped_at` names it) | again only when the survey changes in WFM (new upload / version); else `--survey <id> --force` |
   | `failed` | the copy failed, or a step failed / crashed | retried, up to `WFM_MAX_ATTEMPTS` (3) |
   | `running` | being processed (left behind only if the machine stopped) | processed again |
3. **Upload there?** `<WFM_REPORTS_S3_ROOT>/<survey id>/` must have files.
4. **MIGRATE** to `<WFM_STAGE_S3>/Zone D/Phase 1/Cycle 2/N_04018_05002_AP/Deliverable 1 - RAMS Data/...`
   (d2, d3, d4 alike). The UPC folder is emptied first. Zips are opened into the folder of their name, upload
   timestamps are removed (a file uploaded twice: the newest), videos are left out, damaged zip entries are recovered
   and checked by CRC. The manifest of every file goes to `<WFM_STAGE_S3>/_manifests/<survey id>.json` and
   `logs/wfm/manifests/`.
5. **The stages**, one `run.py --scheduled --only <stage> --zone D --phase 1 --cycle 2 --upc N/04018/05002/AP` each,
   with `RAW_S3_ROOT=<WFM_STAGE_S3>`: **bronze → silver → images → gold → push_silver → push_gold** — every S3 layer
   first, the database only after gold.
   - stage OK → the next stage;
   - a **WARNING** of a stage in `WFM_CONTINUE_ON_WARNING` (default `bronze,silver,images`: data quality, e.g. a
     missing CRRI PCI workbook, silver DQ checks against D2) → recorded in the processed log, the next stage runs;
   - any other warning, a **FAILED** step, or a database **check with blockers** → the survey stops there;
   - push_silver / push_gold write only after their own clean check (one transaction each; an error rolls back).
   Each survey is its own run; gold only changes the rows of that survey.
6. The processed log gets the result, the passed warnings, every run log folder (`logs/run_<time>/`) and, when
   stopped, the step and why.

Surveys run one after another. One survey stopping or failing never stops the next one; only a settings problem
(`run.py` exit 2) stops the whole trigger run.

## Setup (EC2, once — after the repository's own setup in `../README.md`)

```bash
cd /root/dhms/dhms-nsv-pipeline
cp wfm_flow/wfm.env.example wfm_flow/.env && nano wfm_flow/.env    # WFM_DB_NAME if the WFM tables are in another database
.venv/bin/python -m wfm_flow.metadata <a survey id>               # Postgres works: the survey's rows + "ready to process"
.venv/bin/python -m wfm_flow.trigger --dry-run                    # what would run, and why the others would not
.venv/bin/python -m wfm_flow.trigger --survey <id> --no-write     # one survey end to end, nothing written to the DB
.venv/bin/python -m wfm_flow.trigger --survey <id> --force        # the same with the DB writes
chmod +x wfm_flow/install_cron.sh && ./wfm_flow/install_cron.sh --daily-ist 00:30
```

Needs, besides what the pipeline needs: S3 read on `s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/`, read / write
/ delete on `WFM_STAGE_S3` and write on `WFM_STATE_S3`; SELECT on `wfm_survey_schedule`, `wfm_report`, `master_zone`,
`upc_project_zone`, `master_region_zone`, `master_state`, `master_piu` (the last four only for names).

## Commands

```bash
python -m wfm_flow.trigger                        # every new final-approved survey (what cron runs)
python -m wfm_flow.trigger --dry-run              # show only
python -m wfm_flow.trigger --survey <id>          # one survey (still checked against the rules)
python -m wfm_flow.trigger --survey <id> --force  # one survey again / although not final-approved
python -m wfm_flow.trigger --reprocess            # every final-approved survey again (e.g. after a logic change)
python -m wfm_flow.trigger --no-write             # passed to run.py, as are --from / --to / --db
python -m wfm_flow.status                         # every processed survey, newest first (IST)
python -m wfm_flow.status --status stopped        # only these
python -m wfm_flow.status <survey id>             # one survey, every field
python -m wfm_flow.metadata <survey id>           # the Postgres rows of a survey, ready or not
python -m wfm_flow.migrate <survey id> --zone D --phase 1 --cycle 2 --upc N/04018/05002/AP   # only the copy
```

A stopped survey after fixing the cause: `python -m wfm_flow.trigger --survey <id> --force`. To go through a warning by
hand instead (`run.py` asks *Continue?*): `RAW_S3_ROOT=<WFM_STAGE_S3> python run.py --zone D --phase 1 --cycle 2 --upc ...`.

## Cron and logs

```bash
./wfm_flow/install_cron.sh --daily-ist 00:30      # daily 00:30 IST (written in the server's time zone)
./wfm_flow/install_cron.sh 30                     # every 30 minutes
NO_WRITE=1 ./wfm_flow/install_cron.sh ...         # checks only, nothing written
./wfm_flow/install_cron.sh --remove
crontab -l | grep wfm-flow-trigger                # the job
tail -f logs/wfm_trigger_$(date +%Y%m%d).log      # today's trigger output (each survey's run.py output too)
ls -t logs/ | head                                # run_<time>/pipeline.log + summary.csv per survey run
python -m wfm_flow.status                         # started / finished (IST), status, where it stopped
```

## Settings (`wfm_flow/.env`, see `wfm.env.example`)

An environment variable wins over `wfm_flow/.env`, which wins over the repository `.env`; a key that is not set
anywhere takes the default. A key set but empty stays empty (`WFM_STATE_S3=` → processed log only on the machine).

| Key | Default | |
|---|---|---|
| `WFM_REPORTS_S3_ROOT` | `s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/` | the WFM uploads |
| `WFM_STAGE_S3` | `s3://dhms-new-stgae/DHMS/NSV/WFM_Raw/` | migrated copy = `RAW_S3_ROOT` of the runs |
| `WFM_STATE_S3` | `s3://dhms-new-stgae/DHMS/NSV/Pipeline/wfm_state` | processed log mirror |
| `WFM_DB_NAME`, `WFM_DB_SCHEMA` | `DB_NAME`, `DB_SCHEMA` | where `wfm_survey_schedule` / `wfm_report` are |
| `WFM_FINAL_STATUSES` | `Survey Final Approved` | ready statuses (comma separated) |
| `WFM_SOURCE_SYSTEM` | `NSV` | only schedules of this `source_system` |
| `WFM_MAX_ATTEMPTS` | `3` | retries of a failed survey |
| `WFM_CONTINUE_ON_WARNING` | `bronze,silver,images` | stages whose warning is recorded and passed; empty = stop at every warning |
| `WFM_WORK_DIR` | `logs/wfm` | local processed log, Postgres rows (`metadata/<id>.json`), manifests |

## Tests

```bash
python -m pytest -q wfm_flow/tests
```
Upload layout → stage layout, bronze's `run_download.py` finding the stage folders, the stage order (DB last) and the
trigger rules (new / ok / warnings passed / stopped / failed / not ready / dry run).
