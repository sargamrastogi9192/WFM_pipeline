"""
migrate.py - copy one WFM upload into the layout bronze reads: every zip unzipped, upload timestamps removed,
videos left out.

    python -m wfm_flow.migrate <survey id> --zone D --phase 1 --cycle 2 --upc N/04018/05002/AP
    python -m wfm_flow.migrate <local copy of an upload> --survey-id <id> --zone ... --dest <local folder>   (tests)

  from  <WFM_REPORTS_S3_ROOT>/<survey id>/D1..D4/<timestamp>-<file>            (+ zips, see report_storage.py)
  to    <WFM_STAGE_S3>/Zone D/Phase 1/Cycle 2/N_04018_05002_AP/Deliverable 1 - RAMS Data/<file>
                                                          .../Deliverable 2 - Comparison/...   (d3, d4 alike)
        <WFM_STAGE_S3>/_manifests/<survey id>.json        every file: source, size, sha256 / skipped / unreadable

The UPC folder is emptied first, so files of an earlier upload never mix with the new one. The run then reads it
with RAW_S3_ROOT=<WFM_STAGE_S3> (bronze/run_bronze_s3.sh -> run_download.py finds Zone / Phase / Cycle / UPC /
'Deliverable N' folders by name).
"""
import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .report_storage import ReportStorage

VIDEO_EXTENSIONS = (".mp4", ".avi", ".mov", ".mkv", ".wmv", ".m4v", ".mpg", ".mpeg", ".3gp", ".flv", ".webm")
# folder names as in the vendor download (bronze/run_download.py: '^deliverable\s*[-_.]?\s*(\d)')
DELIVERABLE_FOLDERS = {"d1": "Deliverable 1 - RAMS Data", "d2": "Deliverable 2 - Comparison",
                       "d3": "Deliverable 3 - GIS Mapping", "d4": "Deliverable 4 - NHAI One & Inspection Sathi Format"}
WORKERS = 16


def upc_code(upc):
    """'N/04018/05002/AP' -> 'N_04018_05002_AP'"""
    return "_".join(p for p in str(upc).replace("-", "/").replace("_", "/").upper().split("/") if p)


def stage_folder(zone, phase, cycle, upc):
    return f"Zone {zone}/Phase {phase}/Cycle {cycle}/{upc_code(upc)}"


class Target:
    """Files under an s3:// prefix or a local folder."""

    def __init__(self, root):
        self.root = str(root).rstrip("/")
        self.s3 = self.root.startswith("s3://")
        if self.s3:
            import boto3
            self.client = boto3.client("s3")
            self.bucket, _, self.prefix = self.root[5:].partition("/")

    def _key(self, rel):
        return f"{self.prefix}/{rel}".lstrip("/")

    def put(self, rel, data):
        if self.s3:
            self.client.put_object(Bucket=self.bucket, Key=self._key(rel), Body=data)
        else:
            target = Path(self.root) / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)

    def clear(self, folder):
        """Deletes every file under folder; returns how many."""
        if not self.s3:
            base = Path(self.root) / folder
            files = [p for p in base.rglob("*") if p.is_file()] if base.is_dir() else []
            for p in files:
                p.unlink()
            return len(files)
        keys = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket,
                                                                          Prefix=self._key(folder) + "/"):
            keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": keys[i:i + 1000], "Quiet": True})
        return len(keys)

    def url(self, rel=""):
        return f"{self.root}/{rel}".rstrip("/")


def migrate(source, survey_id, zone, phase, cycle, upc, dest, log=print):
    """Copy + unzip one upload into dest. Returns the manifest (dict)."""
    upload = ReportStorage.open(source)
    if not upload.folders:
        raise RuntimeError(f"no deliverable folder (D1..D4) in {upload.describe()}")
    out = Target(dest)
    folder = stage_folder(zone, phase, cycle, upc)
    files = upload.list_files()
    removed = out.clear(folder)
    manifest = {"survey_id": survey_id, "source": upload.describe(), "stage_root": out.url(),
                "location": out.url(folder), "zone": zone, "phase": phase, "cycle": cycle, "upc": upc,
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "deliverable_folders": dict(upload.folders), "zips": getattr(upload, "zips", []),
                "earlier_files_removed": removed, "files": [], "skipped_videos": [], "unreadable": []}
    log(f"MIGRATE {upload.describe()} -> {out.url(folder)}  ({len(files)} file(s) after unzipping, "
        f"{removed} earlier file(s) removed)")

    def copy(path):
        d, _, rest = path.partition("/")
        if PurePosixPath(rest).suffix.lower() in VIDEO_EXTENSIONS:
            return "video", path, None
        if d not in DELIVERABLE_FOLDERS:
            return "unreadable", path, f"not in a deliverable folder d1..d4 ({d})"
        try:
            data = upload.read_bytes(path)
        except Exception as e:                                   # noqa: BLE001 - listed in the manifest
            return "unreadable", path, f"{type(e).__name__}: {e}"
        rel = f"{folder}/{DELIVERABLE_FOLDERS[d]}/{rest}"
        out.put(rel, data)
        return "ok", path, {"path": rel, "deliverable": d, "source": upload.full_path(path), "uploaded_name": upload.uploaded_name(path),
                            "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for status, path, info in pool.map(copy, files):
            if status == "ok":
                manifest["files"].append(info)
            elif status == "video":
                manifest["skipped_videos"].append(path)
            else:
                manifest["unreadable"].append({"path": path, "error": info})
                log(f"  cannot copy '{path}': {info}")
    manifest.update(finished_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    deliverables=sorted({f["deliverable"] for f in manifest["files"]}),
                    copied=len(manifest["files"]), videos_skipped=len(manifest["skipped_videos"]),
                    unreadable_count=len(manifest["unreadable"]),
                    zip_entries_recovered=getattr(upload, "zip_recovered", 0),
                    other_files_in_root=upload.other, superseded_uploads=upload.superseded)
    out.put(f"_manifests/{survey_id}.json", json.dumps(manifest, indent=2, default=str).encode("utf-8"))
    log(f"  copied {manifest['copied']} file(s) (deliverables {manifest['deliverables']}), "
        f"{manifest['videos_skipped']} video(s) left out, {manifest['unreadable_count']} unreadable, "
        f"{manifest['zip_entries_recovered']} zip entr(y/ies) recovered")
    return manifest


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    from .settings import WfmSettings
    cfg = WfmSettings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", help="survey id, s3:// upload folder or local folder")
    ap.add_argument("--survey-id", help="survey id (when the source folder is not named by it)")
    for k in ("zone", "phase", "cycle", "upc"):
        ap.add_argument(f"--{k}", required=True)
    ap.add_argument("--dest", default=cfg.stage_root(), help="stage root (default WFM_STAGE_S3)")
    o = ap.parse_args(args)
    sid = o.survey_id or o.source.rstrip("/\\").replace("\\", "/").split("/")[-1]
    source = o.source if ("/" in o.source or "\\" in o.source) else f"{cfg.get('WFM_REPORTS_S3_ROOT').rstrip('/')}/{sid}/"
    m = migrate(source, sid, o.zone.upper(), o.phase, o.cycle, o.upc, o.dest)
    return 1 if m["unreadable_count"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
