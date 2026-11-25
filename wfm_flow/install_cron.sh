#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Install the WFM trigger as a cron job (run once, as the user that owns the repository).
#
#   ./wfm_flow/install_cron.sh --daily-ist 00:30     once a day at 00:30 IST (converted to the server's time zone)
#   ./wfm_flow/install_cron.sh 30                    every 30 minutes
#   NO_WRITE=1 ./wfm_flow/install_cron.sh ...        the job runs with --no-write (checks only, nothing written)
#   ./wfm_flow/install_cron.sh --remove              take the job out again
#
# The job:  cd <repo> && flock -n /tmp/wfm_trigger.lock .venv/bin/python -m wfm_flow.trigger
#           >> logs/wfm_trigger_<date>.log
# flock: a run that is still busy is never started twice (run.py also keeps its own lock, logs/.lock).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$REPO/.venv/bin/python}"
TAG="# wfm-flow-trigger"

if [[ "${1:-}" == "--remove" ]]; then
  { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; } | crontab -
  echo "WFM trigger removed from crontab"
  exit 0
fi

if [[ "${1:-}" == "--daily-ist" ]]; then
  IST="${2:-00:30}"
  [[ "$IST" =~ ^([01]?[0-9]|2[0-3]):([0-5][0-9])$ ]] || { echo "ERROR: bad time '$IST' (use HH:MM, e.g. 00:30)"; exit 2; }
  # IST is always UTC+05:30 -> server time = IST - 330 min + the server's own UTC offset
  OFF=$(date +%z); SIGN=${OFF:0:1}; OFFMIN=$(( 10#${OFF:1:2} * 60 + 10#${OFF:3:2} )); [[ $SIGN == "-" ]] && OFFMIN=$(( -OFFMIN ))
  T=$(( ( 10#${BASH_REMATCH[1]} * 60 + 10#${BASH_REMATCH[2]} - 330 + OFFMIN + 2880 ) % 1440 ))
  SCHEDULE="$(( T % 60 )) $(( T / 60 )) * * *"
  WHEN="daily at $IST IST = $(printf '%02d:%02d' $(( T / 60 )) $(( T % 60 ))) server time (UTC$OFF)"
else
  MINUTES="${1:-30}"
  SCHEDULE="*/$MINUTES * * * *"
  WHEN="every $MINUTES min"
fi
[[ -x "$PYTHON" ]] || { echo "ERROR: $PYTHON not found - create the venv first (README.md, Setup)"; exit 2; }
[[ -f "$REPO/.env" ]] || echo "WARNING: no $REPO/.env - cp .env.example .env and fill it in"
[[ -f "$REPO/wfm_flow/.env" ]] || echo "NOTE: no wfm_flow/.env - the defaults of wfm_flow/settings.py are used"
command -v crontab >/dev/null || { echo "ERROR: cron is not installed - run: apt install -y cron && systemctl enable --now cron"; exit 2; }

ARGS=""
[[ "${NO_WRITE:-}" == "1" ]] && ARGS="$ARGS --no-write"
mkdir -p "$REPO/logs"
LINE="$SCHEDULE cd '$REPO' && flock -n /tmp/wfm_trigger.lock '$PYTHON' -m wfm_flow.trigger$ARGS >> '$REPO'/logs/wfm_trigger_\$(date +\\%Y\\%m\\%d).log 2>&1 $TAG"
{ { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; }; echo "$LINE"; } | crontab -
echo "installed ($WHEN):"
crontab -l | grep "$TAG" || { echo "ERROR: the job is not in the crontab"; exit 1; }
systemctl is-active --quiet cron 2>/dev/null || echo "WARNING: the cron service is not running - run: systemctl enable --now cron"
