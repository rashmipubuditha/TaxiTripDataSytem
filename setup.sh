#!/usr/bin/env bash
# =============================================================================
# setup.sh — prepares the local environment for the NYC Taxi batch ETL project.
#
# Steps (each one is safe to re-run; finished work is skipped):
#   1. Create .env from .env.example (first run only) and load it
#   2. Check free disk space
#   3. Create the Python virtual environment (.venv)
#   4. Install Python dependencies (skipped if requirements.txt is unchanged)
#   5. Start PostgreSQL in Docker and wait until it is healthy
#   6. Apply sql/schema.sql (idempotent; skipped if the file does not exist yet)
#   7. Download any missing monthly Parquet files with curl (valid files are kept)
#   8. Download the taxi zone lookup CSV
#
# Usage:  bash setup.sh [--months YYYY-MM,...] [--skip-downloads] [--reset-db] [--help]
#   --months          override MONTHS from .env for this run (e.g. dev profile: 2023-01,2023-02)
#   --skip-downloads  only check files; never download
#   --reset-db        DROP all project tables and data, then re-create the schema (development only)
# Runs in Git Bash on Windows and in Bash on Linux/macOS.
# =============================================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

SKIP_DOWNLOADS=false
RESET_DB=false
MONTHS_OVERRIDE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-downloads) SKIP_DOWNLOADS=true; shift ;;
    --reset-db) RESET_DB=true; shift ;;
    --months) [[ -n "${2:-}" ]] || { echo "--months needs a value, e.g. --months 2023-01,2023-02"; exit 2; }
              MONTHS_OVERRIDE="$2"; shift 2 ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1 (use --help)"; exit 2 ;;
  esac
done

mkdir -p logs
LOG_FILE="logs/setup.log"
# Show output on screen and append it to logs/setup.log.
exec > >(tee -a "$LOG_FILE") 2>&1

# ---------- helpers ----------------------------------------------------------
log()  { printf '%s [INFO]  %s\n'  "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
warn() { printf '%s [WARN]  %s\n'  "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
fail() { printf '%s [ERROR] %s\n'  "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; exit 1; }
step() { printf '\n%s ===== %s =====\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# Free space in whole GB for the drive/filesystem that holds path $1.
free_gb() { df -Pk "$1" | awk 'NR==2 { print int($4 / 1048576) }'; }

# A Parquet file starts and ends with the 4 bytes "PAR1". Checking both ends also
# catches downloads that were cut off half-way.
is_valid_parquet() {
  local f="$1"
  [[ -s "$f" ]] || return 1
  [[ "$(head -c 4 "$f")" == "PAR1" ]] || return 1
  [[ "$(tail -c 4 "$f")" == "PAR1" ]] || return 1
}

# Download URL $1 to file $2 safely: write to .part first, rename only on success.
download() {
  local url="$1" dest="$2"
  rm -f "${dest}.part"
  if curl -fL --retry 3 --retry-delay 5 --connect-timeout 20 -sS -o "${dest}.part" "$url"; then
    mv "${dest}.part" "$dest"
  else
    rm -f "${dest}.part"
    return 1
  fi
}

START_TS=$(date +%s)
step "NYC Taxi ETL setup started (project: $PROJECT_DIR)"

# ---------- 1. .env ------------------------------------------------------------
step "1/8 Configuration (.env)"
if [[ ! -f .env ]]; then
  [[ -f .env.example ]] || fail ".env.example is missing — cannot create .env"
  cp .env.example .env
  log "Created .env from .env.example (edit POSTGRES_PASSWORD if you like)"
else
  log ".env already exists — keeping it"
fi
# Load .env into the environment. 'tr -d \r' protects against Windows line endings.
set -a
# shellcheck disable=SC1090
source <(tr -d '\r' < .env)
set +a
for var in POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB POSTGRES_PORT MONTHS RAW_DIR_PATTERN TLC_BASE_URL ZONE_LOOKUP_URL REFERENCE_DIR; do
  [[ -n "${!var:-}" ]] || fail "Variable $var is not set in .env"
done
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-taxi-postgres}"
if [[ -n "$MONTHS_OVERRIDE" ]]; then
  MONTHS="$MONTHS_OVERRIDE"
  log "Months (from --months): $MONTHS"
else
  log "Months (from .env): $MONTHS"
fi

# ---------- 2. Disk space ------------------------------------------------------------
step "2/8 Disk space"
project_free=$(free_gb "$PROJECT_DIR")
log "Free space for the project folder: ${project_free} GB (need ≥ ${MIN_FREE_GB_PROJECT:-2} GB)"
(( project_free >= ${MIN_FREE_GB_PROJECT:-2} )) || fail "Not enough free space for downloads in $PROJECT_DIR"
if [[ -n "${DOCKER_DISK_PATH:-}" && -d "${DOCKER_DISK_PATH}" ]]; then
  docker_free=$(free_gb "$DOCKER_DISK_PATH")
  log "Free space on Docker's disk drive (${DOCKER_DISK_PATH}): ${docker_free} GB (recommended ≥ ${MIN_FREE_GB_DOCKER:-10} GB)"
  if (( docker_free < ${MIN_FREE_GB_DOCKER:-10} )); then
    warn "Low space for the database. Free space or move Docker's disk image (Docker Desktop → Settings → Resources)."
  fi
else
  log "DOCKER_DISK_PATH not set — skipping the Docker disk check"
fi

# ---------- 3. Python virtual environment ----------------------------------------
step "3/8 Python virtual environment"
PYTHON_BIN=""
for candidate in python python3; do
  if command -v "$candidate" >/dev/null 2>&1 \
     && "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
    PYTHON_BIN="$candidate"; break
  fi
done
[[ -n "$PYTHON_BIN" ]] || fail "Python 3.10+ not found on PATH"
log "Using $("$PYTHON_BIN" --version 2>&1) ($(command -v "$PYTHON_BIN"))"

if [[ ! -d .venv ]]; then
  "$PYTHON_BIN" -m venv .venv
  log "Created .venv"
else
  log ".venv already exists — keeping it"
fi
# Windows venvs use Scripts/, Linux/macOS venvs use bin/.
if [[ -x .venv/Scripts/python.exe ]]; then VENV_PY=".venv/Scripts/python.exe"; else VENV_PY=".venv/bin/python"; fi
[[ -x "$VENV_PY" ]] || fail "Virtual environment looks broken (no $VENV_PY). Delete .venv and re-run."

# ---------- 4. Dependencies ----------------------------------------------------------
step "4/8 Python dependencies"
REQ_HASH_FILE=".venv/.requirements.sha256"
req_hash=$(sha256sum requirements.txt | awk '{print $1}')
if [[ -f "$REQ_HASH_FILE" && "$(cat "$REQ_HASH_FILE")" == "$req_hash" ]]; then
  log "requirements.txt unchanged since last install — skipping pip"
else
  log "Installing packages from requirements.txt (first run can take several minutes)"
  "$VENV_PY" -m pip install --upgrade pip --quiet
  "$VENV_PY" -m pip install -r requirements.txt --quiet || fail "pip install failed — see the messages above"
  echo "$req_hash" > "$REQ_HASH_FILE"
  log "Dependencies installed"
fi

# ---------- 5. PostgreSQL in Docker --------------------------------------------------
step "5/8 PostgreSQL (Docker)"
command -v docker >/dev/null 2>&1 || fail "Docker is not installed or not on PATH"
docker info >/dev/null 2>&1 || fail "Docker engine is not running — start Docker Desktop and re-run"
# Pull the image only if it is not cached; retry because Docker Hub can time out.
pg_image=$(docker compose config --images postgres 2>/dev/null | head -1)
if docker image inspect "$pg_image" >/dev/null 2>&1; then
  log "Image $pg_image is already available locally"
else
  for attempt in 1 2 3; do
    log "Pulling $pg_image (attempt $attempt/3)"
    docker compose pull postgres && break
    [[ "$attempt" -eq 3 ]] && fail "Could not pull $pg_image from Docker Hub — check the internet connection and re-run"
    sleep 10
  done
fi
docker compose up -d postgres || fail "docker compose up failed — check: docker compose logs postgres"
log "Waiting for container '$POSTGRES_CONTAINER' to become healthy"
for i in $(seq 1 60); do
  status=$(docker inspect -f '{{.State.Health.Status}}' "$POSTGRES_CONTAINER" 2>/dev/null || echo "missing")
  [[ "$status" == "healthy" ]] && break
  [[ "$i" -eq 60 ]] && fail "PostgreSQL did not become healthy within 2 minutes (last status: $status). Check: docker compose logs postgres"
  sleep 2
done
log "PostgreSQL is healthy on localhost:${POSTGRES_PORT} (database: ${POSTGRES_DB})"

# ---------- 6. Database schema -------------------------------------------------------
step "6/8 Database schema"
# psql runs inside the container, so no local PostgreSQL client is needed.
if [[ "$RESET_DB" == true ]]; then
  warn "--reset-db: dropping ALL tables and data in database '${POSTGRES_DB}'"
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" \
    || fail "Database reset failed"
  log "Database emptied"
fi
if [[ -f sql/schema.sql ]]; then
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/schema.sql \
    || fail "Applying sql/schema.sql failed — see the psql error above"
  log "sql/schema.sql applied"
  if [[ -f sql/views.sql ]]; then
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q \
      -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/views.sql \
      || fail "Applying sql/views.sql failed — see the psql error above"
    log "sql/views.sql applied (metric views)"
  fi
else
  warn "sql/schema.sql not found yet — skipping (it is created in Phase 2)"
fi

# ---------- 7. Monthly trip data -----------------------------------------------------
step "7/8 Monthly trip data"
declare -a downloaded=() kept=() failed=()
IFS=',' read -r -a month_list <<< "${MONTHS// /}"
for month in "${month_list[@]}"; do
  if [[ ! "$month" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; then
    warn "Skipping invalid month '$month' (expected YYYY-MM)"; failed+=("$month"); continue
  fi
  year="${month:0:4}"
  dir="${RAW_DIR_PATTERN//\{year\}/$year}"
  file="$dir/yellow_tripdata_${month}.parquet"
  mkdir -p "$dir"
  if is_valid_parquet "$file"; then
    kept+=("$month")
    log "$file is present and valid — keeping it"
  elif [[ "$SKIP_DOWNLOADS" == true ]]; then
    warn "$file is missing or invalid, and --skip-downloads was given"; failed+=("$month")
  else
    [[ -e "$file" ]] && warn "$file exists but is not a complete Parquet file — downloading again"
    url="${TLC_BASE_URL}/yellow_tripdata_${month}.parquet"
    log "Downloading $url"
    if download "$url" "$file" && is_valid_parquet "$file"; then
      downloaded+=("$month")
      log "Saved $file ($(du -h "$file" | cut -f1))"
    else
      rm -f "$file"
      failed+=("$month")
      warn "Download failed for $month (not published yet, or no internet)"
    fi
  fi
done

# ---------- 8. Taxi zone lookup ------------------------------------------------------
step "8/8 Taxi zone lookup"
mkdir -p "$REFERENCE_DIR"
zone_file="$REFERENCE_DIR/taxi_zone_lookup.csv"
if [[ -s "$zone_file" ]] && head -1 "$zone_file" | grep -q "LocationID"; then
  log "$zone_file is present — keeping it ($(($(wc -l < "$zone_file") - 1)) zones)"
elif [[ "$SKIP_DOWNLOADS" == true ]]; then
  warn "$zone_file is missing and --skip-downloads was given"
else
  log "Downloading $ZONE_LOOKUP_URL"
  if download "$ZONE_LOOKUP_URL" "$zone_file" && head -1 "$zone_file" | grep -q "LocationID"; then
    log "Saved $zone_file ($(($(wc -l < "$zone_file") - 1)) zones)"
  else
    rm -f "$zone_file"
    warn "Zone lookup download failed — the pipeline will fall back to zone IDs without names"
  fi
fi

# ---------- Summary ------------------------------------------------------------------
step "Summary"
log "Months kept (already valid): ${#kept[@]}  ${kept[*]:-}"
log "Months downloaded now:       ${#downloaded[@]}  ${downloaded[*]:-}"
log "Months failed:               ${#failed[@]}  ${failed[*]:-}"
log "Python: $VENV_PY | Database: postgresql://${POSTGRES_USER}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
log "Finished in $(( $(date +%s) - START_TS )) s. Log: $LOG_FILE"
if (( ${#failed[@]} > 0 )); then
  fail "Some months are not available: ${failed[*]}"
fi
log "Setup complete."
