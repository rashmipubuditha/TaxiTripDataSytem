#!/usr/bin/env bash
# Start the local Prefect server: UI and run history at http://127.0.0.1:4200
# State (SQLite database) is kept inside the project in .prefect/, so nothing is
# written to your global Prefect profile. Leave this running in its own terminal.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PREFECT_HOME="$PWD/.prefect"
if [[ -x .venv/Scripts/prefect.exe ]]; then PREFECT=.venv/Scripts/prefect.exe; else PREFECT=.venv/bin/prefect; fi
[[ -x "$PREFECT" ]] || { echo "Prefect not installed — run: bash setup.sh"; exit 1; }
echo "Starting Prefect server (PREFECT_HOME=$PREFECT_HOME) — UI: http://127.0.0.1:4200"
exec "$PREFECT" server start --host 127.0.0.1 --port 4200
