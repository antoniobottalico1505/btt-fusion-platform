#!/usr/bin/env bash
set -euo pipefail
mkdir -p ./storage
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"