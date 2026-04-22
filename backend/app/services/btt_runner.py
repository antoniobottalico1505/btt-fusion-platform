import csv
import json
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.models import BttJob
from app.services.admin_config import get_btt_preset
from app.services.storage import PRIVATE, engine_paths

_active_jobs_lock = threading.Lock()
_active_jobs: set[int] = set()


def _build_args(preset: dict[str, Any], run_dir: Path) -> list[str]:
    args = [
        sys.executable,
        str(engine_paths()['btt']),
        '--countries', str(preset.get('countries') or ''),
        '--max-per-country', str(preset.get('max_per_country') or 30),
        '--shortlist-multiplier', str(preset.get('shortlist_multiplier') or 4),
        '--workers', str(preset.get('workers') or 6),
        '--top', str(preset.get('top') or 50),
        '--portfolio-size', str(preset.get('portfolio_size') or 12),
        '--resume-cache', str(run_dir / 'resume_cache.json'),
        '--output-prefix', str(run_dir / 'btt_capital'),
    ]
    if preset.get('all_countries'):
        args.append('--all-countries')
    if preset.get('emerging_only'):
        args.append('--emerging-only')
    if preset.get('technical_refine'):
        args.append('--technical-refine')
    return args


def _read_csv_rows(path: Path, limit: int = 20) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        return [row for _, row in zip(range(limit), reader)]


def _run_job(job_id: int, db_factory):
    db: Session = db_factory()
    try:
        job = db.get(BttJob, job_id)
        if not job:
            return
        run_dir = Path(job.run_dir)
        preset = get_btt_preset()
        proc = subprocess.run(
            _build_args(preset, run_dir),
            cwd=str(run_dir),
            capture_output=True,
            text=True,
        )
        top_csv = run_dir / 'btt_capital_top.csv'
        weights_csv = run_dir / 'btt_capital_weights.csv'
        failed_csv = run_dir / 'btt_capital_failed.csv'
        report_html = run_dir / 'btt_capital_report.html'
        summary = {
            'preset': preset,
            'top_rows': _read_csv_rows(top_csv, limit=25),
            'portfolio_rows': _read_csv_rows(weights_csv, limit=20),
            'failed_rows': _read_csv_rows(failed_csv, limit=25),
            'return_code': proc.returncode,
        }
        job.status = 'done' if proc.returncode == 0 else 'failed'
        job.stdout_log = proc.stdout[-40000:]
        job.error_log = proc.stderr[-40000:]
        job.top_csv_path = str(top_csv)
        job.weights_csv_path = str(weights_csv)
        job.failed_csv_path = str(failed_csv)
        job.report_path = str(report_html)
        job.summary_json = json.dumps(summary, ensure_ascii=False)
        db.commit()
    except Exception as exc:
        job = db.get(BttJob, job_id)
        if job:
            job.status = 'failed'
            job.error_log = f'{type(exc).__name__}: {exc}'
            db.commit()
    finally:
        with _active_jobs_lock:
            _active_jobs.discard(job_id)
        db.close()


def create_btt_job(db: Session, user_id: int | None, db_factory) -> BttJob:
    now = datetime.now(timezone.utc)
    run_dir = PRIVATE / 'btt_runs' / now.strftime('%Y%m%d_%H%M%S_%f')
    run_dir.mkdir(parents=True, exist_ok=True)
    job = BttJob(user_id=user_id, status='queued', run_dir=str(run_dir))
    db.add(job)
    db.commit()
    db.refresh(job)

    with _active_jobs_lock:
        _active_jobs.add(job.id)
    thread = threading.Thread(target=_run_job, args=(job.id, db_factory), daemon=True)
    thread.start()
    return job
