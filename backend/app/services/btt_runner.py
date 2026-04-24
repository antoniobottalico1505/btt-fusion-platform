import csv
import json
import subprocess
import sys
import threading
import time
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


def _set_job_fields(db: Session, job_id: int, **fields) -> BttJob | None:
    job = db.get(BttJob, job_id)
    if not job:
        return None

    for k, v in fields.items():
        setattr(job, k, v)

    db.commit()
    db.refresh(job)
    return job


def _run_job(job_id: int, db_factory):
    db: Session = db_factory()
    proc: subprocess.Popen | None = None

    try:
        job = db.get(BttJob, job_id)
        if not job:
            return

        run_dir = Path(job.run_dir)
        preset = get_btt_preset()

        _set_job_fields(
            db,
            job_id,
            status='running',
            stdout_log='',
            error_log='',
        )

        cmd = _build_args(preset, run_dir)

        proc = subprocess.Popen(
            cmd,
            cwd=str(run_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        chunks: list[str] = []
        last_flush = 0.0

        if proc.stdout is not None:
            for line in proc.stdout:
                chunks.append(line)
                now = time.time()

                if now - last_flush >= 1.0:
                    _set_job_fields(
                        db,
                        job_id,
                        status='running',
                        stdout_log=''.join(chunks)[-40000:],
                    )
                    last_flush = now

        return_code = proc.wait()
        stdout_text = ''.join(chunks)

        top_csv = run_dir / 'btt_capital_top.csv'
        weights_csv = run_dir / 'btt_capital_weights.csv'
        failed_csv = run_dir / 'btt_capital_failed.csv'
        report_html = run_dir / 'btt_capital_report.html'

        summary = {
            'preset': preset,
            'top_rows': _read_csv_rows(top_csv, limit=25),
            'portfolio_rows': _read_csv_rows(weights_csv, limit=20),
            'failed_rows': _read_csv_rows(failed_csv, limit=25),
            'return_code': return_code,
        }

        _set_job_fields(
            db,
            job_id,
            status='done' if return_code == 0 else 'failed',
            stdout_log=stdout_text[-40000:],
            error_log='' if return_code == 0 else stdout_text[-4000:],
            top_csv_path=str(top_csv),
            weights_csv_path=str(weights_csv),
            failed_csv_path=str(failed_csv),
            report_path=str(report_html),
            summary_json=json.dumps(summary, ensure_ascii=False),
        )

    except Exception as exc:
        _set_job_fields(
            db,
            job_id,
            status='failed',
            error_log=f'{type(exc).__name__}: {exc}',
        )
    finally:
        try:
            if proc and proc.stdout:
                proc.stdout.close()
        except Exception:
            pass

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