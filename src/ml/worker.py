from __future__ import annotations

import signal
import time
from pathlib import Path

from common.config import settings
from common.db import close_pool, get_conn
from common.logging import get_logger
from ml.infer import score_unscored
from ml.train import load_bundle, train_and_save

log = get_logger("ml.worker")
_running = True


def _handle_stop(signum, _frame) -> None:
    global _running
    log.info("shutdown_signal", extra={"event": "shutdown", "count": signum})
    _running = False


def _clean_count() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM clean.network_metrics")
            return int(cur.fetchone()[0])


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    bundle = None
    model_path = Path(settings.model_path)
    while _running and bundle is None:
        if model_path.exists():
            try:
                bundle = load_bundle()
                log.info("model_loaded", extra={"event": "train", "count": bundle.get("train_rows")})
                break
            except Exception as exc:  # noqa: BLE001
                log.warning("model_load_failed", extra={"event": "train", "error": str(exc)})
        n = _clean_count()
        if n >= settings.ml_min_train_rows:
            try:
                bundle = train_and_save()
            except Exception as exc:  # noqa: BLE001
                log.warning("train_wait", extra={"event": "train", "error": str(exc), "count": n})
        else:
            log.info("waiting_for_data", extra={"event": "train", "count": n})
        for _ in range(15):
            if not _running:
                close_pool()
                return
            time.sleep(1)

    while _running and bundle is not None:
        try:
            scored = 1
            while _running and scored:
                scored = score_unscored(bundle)
        except Exception as exc:  # noqa: BLE001
            log.warning("infer_error", extra={"event": "infer", "error": str(exc)})
        for _ in range(int(settings.ml_score_interval_seconds)):
            if not _running:
                break
            time.sleep(1)
    close_pool()


if __name__ == "__main__":
    run()
