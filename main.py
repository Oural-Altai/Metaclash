"""
main.py — Meta Clash ETL pipeline orchestrator.

Chains Extract → Transform → Load in a single run.
Runs locally (CLI) or as a Cloud Run job (HTTP trigger via Flask).

Environment variables:
    ENV                 : "local" (default) or "gcp"
    GCP_PROJECT_ID      : GCP project ID (required in gcp mode)
    BQ_DATASET_ID       : BigQuery dataset name (default: "clash_royale")
    CLASH_ROYALE_TOKEN  : API token (local mode only)
    SECRET_NAME         : Secret Manager secret name (gcp mode, default: "clash-royale-token")
    LOG_LEVEL           : DEBUG / INFO / WARNING (default: INFO)
    PORT                : HTTP port for Cloud Run (default: 8080)

Local usage:
    python main.py

Cloud Run:
    POST /run   → triggers the full pipeline
    GET  /health → healthcheck endpoint
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Logging setup — must happen before any other import
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    """Configure logging for local (console) or GCP (Cloud Logging + console)."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    env = os.getenv("ENV", "local")

    handlers: list[logging.Handler] = []

    # Console handler — always active
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(log_level)
    console.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    handlers.append(console)

    # Google Cloud Logging — only in GCP mode
    if env == "gcp":
        try:
            import google.cloud.logging as cloud_logging
            cloud_client = cloud_logging.Client()
            cloud_handler = cloud_logging.handlers.CloudLoggingHandler(cloud_client)
            cloud_handler.setLevel(log_level)
            handlers.append(cloud_handler)
        except Exception as e:
            # Don't crash if Cloud Logging isn't available
            print(f"[WARNING] Could not set up Cloud Logging: {e}", file=sys.stderr)

    logging.basicConfig(level=log_level, handlers=handlers, force=True)
    return logging.getLogger("meta_clash.main")


logger = setup_logging()


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def run_extract(api_client) -> dict:
    """Step 1 — Extract raw data from Clash Royale API."""
    from src.extract import Extractor

    logger.info("── Step 1/3 : EXTRACT ──────────────────────────")
    extractor = Extractor(api_client=api_client)
    raw_data = extractor.run()

    n_players = len(raw_data.get("players", []))
    n_battlelogs = len(raw_data.get("battlelogs", []))
    n_cards = len(raw_data.get("cards", []))
    logger.info(
        "Extract done — players=%d  battlelogs=%d  cards=%d",
        n_players, n_battlelogs, n_cards,
    )
    return raw_data


def run_transform(raw_data: dict) -> dict:
    """Step 2 — Transform raw data into BigQuery-ready tables."""
    from src.transform import Transformer

    logger.info("── Step 2/3 : TRANSFORM ────────────────────────")
    transformer = Transformer(raw_data)
    tables = transformer.run()

    for table_name, rows in tables.items():
        logger.info("  %-20s : %d rows", table_name, len(rows))

    return tables


def run_load(tables: dict) -> dict:
    """Step 3 — Load transformed tables into BigQuery."""
    from src.load import Loader

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID", "clash_royale")

    if not project_id:
        raise EnvironmentError(
            "GCP_PROJECT_ID is not set. "
            "Add it to your .env file or environment variables."
        )

    logger.info("── Step 3/3 : LOAD ─────────────────────────────")
    loader = Loader(project_id=project_id, dataset_id=dataset_id)
    loader.ensure_tables()
    results = loader.run(tables)

    total = sum(results.values())
    logger.info("Load done — %d total rows inserted", total)
    for table_name, count in results.items():
        logger.info("  %-20s : %d rows inserted", table_name, count)

    return results


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

class PipelineResult:
    """Holds the outcome of a full pipeline run."""

    def __init__(self):
        self.started_at: str = datetime.now(timezone.utc).isoformat()
        self.finished_at: str | None = None
        self.duration_seconds: float | None = None
        self.success: bool = False
        self.steps: dict[str, str] = {
            "extract":   "pending",
            "transform": "pending",
            "load":      "pending",
        }
        self.row_counts: dict[str, int] = {}
        self.errors: list[str] = []

    def to_dict(self) -> dict:
        return {
            "started_at":        self.started_at,
            "finished_at":       self.finished_at,
            "duration_seconds":  self.duration_seconds,
            "success":           self.success,
            "steps":             self.steps,
            "row_counts":        self.row_counts,
            "errors":            self.errors,
        }


def run_pipeline() -> PipelineResult:
    """Execute the full ETL pipeline: Extract → Transform → Load.

    Errors are logged and recorded but do not crash the process.
    Each step is attempted regardless of prior failures where possible.

    Returns a PipelineResult with status of each step.
    """
    from src.api_client import ClashRoyalClient

    result = PipelineResult()
    t_start = time.perf_counter()

    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║       META CLASH — ETL Pipeline start        ║")
    logger.info("╚══════════════════════════════════════════════╝")
    logger.info("ENV=%s  PROJECT=%s  DATASET=%s",
                os.getenv("ENV", "local"),
                os.getenv("GCP_PROJECT_ID", "not set"),
                os.getenv("BQ_DATASET_ID", "clash_royale"))

    # ── Step 1: Extract ────────────────────────────────────────────────────
    raw_data = {}
    try:
        api_client = ClashRoyalClient()
        raw_data = run_extract(api_client)
        result.steps["extract"] = "success"
    except Exception as e:
        msg = f"Extract failed: {e}"
        logger.error(msg, exc_info=True)
        result.steps["extract"] = "failed"
        result.errors.append(msg)
        # Cannot transform or load without data — finalize and return early
        _finalize(result, t_start)
        return result

    # ── Step 2: Transform ──────────────────────────────────────────────────
    tables = {}
    try:
        tables = run_transform(raw_data)
        result.steps["transform"] = "success"
    except Exception as e:
        msg = f"Transform failed: {e}"
        logger.error(msg, exc_info=True)
        result.steps["transform"] = "failed"
        result.errors.append(msg)
        # Cannot load without transformed data — finalize and return early
        _finalize(result, t_start)
        return result

    # ── Step 3: Load ───────────────────────────────────────────────────────
    try:
        row_counts = run_load(tables)
        result.steps["load"] = "success"
        result.row_counts = row_counts
    except Exception as e:
        msg = f"Load failed: {e}"
        logger.error(msg, exc_info=True)
        result.steps["load"] = "failed"
        result.errors.append(msg)
        # Continue — we still finalize and report what we have

    # ── Finalize ───────────────────────────────────────────────────────────
    _finalize(result, t_start)

    if result.errors:
        logger.warning(
            "Pipeline finished with %d error(s): %s",
            len(result.errors), result.errors,
        )
    else:
        logger.info(
            "Pipeline finished successfully in %.1fs",
            result.duration_seconds,
        )

    return result


def _finalize(result: PipelineResult, t_start: float) -> None:
    """Set finish time and duration on the result object."""
    result.finished_at = datetime.now(timezone.utc).isoformat()
    result.duration_seconds = round(time.perf_counter() - t_start, 2)
    result.success = not result.errors


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_local() -> None:
    """Entry point for local CLI execution: python main.py"""
    from dotenv import load_dotenv
    load_dotenv()

    result = run_pipeline()

    # Print summary
    print("\n" + "─" * 50)
    print("PIPELINE SUMMARY")
    print("─" * 50)
    for step, status in result.steps.items():
        icon = "✅" if status == "success" else ("❌" if status == "failed" else "⏭")
        print(f"  {icon}  {step:<12} {status}")

    if result.row_counts:
        print("\nROWS INSERTED")
        for table, count in result.row_counts.items():
            print(f"  {'·'} {table:<22} {count}")

    if result.errors:
        print("\nERRORS")
        for err in result.errors:
            print(f"  ⚠  {err}")

    print(f"\nDuration : {result.duration_seconds}s")
    print(f"Status   : {'SUCCESS' if result.success else 'PARTIAL / FAILED'}")
    print("─" * 50)

    sys.exit(0 if result.success else 1)


def run_cloud() -> None:
    """Entry point for Cloud Run: starts a Flask HTTP server.

    Routes:
        POST /run    → triggers the ETL pipeline
        GET  /health → returns 200 if the server is alive
    """
    try:
        from flask import Flask, jsonify, request
    except ImportError:
        logger.error("Flask is not installed. Run: pip install flask")
        sys.exit(1)

    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"}), 200

    @app.route("/run", methods=["POST"])
    def trigger_pipeline():
        logger.info("Pipeline triggered via HTTP POST /run")
        result = run_pipeline()
        status_code = 200 if result.success else 207  # 207 = partial success
        return jsonify(result.to_dict()), status_code

    port = int(os.getenv("PORT", 8080))
    logger.info("Starting Cloud Run server on port %d", port)
    app.run(host="0.0.0.0", port=port)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    env = os.getenv("ENV", "local")

    if env == "gcp":
        run_cloud()
    else:
        run_local()