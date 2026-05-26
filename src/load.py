"""
load.py — Clash Royale BigQuery loading module.

Handles all writes to BigQuery for the Meta Clash pipeline:
  - Schema definitions for all 6 tables
  - Table creation with partitioning / clustering
  - Upsert logic (MERGE) for slowly-changing tables (players, cards)
  - Append logic for time-series tables (snapshots, rankings, battles, decks)
  - Batch insert with retry and error reporting

Tables managed:
  cards             → UPSERT on card_id         (static reference)
  players           → UPSERT on player_tag       (current profile)
  player_snapshots  → APPEND partitioned by extracted_at
  rankings          → APPEND partitioned by extracted_at
  battles           → APPEND partitioned by battle_time, deduplicated
  battle_decks      → APPEND partitioned by battle_time, deduplicated

Usage:
    from src.load import Loader

    loader = Loader(project_id="my-project", dataset_id="clash_royale")
    loader.ensure_tables()
    loader.run(transformed_tables)
"""

import logging
import time
from typing import Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Max rows per insert_rows_json() call — BQ hard limit is 10 MB / 50 000 rows
BATCH_SIZE = 500

# Number of retries on transient BQ errors
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {

    "cards": [
        bigquery.SchemaField("card_id",             "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("name",                "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("max_level",           "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("max_evolution_level", "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("rarity",              "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("elixir_cost",         "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("icon_url",            "STRING",    mode="NULLABLE"),
    ],

    "players": [
        bigquery.SchemaField("player_tag",               "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("name",                     "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("exp_level",                "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("trophies",                 "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("best_trophies",            "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("wins",                     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("losses",                   "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("battle_count",             "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("three_crown_wins",         "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("challenge_max_wins",       "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("challenge_cards_won",      "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("tournament_cards_won",     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("tournament_battle_count",  "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("total_donations",          "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("war_day_wins",             "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("clan_tag",                 "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("clan_name",                "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("arena_id",                 "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("arena_name",               "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("category",                 "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("star_points",              "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("total_exp_points",         "INTEGER",   mode="NULLABLE"),
    ],

    "player_snapshots": [
        bigquery.SchemaField("player_tag",               "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("extracted_at",             "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("trophies",                 "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("exp_level",                "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("wins",                     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("losses",                   "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("battle_count",             "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("current_win_lose_streak",  "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("donations",                "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("donations_received",       "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("pol_league_number",        "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("pol_trophies",             "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("pol_rank",                 "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("pol_best_trophies",        "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("category",                 "STRING",    mode="NULLABLE"),
    ],

    "rankings": [
        bigquery.SchemaField("player_tag",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("rank",         "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("trophies",     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("exp_level",    "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("category",     "STRING",    mode="NULLABLE"),
    ],

    "battles": [
        bigquery.SchemaField("battle_id",                    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("battle_time",                  "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("battle_type",                  "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("arena_id",                     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("arena_name",                   "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("game_mode_id",                 "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("game_mode_name",               "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("is_ladder_tournament",         "BOOLEAN",   mode="NULLABLE"),
        bigquery.SchemaField("deck_selection",               "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("team_tag",                     "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("team_crowns",                  "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("team_starting_trophies",       "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("team_trophy_change",           "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("opponent_tag",                 "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("opponent_crowns",              "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("opponent_starting_trophies",   "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("opponent_trophy_change",       "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("team_king_tower_hp",           "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("team_princess_tower_1_hp",     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("team_princess_tower_2_hp",     "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("opponent_king_tower_hp",       "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("opponent_princess_tower_1_hp", "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("opponent_princess_tower_2_hp", "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("crawler_tag",                  "STRING",    mode="NULLABLE"),
    ],

    "battle_decks": [
        bigquery.SchemaField("battle_id",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("battle_time",         "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("player_tag",          "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("side",                "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("is_winner",           "BOOLEAN",   mode="NULLABLE"),
        bigquery.SchemaField("crowns",              "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("deck_card_ids",       "INTEGER",   mode="REPEATED"),
        bigquery.SchemaField("deck_signature",      "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("deck_avg_elixir",     "FLOAT",     mode="NULLABLE"),
        bigquery.SchemaField("deck_avg_level",      "FLOAT",     mode="NULLABLE"),
        bigquery.SchemaField("deck_num_evolutions", "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("deck_num_cards",      "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("king_tower_hp",       "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("princess_tower_1_hp", "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("princess_tower_2_hp", "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("battle_type",         "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("arena_id",            "INTEGER",   mode="NULLABLE"),
    ],
}


# ---------------------------------------------------------------------------
# Table configuration: partitioning + clustering per table
# ---------------------------------------------------------------------------

def _make_table_config(table_name: str) -> dict:
    """Return BigQuery TableReference configuration for each table."""
    configs = {
        "cards": {
            "clustering_fields": None,
            "time_partitioning": None,
        },
        "players": {
            "clustering_fields": ["category", "arena_id"],
            "time_partitioning": None,
        },
        "player_snapshots": {
            "clustering_fields": ["player_tag"],
            "time_partitioning": bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="extracted_at",
            ),
        },
        "rankings": {
            "clustering_fields": ["category"],
            "time_partitioning": bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="extracted_at",
            ),
        },
        "battles": {
            "clustering_fields": ["arena_id", "battle_type"],
            "time_partitioning": bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="battle_time",
            ),
        },
        "battle_decks": {
            "clustering_fields": ["arena_id", "deck_signature"],
            "time_partitioning": bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="battle_time",
            ),
        },
    }
    return configs.get(table_name, {"clustering_fields": None, "time_partitioning": None})


# ---------------------------------------------------------------------------
# Upsert strategy per table
# ---------------------------------------------------------------------------

# Tables that support MERGE (upsert on a primary key)
UPSERT_TABLES = {
    "cards":   "card_id",
    "players": "player_tag",
}

# Tables that are append-only (time-series)
# Deduplication is handled by checking existing IDs before insert
APPEND_TABLES_WITH_DEDUP = {
    "battles":     "battle_id",
    "battle_decks": ("battle_id", "player_tag"),  # composite PK
}

# Pure append, no dedup needed (each run produces a new snapshot)
PURE_APPEND_TABLES = ["player_snapshots", "rankings"]


# ---------------------------------------------------------------------------
# Core Loader class
# ---------------------------------------------------------------------------

class Loader:
    """Loads transformed data into BigQuery for the Meta Clash pipeline.

    Usage:
        loader = Loader(project_id="my-project", dataset_id="clash_royale")
        loader.ensure_tables()          # idempotent — safe to run every time
        loader.run(transformed_tables)  # dict from Transformer.run()
    """

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = f"{project_id}.{dataset_id}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_tables(self) -> None:
        """Create dataset and all tables if they don't exist.

        Idempotent — safe to call at the start of every pipeline run.
        """
        self._ensure_dataset()
        for table_name in SCHEMAS:
            self._ensure_table(table_name)
        logger.info("All tables verified / created in %s", self.dataset_ref)

    def run(self, tables: dict[str, list[dict]]) -> dict[str, int]:
        """Load all transformed tables into BigQuery.

        Args:
            tables: dict from Transformer.run()
                    keys: cards, players, player_snapshots,
                          rankings, battles, battle_decks

        Returns:
            dict[table_name → rows_inserted]
        """
        results = {}

        # Load order matters: reference data first, then facts
        load_order = [
            "cards",
            "players",
            "player_snapshots",
            "rankings",
            "battles",
            "battle_decks",
        ]

        for table_name in load_order:
            rows = tables.get(table_name, [])
            if not rows:
                logger.info("%-20s → skipped (0 rows)", table_name)
                results[table_name] = 0
                continue

            inserted = self._load_table(table_name, rows)
            results[table_name] = inserted
            logger.info("%-20s → %d rows inserted", table_name, inserted)

        total = sum(results.values())
        logger.info("=== Load complete: %d total rows inserted ===", total)
        return results

    # ------------------------------------------------------------------
    # Dataset + table management
    # ------------------------------------------------------------------

    def _ensure_dataset(self) -> None:
        """Create the dataset if it doesn't exist."""
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.debug("Dataset %s already exists", self.dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "EU"
            self.client.create_dataset(dataset)
            logger.info("Created dataset %s", self.dataset_ref)

    def _ensure_table(self, table_name: str) -> None:
        """Create a table with schema, partitioning, and clustering if missing."""
        table_ref = f"{self.dataset_ref}.{table_name}"
        try:
            self.client.get_table(table_ref)
            logger.debug("Table %s already exists", table_ref)
        except NotFound:
            schema = SCHEMAS[table_name]
            config = _make_table_config(table_name)

            table = bigquery.Table(table_ref, schema=schema)

            if config["time_partitioning"]:
                table.time_partitioning = config["time_partitioning"]
            if config["clustering_fields"]:
                table.clustering_fields = config["clustering_fields"]

            self.client.create_table(table)
            logger.info("Created table %s", table_ref)

    # ------------------------------------------------------------------
    # Load routing
    # ------------------------------------------------------------------

    def _load_table(self, table_name: str, rows: list[dict]) -> int:
        """Route to the correct load strategy for this table."""
        if table_name in UPSERT_TABLES:
            return self._upsert(table_name, rows, UPSERT_TABLES[table_name])

        if table_name in APPEND_TABLES_WITH_DEDUP:
            pk = APPEND_TABLES_WITH_DEDUP[table_name]
            return self._append_with_dedup(table_name, rows, pk)

        # Pure append — player_snapshots, rankings
        return self._append(table_name, rows)

    # ------------------------------------------------------------------
    # Upsert via MERGE (cards, players)
    # ------------------------------------------------------------------

    def _upsert(self, table_name: str, rows: list[dict], pk_field: str) -> int:
        """MERGE new rows into an existing table on a primary key.

        Strategy: write rows to a temp table, then MERGE into target.
        This avoids duplicate rows when re-running the pipeline.
        """
        if not rows:
            return 0

        temp_table = f"{self.dataset_ref}.{table_name}_tmp"

        # 1. Write to temp table (full replace)
        self._write_temp_table(temp_table, rows, table_name)

        # 2. MERGE temp → target
        target = f"{self.dataset_ref}.{table_name}"
        schema_fields = [f.name for f in SCHEMAS[table_name] if f.name != pk_field]
        update_clause = ", ".join(
            f"T.{col} = S.{col}" for col in schema_fields
        )
        insert_cols = ", ".join([pk_field] + schema_fields)
        insert_vals = ", ".join(f"S.{col}" for col in [pk_field] + schema_fields)

        merge_sql = f"""
            MERGE `{target}` T
            USING `{temp_table}` S
            ON T.{pk_field} = S.{pk_field}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """

        job = self.client.query(merge_sql)
        job.result()

        # 3. Drop temp table
        self.client.delete_table(temp_table, not_found_ok=True)

        logger.debug("MERGE %s: %d rows processed", table_name, len(rows))
        return len(rows)

    def _write_temp_table(
        self,
        temp_table_id: str,
        rows: list[dict],
        schema_name: str,
    ) -> None:
        """Write rows to a temporary table (WRITE_TRUNCATE)."""
        schema = SCHEMAS[schema_name]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        table = bigquery.Table(temp_table_id, schema=schema)
        try:
            self.client.delete_table(temp_table_id, not_found_ok=True)
            self.client.create_table(table)
        except Exception:
            pass

        errors = self.client.insert_rows_json(temp_table_id, rows)
        if errors:
            raise RuntimeError(
                f"Error writing temp table {temp_table_id}: {errors[:3]}"
            )

    # ------------------------------------------------------------------
    # Append with dedup (battles, battle_decks)
    # ------------------------------------------------------------------

    def _append_with_dedup(
        self,
        table_name: str,
        rows: list[dict],
        pk: str | tuple,
    ) -> int:
        """Insert only rows whose primary key doesn't exist in the table.

        Fetches existing PKs from the last 7 days (partition pruning),
        then filters new rows client-side before inserting.
        """
        table_ref = f"{self.dataset_ref}.{table_name}"
        existing_keys = self._fetch_existing_keys(table_name, pk)

        # Filter new rows
        new_rows = []
        for row in rows:
            key = self._row_key(row, pk)
            if key not in existing_keys:
                new_rows.append(row)

        if not new_rows:
            logger.debug("%s: all %d rows already present", table_name, len(rows))
            return 0

        skipped = len(rows) - len(new_rows)
        if skipped > 0:
            logger.debug(
                "%s: %d rows already existed, inserting %d new",
                table_name, skipped, len(new_rows),
            )

        return self._append(table_name, new_rows)

    def _fetch_existing_keys(self, table_name: str, pk: str | tuple) -> set:
        """Fetch existing primary keys from BigQuery (last 7 days for partitioned tables)."""
        table_ref = f"{self.dataset_ref}.{table_name}"

        # Build SELECT clause
        if isinstance(pk, tuple):
            select_cols = ", ".join(pk)
        else:
            select_cols = pk

        # Restrict to recent partitions to keep query cost low
        partition_filter = ""
        config = _make_table_config(table_name)
        if config["time_partitioning"]:
            partition_col = config["time_partitioning"].field
            partition_filter = f"WHERE {partition_col} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"

        sql = f"SELECT {select_cols} FROM `{table_ref}` {partition_filter}"

        try:
            results = self.client.query(sql).result()
            existing = set()
            for row in results:
                if isinstance(pk, tuple):
                    existing.add(tuple(getattr(row, col) for col in pk))
                else:
                    existing.add(getattr(row, pk))
            logger.debug(
                "%s: fetched %d existing keys", table_name, len(existing)
            )
            return existing
        except Exception as e:
            logger.warning(
                "Could not fetch existing keys for %s: %s — skipping dedup",
                table_name, e,
            )
            return set()

    @staticmethod
    def _row_key(row: dict, pk: str | tuple) -> Any:
        """Extract primary key value(s) from a row dict."""
        if isinstance(pk, tuple):
            return tuple(row.get(col) for col in pk)
        return row.get(pk)

    # ------------------------------------------------------------------
    # Pure append (player_snapshots, rankings + internal use)
    # ------------------------------------------------------------------

    def _append(self, table_name: str, rows: list[dict]) -> int:
        """Insert rows in batches with retry logic.

        Returns the number of rows successfully inserted.
        """
        table_ref = f"{self.dataset_ref}.{table_name}"
        total_inserted = 0

        batches = _chunk(rows, BATCH_SIZE)
        for i, batch in enumerate(batches):
            inserted = self._insert_batch_with_retry(table_ref, batch, i)
            total_inserted += inserted

        return total_inserted

    def _insert_batch_with_retry(
        self,
        table_ref: str,
        batch: list[dict],
        batch_index: int,
    ) -> int:
        """Insert a single batch with exponential backoff retry."""
        for attempt in range(1, MAX_RETRIES + 1):
            errors = self.client.insert_rows_json(table_ref, batch)

            if not errors:
                return len(batch)

            # Log the first few errors for debugging
            logger.warning(
                "Batch %d, attempt %d/%d failed for %s: %s",
                batch_index, attempt, MAX_RETRIES,
                table_ref,
                errors[:2],
            )

            if attempt < MAX_RETRIES:
                wait = RETRY_DELAY * (2 ** (attempt - 1))  # 5s, 10s, 20s
                logger.info("Retrying in %ds...", wait)
                time.sleep(wait)

        # All retries exhausted — log and continue (don't crash the pipeline)
        logger.error(
            "Batch %d permanently failed for %s after %d attempts",
            batch_index, table_ref, MAX_RETRIES,
        )
        return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk(lst: list, size: int):
    """Split a list into chunks of `size`."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]