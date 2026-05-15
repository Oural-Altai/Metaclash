import logging
import os
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID", "clash_royale")


def get_client() -> bigquery.Client:
    """Initialise le client BigQuery."""
    return bigquery.Client(project=PROJECT_ID)


def load_table(client: bigquery.Client, rows: list[dict], table_id: str) -> None:
    """
    Charge une liste de rows dans une table BigQuery.
    Utilise insert_rows_json pour du streaming insert.
    """
    if not rows:
        logger.warning(f"Aucune donnée à charger pour {table_id}")
        return

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"

    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        logger.error(f"Erreurs lors du chargement dans {table_id} : {errors}")
        raise RuntimeError(f"BigQuery insert errors : {errors}")

    logger.info(f"{len(rows)} lignes chargées dans {table_id}")


def load_all(players: list, battles: list, battle_decks: list, cards: list) -> None:
    """
    Point d'entrée principal — charge toutes les tables.
    """
    client = get_client()

    load_table(client, players, "players")
    load_table(client, battles, "battles")
    load_table(client, battle_decks, "battle_decks")
    load_table(client, cards, "cards")