"""
test_extract_integration.py — Test réel extract + transform avec limites basses.

Usage:
    Lance depuis CR_ETL/ : python test_extract_integration.py
    (le token est lu depuis .env)
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.api_client import ClashRoyalClient
from src.extract import Extractor, PlayerCategory
from src.transform import Transformer

# ---- Config ----------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("test_integration")

# ---- Test ------------------------------------------------------------------

def main():
    client = ClashRoyalClient()

    # ===== EXTRACT ==========================================================
    test_locations = {"FR": 57000076}

    extractor = Extractor(
        api_client=client,
        bq_client=None,
        locations=test_locations,
        top_limit=5,
        max_players=10,
        snowball_depth=1,
    )

    logger.info("Lancement de l'extraction...")
    raw_data = extractor.run()

    print("\n" + "=" * 60)
    print("EXTRACT RESULTS")
    print("=" * 60)
    print(f"  Joueurs extraits : {len(raw_data['players'])}")
    print(f"  Battlelogs       : {len(raw_data['battlelogs'])}")
    print(f"  Cartes           : {len(raw_data['cards'])}")

    # Smurf check
    smurfs = [p for p in raw_data["players"] if p.get("_category") == PlayerCategory.SMURF]
    print(f"  {'⚠️ ' + str(len(smurfs)) + ' smurf(s)' if smurfs else '✅ Aucun smurf'}")

    # ===== TRANSFORM ========================================================
    logger.info("Lancement de la transformation...")
    transformer = Transformer(raw_data)
    tables = transformer.run()

    print("\n" + "=" * 60)
    print("TRANSFORM RESULTS")
    print("=" * 60)
    for table_name, rows in tables.items():
        print(f"  {table_name:20s} : {len(rows)} rows")

    # Sample battle_decks row
    if tables["battle_decks"]:
        print("\n📊 Sample battle_deck row:")
        sample = tables["battle_decks"][0]
        for key, val in sample.items():
            print(f"    {key:30s} : {val}")

    # is_winner distribution
    if tables["battle_decks"]:
        winners = sum(1 for d in tables["battle_decks"] if d["is_winner"])
        total = len(tables["battle_decks"])
        print(f"\n🎯 is_winner distribution: {winners}/{total} "
              f"({winners/total*100:.1f}% winners)")

    # Sample player row
    if tables["players"]:
        print("\n👤 Sample player row:")
        sample = tables["players"][0]
        for key, val in sample.items():
            print(f"    {key:30s} : {val}")

    print()


if __name__ == "__main__":
    main()