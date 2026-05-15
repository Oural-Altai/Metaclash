"""
test_extract_integration.py — Test réel de extract.py avec limites basses.

Usage:
    1. Remplace TON_TOKEN_ICI par ton token API Clash Royale
    2. Lance : python test_extract_integration.py
"""

import logging
import sys
import os

# Ajouter le dossier parent au path si besoin
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.api_client import ClashRoyalClient
from src.extract import Extractor, classify_player, PlayerCategory

# ---- Config ----------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("test_integration")

# Remplace par ton vrai token ↓
os.environ["API_TOKEN"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImU5NjE4OGE0LWYzYjktNGM0Zi1hZmI0LTBkZjE2MTBjMzVhMyIsImlhdCI6MTc3ODg2MDYzOSwic3ViIjoiZGV2ZWxvcGVyL2VlOWY3MjA3LTRmNzgtNzE5OC1hYzNhLThkMDk5MGY2MGE5MiIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyIzNy42NS4xNC44NCJdLCJ0eXBlIjoiY2xpZW50In1dfQ.r1LcgvHwE9_fXn9xtTBhMYznO4nEghm3IaYjyzxDUdUIBTcrzk6Ds7Rq7eBmbPQwlia_8yU69kW5frJPZR7UAQ"

os.environ["ENV"] = "local"

# ---- Test ------------------------------------------------------------------

def main():
    client = ClashRoyalClient()

    # Limites basses : 1 seule location, 5 joueurs seed, cap à 10 total
    test_locations = {"global": 57000000}

    extractor = Extractor(
        api_client=client,
        bq_client=None,             # pas de BQ pour le test
        locations=test_locations,
        top_limit=5,                # 5 joueurs du ranking
        max_players=10,             # cap à 10 joueurs total
        snowball_depth=1,
    )

    logger.info("Lancement de l'extraction...")
    data = extractor.run()

    # ---- Résultats ---------------------------------------------------------

    print("\n" + "=" * 60)
    print(f"Joueurs extraits : {len(data['players'])}")
    print(f"Battlelogs       : {len(data['battlelogs'])}")
    print(f"Cartes           : {len(data['cards'])}")
    print("=" * 60)

    # Afficher les joueurs avec leur classification
    for p in data["players"]:
        tag = p.get("tag", "?")
        name = p.get("name", "?")
        lvl = p.get("expLevel", 0)
        trophies = p.get("trophies", 0)
        cat = p.get("_category", "?")
        print(f"  {tag} | {name:20s} | lvl={lvl:2d} | trophies={trophies:5d} | {cat}")

    # Vérifier qu'aucun smurf ne s'est glissé
    smurfs = [p for p in data["players"] if p.get("_category") == PlayerCategory.SMURF]
    if smurfs:
        print(f"\n {len(smurfs)} smurf(s) trouvé(s) — le filtre a un bug !")
    else:
        print("\n Aucun smurf — filtre OK")

    # Overperformers
    overp = [p for p in data["players"] if p.get("_category") == PlayerCategory.OVERPERFORMER]
    if overp:
        print(f"⭐ {len(overp)} overperformer(s) détecté(s)")

    print()


if __name__ == "__main__":
    main()