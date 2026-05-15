import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def transform_players(raw_players: list[dict]) -> list[dict]:
    """
    Nettoie et structure les profils joueurs bruts.
    Retourne une liste prête pour BigQuery.
    """
    players = []

    for raw in raw_players:
        try:
            player = {
                "player_tag": raw.get("tag", "").replace("#", ""),
                "name": raw.get("name", ""),
                "trophies": raw.get("trophies", 0),
                "best_trophies": raw.get("bestTrophies", 0),
                "arena_id": raw.get("arena", {}).get("id", 0),
                "clan_tag": raw.get("clan", {}).get("tag", "").replace("#", ""),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            players.append(player)
        except Exception as e:
            logger.warning(f"Erreur transform joueur {raw.get('tag')} : {e}")
            continue

    logger.info(f"Joueurs transformés : {len(players)}")
    return players



def _extract_deck(battle_id: str, player_tag: str, cards: list, is_winner: bool) -> dict:
    """Helper — structure un deck de 8 cartes pour BigQuery."""
    if len(cards) != 8:
        return {}

    deck = {
        "deck_id": f"{battle_id}_{player_tag.replace('#', '')}",
        "battle_id": battle_id,
        "player_tag": player_tag.replace("#", ""),
        "is_winner": is_winner,
    }

    for i, card in enumerate(cards, start=1):
        deck[f"card_{i}_id"] = str(card.get("id", ""))

    return deck


def transform_battles(raw_battles: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Transforme les batailles brutes en deux listes :
    - battles : données de la bataille
    - battle_decks : decks utilisés par chaque joueur
    """
    battles = []
    battle_decks = []

    for raw in raw_battles:
        try:
            battle_id = f"{raw.get('player_tag')}_{raw.get('battleTime', '')}"

            team = raw.get("team", [{}])[0]
            opponent = raw.get("opponent", [{}])[0]

            battle = {
                "battle_id": battle_id,
                "player_tag": raw.get("player_tag", "").replace("#", ""),
                "opponent_tag": opponent.get("tag", "").replace("#", ""),
                "battle_time": raw.get("battleTime", ""),
                "arena_id": raw.get("arena", {}).get("id", 0),
                "crowns_won": team.get("crowns", 0),
                "crowns_lost": opponent.get("crowns", 0),
                "win": team.get("crowns", 0) > opponent.get("crowns", 0),
                "elixir_leaked": team.get("elixirLeaked", 0.0),
                "avg_elixir_cost": team.get("elixirCost", 0.0),
            }
            battles.append(battle)

            # Deck du joueur
            team_deck = _extract_deck(
                battle_id=battle_id,
                player_tag=raw.get("player_tag", ""),
                cards=team.get("cards", []),
                is_winner=battle["win"]
            )
            if team_deck:
                battle_decks.append(team_deck)

            # Deck de l'adversaire
            opponent_deck = _extract_deck(
                battle_id=battle_id,
                player_tag=opponent.get("tag", ""),
                cards=opponent.get("cards", []),
                is_winner=not battle["win"]
            )
            if opponent_deck:
                battle_decks.append(opponent_deck)

        except Exception as e:
            logger.warning(f"Erreur transform bataille {raw.get('player_tag')} : {e}")
            continue

    logger.info(f"Battles transformées : {len(battles)}")
    logger.info(f"Battle decks extraits : {len(battle_decks)}")
    return battles, battle_decks

def transform_cards(raw_cards: list[dict]) -> list[dict]:
    """
    Nettoie et structure le référentiel des cartes.
    Retourne une liste prête pour BigQuery.
    """
    cards = []

    for raw in raw_cards:
        try:
            card = {
                "card_id": str(raw.get("id", "")),
                "name": raw.get("name", ""),
                "rarity": raw.get("rarity", ""),
                "type": raw.get("type", ""),
                "elixir_cost": raw.get("elixirCost", 0),
                "max_level": raw.get("maxLevel", 0),
                "icon_url": raw.get("iconUrls", {}).get("medium", ""),
            }
            cards.append(card)
        except Exception as e:
            logger.warning(f"Erreur transform carte {raw.get('id')} : {e}")
            continue

    logger.info(f"Cartes transformées : {len(cards)}")
    return cards