"""
transform.py — Clash Royale data transformation module.

Transforms raw data from extract.py into BigQuery-ready tables:
  - players         : player profiles (flat)
  - player_snapshots: point-in-time player state per extraction run
  - battles         : one row per battle with ML features
  - battle_decks    : one row per battle side (team/opponent) with deck + target
  - cards           : card reference catalogue (flat)
  - rankings        : ranking snapshot per extraction run

Input:  dict from Extractor.run() → {"players", "battlelogs", "cards"}
Output: dict of lists of dicts, ready for BigQuery insert_rows_json()
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_battle_time(bt: str) -> str:
    """Convert API battleTime '20260515T164812.000Z' to ISO format."""
    try:
        dt = datetime.strptime(bt, "%Y%m%dT%H%M%S.%fZ")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return bt


def _calc_deck_elixir(cards: list[dict]) -> float:
    """Average elixir cost of a deck (list of card dicts)."""
    costs = [c.get("elixirCost", 0) for c in cards if c.get("elixirCost")]
    return round(sum(costs) / len(costs), 2) if costs else 0.0


def _calc_deck_avg_level(cards: list[dict]) -> float:
    """Average card level in a deck."""
    levels = [c.get("level", 0) for c in cards]
    return round(sum(levels) / len(levels), 2) if levels else 0.0


def _count_evolutions(cards: list[dict]) -> int:
    """Count cards that have an evolution level."""
    return sum(1 for c in cards if c.get("evolutionLevel"))


def _extract_card_ids(cards: list[dict]) -> list[int]:
    """Extract sorted list of card IDs from a deck."""
    return sorted(c.get("id", 0) for c in cards)


def _deck_signature(cards: list[dict]) -> str:
    """Create a canonical deck signature from card IDs for grouping."""
    return "|".join(str(cid) for cid in _extract_card_ids(cards))


def _tower_hp_remaining(side: dict) -> dict:
    """Extract tower HP info from a battle side."""
    princess = side.get("princessTowersHitPoints") or []
    return {
        "king_tower_hp": side.get("kingTowerHitPoints", 0),
        "princess_tower_1_hp": princess[0] if len(princess) > 0 else 0,
        "princess_tower_2_hp": princess[1] if len(princess) > 1 else 0,
    }


# ---------------------------------------------------------------------------
# Table transformers
# ---------------------------------------------------------------------------

def transform_cards(raw_cards: list[dict]) -> list[dict]:
    """Flatten card catalogue into BQ-ready rows."""
    rows = []
    for card in raw_cards:
        rows.append({
            "card_id": card.get("id"),
            "name": card.get("name"),
            "max_level": card.get("maxLevel"),
            "max_evolution_level": card.get("maxEvolutionLevel"),
            "rarity": card.get("rarity"),
            "elixir_cost": card.get("elixirCost"),
            "icon_url": card.get("iconUrls", {}).get("medium"),
        })
    return rows


def transform_players(raw_players: list[dict]) -> list[dict]:
    """Flatten player profiles into BQ-ready rows."""
    rows = []
    for p in raw_players:
        clan = p.get("clan", {})
        arena = p.get("arena", {})
        rows.append({
            "player_tag": p.get("tag"),
            "name": p.get("name"),
            "exp_level": p.get("expLevel"),
            "trophies": p.get("trophies"),
            "best_trophies": p.get("bestTrophies"),
            "wins": p.get("wins"),
            "losses": p.get("losses"),
            "battle_count": p.get("battleCount"),
            "three_crown_wins": p.get("threeCrownWins"),
            "challenge_max_wins": p.get("challengeMaxWins"),
            "challenge_cards_won": p.get("challengeCardsWon"),
            "tournament_cards_won": p.get("tournamentCardsWon"),
            "tournament_battle_count": p.get("tournamentBattleCount"),
            "total_donations": p.get("totalDonations"),
            "war_day_wins": p.get("warDayWins"),
            "clan_tag": clan.get("tag"),
            "clan_name": clan.get("name"),
            "arena_id": arena.get("id"),
            "arena_name": arena.get("name"),
            "category": p.get("_category"),
            "star_points": p.get("starPoints"),
            "total_exp_points": p.get("totalExpPoints"),
        })
    return rows


def transform_player_snapshots(
    raw_players: list[dict],
    extraction_ts: str,
) -> list[dict]:
    """Create point-in-time snapshots of player state."""
    rows = []
    for p in raw_players:
        pol_current = p.get("currentPathOfLegendSeasonResult", {}) or {}
        pol_best = p.get("bestPathOfLegendSeasonResult", {}) or {}
        rows.append({
            "player_tag": p.get("tag"),
            "extracted_at": extraction_ts,
            "trophies": p.get("trophies"),
            "exp_level": p.get("expLevel"),
            "wins": p.get("wins"),
            "losses": p.get("losses"),
            "battle_count": p.get("battleCount"),
            "current_win_lose_streak": p.get("currentWinLoseStreak"),
            "donations": p.get("donations"),
            "donations_received": p.get("donationsReceived"),
            "pol_league_number": pol_current.get("leagueNumber"),
            "pol_trophies": pol_current.get("trophies"),
            "pol_rank": pol_current.get("rank"),
            "pol_best_trophies": pol_best.get("trophies"),
            "category": p.get("_category"),
        })
    return rows


def transform_rankings(
    raw_players: list[dict],
    extraction_ts: str,
) -> list[dict]:
    """Create ranking snapshot rows (position based on extraction order)."""
    rows = []
    # Sort by trophies descending to assign rank
    sorted_players = sorted(
        raw_players,
        key=lambda p: p.get("trophies", 0),
        reverse=True,
    )
    for rank, p in enumerate(sorted_players, start=1):
        rows.append({
            "player_tag": p.get("tag"),
            "extracted_at": extraction_ts,
            "rank": rank,
            "trophies": p.get("trophies"),
            "exp_level": p.get("expLevel"),
            "category": p.get("_category"),
        })
    return rows


def transform_battles(raw_battlelogs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Transform battlelogs into battles + battle_decks tables.

    Returns:
        (battles_rows, battle_decks_rows)
    """
    battles_rows = []
    decks_rows = []
    seen_battle_ids = set()

    for blog in raw_battlelogs:
        crawler_tag = blog.get("player_tag")
        battles = blog.get("battles", [])

        for battle in battles:
            battle_time = _parse_battle_time(battle.get("battleTime", ""))
            battle_type = battle.get("type", "")
            arena = battle.get("arena", {})
            game_mode = battle.get("gameMode", {})

            team_list = battle.get("team", [])
            opp_list = battle.get("opponent", [])

            if not team_list or not opp_list:
                continue

            team = team_list[0]
            opponent = opp_list[0]

            # Build a unique battle ID
            battle_id = (
                f"{team.get('tag')}_{opponent.get('tag')}_{battle_time}"
            )

            # Skip duplicate battles (same battle seen from both sides)
            if battle_id in seen_battle_ids:
                continue
            seen_battle_ids.add(battle_id)
            # Also add reverse ID
            reverse_id = f"{opponent.get('tag')}_{team.get('tag')}_{battle_time}"
            seen_battle_ids.add(reverse_id)

            # Determine winner via trophyChange
            team_trophy_change = team.get("trophyChange", 0)
            team_crowns = team.get("crowns", 0)
            opp_crowns = opponent.get("crowns", 0)

            if team_trophy_change > 0:
                team_is_winner = True
            elif team_trophy_change < 0:
                team_is_winner = False
            else:
                # trophyChange == 0 or missing: use crowns
                team_is_winner = team_crowns > opp_crowns

            # Team deck info
            team_cards = team.get("cards", [])
            opp_cards = opponent.get("cards", [])

            team_towers = _tower_hp_remaining(team)
            opp_towers = _tower_hp_remaining(opponent)

            # --- battles table row ---
            battles_rows.append({
                "battle_id": battle_id,
                "battle_time": battle_time,
                "battle_type": battle_type,
                "arena_id": arena.get("id"),
                "arena_name": arena.get("name"),
                "game_mode_id": game_mode.get("id"),
                "game_mode_name": game_mode.get("name"),
                "is_ladder_tournament": battle.get("isLadderTournament", False),
                "deck_selection": battle.get("deckSelection"),
                "team_tag": team.get("tag"),
                "team_crowns": team_crowns,
                "team_starting_trophies": team.get("startingTrophies"),
                "team_trophy_change": team_trophy_change,
                "opponent_tag": opponent.get("tag"),
                "opponent_crowns": opp_crowns,
                "opponent_starting_trophies": opponent.get("startingTrophies"),
                "opponent_trophy_change": opponent.get("trophyChange", 0),
                "team_king_tower_hp": team_towers["king_tower_hp"],
                "team_princess_tower_1_hp": team_towers["princess_tower_1_hp"],
                "team_princess_tower_2_hp": team_towers["princess_tower_2_hp"],
                "opponent_king_tower_hp": opp_towers["king_tower_hp"],
                "opponent_princess_tower_1_hp": opp_towers["princess_tower_1_hp"],
                "opponent_princess_tower_2_hp": opp_towers["princess_tower_2_hp"],
                "crawler_tag": crawler_tag,
            })

            # --- battle_decks table rows (one per side) ---
            for side, cards, is_winner, crowns, towers, tag in [
                ("team", team_cards, team_is_winner, team_crowns, team_towers, team.get("tag")),
                ("opponent", opp_cards, not team_is_winner, opp_crowns, opp_towers, opponent.get("tag")),
            ]:
                decks_rows.append({
                    "battle_id": battle_id,
                    "battle_time": battle_time,
                    "player_tag": tag,
                    "side": side,
                    "is_winner": is_winner,
                    "crowns": crowns,
                    "deck_card_ids": _extract_card_ids(cards),
                    "deck_signature": _deck_signature(cards),
                    "deck_avg_elixir": _calc_deck_elixir(cards),
                    "deck_avg_level": _calc_deck_avg_level(cards),
                    "deck_num_evolutions": _count_evolutions(cards),
                    "deck_num_cards": len(cards),
                    "king_tower_hp": towers["king_tower_hp"],
                    "princess_tower_1_hp": towers["princess_tower_1_hp"],
                    "princess_tower_2_hp": towers["princess_tower_2_hp"],
                    "battle_type": battle_type,
                    "arena_id": arena.get("id"),
                })

    return battles_rows, decks_rows


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

class Transformer:
    """Transforms raw extracted data into BigQuery-ready tables.

    Usage:
        from src.transform import Transformer

        transformer = Transformer(raw_data)
        tables = transformer.run()
        # tables = {
        #     "cards": [...],
        #     "players": [...],
        #     "player_snapshots": [...],
        #     "rankings": [...],
        #     "battles": [...],
        #     "battle_decks": [...],
        # }
    """

    def __init__(self, raw_data: dict):
        """
        Args:
            raw_data: dict with keys "players", "battlelogs", "cards"
                      as returned by Extractor.run()
        """
        self.raw_players = raw_data.get("players", [])
        self.raw_battlelogs = raw_data.get("battlelogs", [])
        self.raw_cards = raw_data.get("cards", [])
        self.extraction_ts = datetime.now(timezone.utc).isoformat()

    def run(self) -> dict:
        """Execute the full transformation pipeline.

        Returns dict with table names as keys, lists of row-dicts as values.
        """
        logger.info("=== Transformation started ===")

        # 1. Cards
        cards = transform_cards(self.raw_cards)
        logger.info("Cards: %d rows", len(cards))

        # 2. Players
        players = transform_players(self.raw_players)
        logger.info("Players: %d rows", len(players))

        # 3. Player snapshots
        snapshots = transform_player_snapshots(
            self.raw_players, self.extraction_ts
        )
        logger.info("Player snapshots: %d rows", len(snapshots))

        # 4. Rankings
        rankings = transform_rankings(
            self.raw_players, self.extraction_ts
        )
        logger.info("Rankings: %d rows", len(rankings))

        # 5. Battles + battle_decks
        battles, battle_decks = transform_battles(self.raw_battlelogs)
        logger.info("Battles: %d rows", len(battles))
        logger.info("Battle decks: %d rows", len(battle_decks))

        logger.info("=== Transformation complete ===")

        return {
            "cards": cards,
            "players": players,
            "player_snapshots": snapshots,
            "rankings": rankings,
            "battles": battles,
            "battle_decks": battle_decks,
        }