"""
extract.py — Clash Royale data extraction module.

Seed players from top rankings across multiple locations,
then snowball-crawl battlelogs with smart filtering:
  - High level + high arena  → experienced player (keep)
  - Low level + high arena   → overperformer (priority keep)
  - High level + low arena   → smurf (skip)

Deduplication: in-memory set + BigQuery check between runs.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Seed locations: top 5 countries
# Note: global rankings use a separate endpoint not supported by get_top_players
LOCATIONS = {
    "US":      57000249,
    "FR":      57000076,
    "DE":      57000056,
    "UK":      57000248,
    "BR":      57000032,
}

# Arena thresholds (trophy-based, approximations from current seasons)
# Below this trophy count → considered "low arena"
LOW_ARENA_THRESHOLD = 5000
# Above this → "high arena"
HIGH_ARENA_THRESHOLD = 6500

# King level thresholds
LOW_LEVEL_THRESHOLD = 35
HIGH_LEVEL_THRESHOLD = 45

# Crawl limits
DEFAULT_TOP_PLAYERS_LIMIT = 50      # per location
MAX_SNOWBALL_PLAYERS = 2000         # total cap to avoid runaway crawl
SNOWBALL_DEPTH = 1                  # levels of opponent expansion


# ---------------------------------------------------------------------------
# Player classification
# ---------------------------------------------------------------------------

class PlayerCategory:
    EXPERIENCED = "experienced"       # high level + high arena
    OVERPERFORMER = "overperformer"   # low level + high arena
    SMURF = "smurf"                   # high level + low arena
    NORMAL = "normal"                 # everything else


def classify_player(trophies: int, king_level: int) -> str:
    """Classify a player based on arena (trophies) and king level.

    Returns one of PlayerCategory values.
    """
    high_arena = trophies >= HIGH_ARENA_THRESHOLD
    low_arena = trophies < LOW_ARENA_THRESHOLD
    high_level = king_level >= HIGH_LEVEL_THRESHOLD
    low_level = king_level <= LOW_LEVEL_THRESHOLD

    if high_level and high_arena:
        return PlayerCategory.EXPERIENCED
    if low_level and high_arena:
        return PlayerCategory.OVERPERFORMER
    if high_level and low_arena:
        return PlayerCategory.SMURF
    return PlayerCategory.NORMAL


def should_crawl(trophies: int, king_level: int) -> bool:
    """Decide if a player is worth crawling (skip smurfs)."""
    return classify_player(trophies, king_level) != PlayerCategory.SMURF


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class PlayerDeduplicator:
    """Dual deduplication: in-memory set + optional BigQuery check."""

    def __init__(self, bq_client=None, bq_table: str = "players"):
        self._seen: set[str] = set()
        self._bq_client = bq_client
        self._bq_table = bq_table
        self._bq_known: set[str] = set()

        if self._bq_client:
            self._load_known_from_bq()

    def _load_known_from_bq(self) -> None:
        """Pre-load existing player tags from BigQuery."""
        try:
            query = f"SELECT DISTINCT player_tag FROM `{self._bq_table}`"
            results = self._bq_client.query(query).result()
            self._bq_known = {row.player_tag for row in results}
            logger.info(
                "Loaded %d known players from BigQuery", len(self._bq_known)
            )
        except Exception:
            logger.warning("Could not load players from BigQuery, skipping BQ dedup")
            self._bq_known = set()

    def is_new(self, player_tag: str) -> bool:
        """Check if player_tag has not been seen in this run or in BQ."""
        if player_tag in self._seen:
            return False
        if player_tag in self._bq_known:
            return False
        return True

    def mark_seen(self, player_tag: str) -> None:
        """Mark a player_tag as processed."""
        self._seen.add(player_tag)

    @property
    def seen_count(self) -> int:
        return len(self._seen)


# ---------------------------------------------------------------------------
# Extraction logic
# ---------------------------------------------------------------------------

class Extractor:
    """Orchestrates data extraction from the Clash Royale API.

    Usage:
        from src.api_client import ClashRoyalClient

        client = ClashRoyalClient()
        extractor = Extractor(client)
        data = extractor.run()
        # data = {
        #     "players": [...],
        #     "battlelogs": [...],
        #     "cards": [...],
        # }
    """

    def __init__(
        self,
        api_client,
        bq_client=None,
        locations: Optional[dict] = None,
        top_limit: int = DEFAULT_TOP_PLAYERS_LIMIT,
        max_players: int = MAX_SNOWBALL_PLAYERS,
        snowball_depth: int = SNOWBALL_DEPTH,
    ):
        self.api = api_client
        self.locations = locations or LOCATIONS
        self.top_limit = top_limit
        self.max_players = max_players
        self.snowball_depth = snowball_depth
        self.dedup = PlayerDeduplicator(bq_client=bq_client)

        # Raw extracted data
        self._players: list[dict] = []
        self._battlelogs: list[dict] = []
        self._cards: list[dict] = []

    # ----- public -----------------------------------------------------------

    def run(self) -> dict:
        """Execute the full extraction pipeline.

        Returns dict with keys: players, battlelogs, cards.
        """
        logger.info("=== Extraction started ===")

        # 1. Reference data
        self._extract_cards()

        # 2. Seed from rankings
        seed_tags = self._seed_from_rankings()
        logger.info("Seed: %d unique player tags from rankings", len(seed_tags))

        # 3. Crawl seed players + snowball
        self._crawl_players(seed_tags, depth=0)

        logger.info(
            "=== Extraction complete: %d players, %d battlelogs, %d cards ===",
            len(self._players),
            len(self._battlelogs),
            len(self._cards),
        )

        return {
            "players": self._players,
            "battlelogs": self._battlelogs,
            "cards": self._cards,
        }

    # ----- seed -------------------------------------------------------------

    def _seed_from_rankings(self) -> list[str]:
        """Fetch top players from each location and return their tags."""
        tags = []

        for name, location_id in self.locations.items():
            logger.info("Fetching top %d from %s", self.top_limit, name)
            ranking = self.api.get_top_players(
                location_id=location_id, limit=self.top_limit
            )

            if not ranking:
                logger.warning("No ranking data for %s", name)
                continue

            items = ranking.get("items", [])
            for player in items:
                tag = player.get("tag")
                if tag and self.dedup.is_new(tag):
                    tags.append(tag)

        return tags

    # ----- crawl ------------------------------------------------------------

    def _crawl_players(self, player_tags: list[str], depth: int) -> None:
        """Crawl a list of players: fetch profile + battlelog.

        Recursively snowball opponents up to self.snowball_depth.
        """
        opponent_tags: list[str] = []

        for tag in player_tags:
            if self.dedup.seen_count >= self.max_players:
                logger.info("Reached max player cap (%d), stopping", self.max_players)
                return

            if not self.dedup.is_new(tag):
                continue

            # Fetch profile
            profile = self.api.get_player(tag)
            if not profile:
                self.dedup.mark_seen(tag)
                continue

            trophies = profile.get("trophies", 0)
            king_level = profile.get("expLevel", 0)

            # Smurf filter
            if not should_crawl(trophies, king_level):
                category = classify_player(trophies, king_level)
                logger.debug(
                    "Skipping %s — classified as %s (lvl=%d, trophies=%d)",
                    tag, category, king_level, trophies,
                )
                self.dedup.mark_seen(tag)
                continue

            # Keep this player
            category = classify_player(trophies, king_level)
            profile["_category"] = category
            self._players.append(profile)
            self.dedup.mark_seen(tag)

            logger.debug(
                "Crawled %s [%s] lvl=%d trophies=%d",
                tag, category, king_level, trophies,
            )

            # Fetch battlelog
            battlelog = self.api.get_battlelog(tag)
            if battlelog:
                battles = battlelog if isinstance(battlelog, list) else battlelog.get("items", battlelog)
                self._battlelogs.append({
                    "player_tag": tag,
                    "battles": battles,
                })

                # Collect opponent tags for snowball
                if depth < self.snowball_depth:
                    opponents = self._extract_opponents(battles, tag)
                    opponent_tags.extend(opponents)

        # Snowball: crawl opponents at next depth
        if depth < self.snowball_depth and opponent_tags:
            # Deduplicate the opponent list before recursion
            unique_opponents = list(dict.fromkeys(opponent_tags))
            logger.info(
                "Snowball depth %d → %d new opponents to crawl",
                depth + 1, len(unique_opponents),
            )
            self._crawl_players(unique_opponents, depth=depth + 1)

    # ----- opponents --------------------------------------------------------

    def _extract_opponents(self, battles: list[dict], player_tag: str) -> list[str]:
        """Extract opponent tags from a list of battles."""
        opponents = []

        for battle in battles:
            # The API nests opponents under "opponent" as a list
            opponent_list = battle.get("opponent", [])
            for opp in opponent_list:
                opp_tag = opp.get("tag")
                if opp_tag and opp_tag != player_tag and self.dedup.is_new(opp_tag):
                    opponents.append(opp_tag)

        return opponents

    # ----- cards ------------------------------------------------------------

    def _extract_cards(self) -> None:
        """Fetch the full card reference catalogue."""
        logger.info("Fetching card catalogue")
        cards = self.api.get_cards()

        if cards:
            if isinstance(cards, list):
                self._cards = cards
            else:
                items = cards.get("items", cards)
                self._cards = items if isinstance(items, list) else [items]
            logger.info("Got %d cards", len(self._cards))
        else:
            logger.warning("Could not fetch cards catalogue")