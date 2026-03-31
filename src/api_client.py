import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_token() -> str:
    """Charge le token depuis .env (local) ou Secret Manager (GCP)."""
    env = os.getenv("ENV", "local")

    if env == "local":
        token = os.getenv("CLASH_ROYALE_TOKEN")
        if not token:
            raise EnvironmentError("CLASH_ROYALE_TOKEN manquant dans .env")
        logger.info("Token chargé depuis .env")
        return token

    # env == "gcp"
    from google.cloud import secretmanager

    project_id = os.getenv("GCP_PROJECT_ID")
    secret_name = os.getenv("SECRET_NAME", "clash-royale-token")

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    logger.info("Token chargé depuis Secret Manager")
    return response.payload.data.decode("UTF-8")

import time
import requests
from typing import Optional

RATE_LIMIT_DELAY = 0.65

class ClashRoyalClient:
    def __init__(self):
        self.token = get_token()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        })

    def _get(self,endpoint: str, params : Optional[dict] =  None) -> dict:
        """Call get with rate limit handling and error"""
        url = f"https://api.clashroyale.com/v1{endpoint}"

        try :
            response = self.session.get(url,params=params,timeout=10)
            time.sleep(RATE_LIMIT_DELAY)

            if response.status_code == 200 :
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"Ressource not found : {endpoint}")
                return {}
            elif response.status_code == 429 :
                logger.warning("Rate limit reached - 30 sec pause")
                time.sleep(30)
                return self._get(endpoint,params)
            else:
                logger.error(f"Error {response.status_code} on {endpoint}")
                response.raise_for_status()
        
        except requests.exceptions.Timeout:
            logger.error(f"Timeout on {endpoint}")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("Connexion mistake - check whitelisted IP")
            raise

    def get_player(self, player_tag:str) -> dict:
        """ID of a player"""
        tag = player_tag.replace("#", "%23")
        return self._get(f"/players/{tag}")
    
    
    def get_battlelog(self,player_tag:str) -> dict:
        """25 last match of a player """
        tag = player_tag.replace("#", "%23")
        data = self._get(f"/players/{tag}/battlelog")
        return data.get("items",[])


    def get_cards(self) -> list:
        "Full card referential"
        data = self._get("/cards")
        return data.get("items",[])
    

    def get_top_players(self,location_id:str = "global", limit : int =100) -> list:
        """Ranking of the best player"""
        if location_id == "global" :
            data = self._get("/locations/global/rankings/players", params= {"limit": limit})
        else :
            data = self._get(f"/locations/{location_id}/rankings/players", params ={"limit": limit})
        return data.get("items", [])





    
