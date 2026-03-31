import os 
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_tokeen() -> str :
    env=os.getenv("ENV", "local")


    if env =="local" :
        token = os.getenv("CLASH_ROYALE_TOKEN")
    if not token :
        raise EnvironmentError("Clash royal token missing in .env")
    logger.info ("Token loaded from .env")
    return token 


#load token from  .env(local) / CGP secret manager


# env == "CGO"
from google.cloud import secretmanager

projectid=os.getenv("GCP_PROJECT_ID")
secret_name= os.getenv("SECRET_NAME", "TOKEN")

client= secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
response = client.access_secret_version(request={"name":name})
logger.info("Token loaded from secret manager")
return response.payload.data.decode("UTF-8")

