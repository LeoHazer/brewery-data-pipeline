import os 
import requests
import pandas as pd
from datetime import datetime
from loguru import logger

#Configuração dos logs
logger.add("logs/bronze_ingestion.log", rotation="1 day", retention="7 days")

# URL base da APU
API_URL = "https://api.openbrewerydb.org/v1/breweries"

# Pasta Bronze
BRONZE_PATH = "bronze"

def fetch_breweries():
    logger.info("Iniciando a coleta de dados da API OpenBreweryDB...")
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status() # Verifica se a resposta foi bem-sucedida (Erro se status_code != 200)
        data = response.json()
        
        logger.info(f"{len(data)} registros recebidos da API.")
        
        # Nome do arquivo com timestamp
        nome_arq = f"breweries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        caminho_arq = os.path.join(BRONZE_PATH, nome_arq)
        
        # Garantir que a pasta existe
        os.makedirs(BRONZE_PATH, exist_ok=True)
        
        # Salvar os dados crus em JSON
        pd.DataFrame(data).to_json(caminho_arq, orient="records", lines=True)
        
        logger.success(f"Dados salvos com sucesso em {caminho_arq}")
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar API:{e}")
    except Exception as e:
        logger.error(f"Erro desconhecido: {e}")
    


if __name__ == "__main__":
    fetch_breweries()