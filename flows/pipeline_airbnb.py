import os
import re
import json
import logging
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime

# Desativa a telemetria do Prefect
os.environ["PREFECT_UI_TELEMETRY_ENABLED"] = "false"

# 📋 Configuração de logging
log_dir = r".\logs"
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_file = os.path.join(log_dir, f"app_{timestamp}.log")

logger = logging.getLogger("airbnb")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

@task
def clean_json_file(input_path: str, output_path: str) -> str:
    """
    Remove vírgulas finais antes de colchetes ou chaves de fechamento
    e salva o JSON limpo em `output_path`. Retorna o `output_path`.
    """
    if not os.path.exists(input_path):
        logger.error(f"Arquivo não encontrado para limpeza: {input_path}")
        raise FileNotFoundError(f"{input_path} não existe.")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()
        cleaned = re.sub(r',\s*(\]|\})', r'\1', content)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(cleaned)
        logger.info(f"Arquivo JSON limpo salvo em: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Erro ao limpar {input_path}: {e}")
        raise
    
@task(retries=2, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load_json_file(file_path: str):
    if not os.path.exists(file_path):
        logger.error(f"Arquivo não encontrado: {file_path}")
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.info(f"Arquivo carregado com sucesso: {file_path}")
        return data
    except Exception as e:
        logger.error(f"Erro ao carregar {file_path}: {e}")
        raise

@task
def transform_and_join(availability_data, listing_data):
    try:
        availability_df = pd.DataFrame(availability_data['listing_dates'])
        availability_df['airbnb_id'] = int(availability_data['airbnb_id'])
        listing_df = pd.json_normalize(listing_data)

        merged_df = pd.merge(
            availability_df,
            listing_df,
            left_on='airbnb_id',
            right_on='id',
            how='left'
        )
        logger.info("Join realizado com sucesso.")
        return merged_df
    except Exception as e:
        logger.error(f"Erro ao transformar e unir os dados: {e}")
        raise

@task
def save_to_parquet(df, output_path: str):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Arquivo salvo com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo Parquet: {e}")
        raise

@flow(name="Pipeline Airbnb")
def airbnb_pipeline(
    availability_file: str = r".\data\raw\listing_availability_scrape.json",
    listing_raw_file: str = r".\data\raw\listing_scrape.json",
    listing_clean_file: str = r".\data\raw\listing_scrape_cleaned.json",
    output_file: str = r".\data\processed\final_table.parquet"
):
    # 1. Carrega disponibilidade
    availability_data = load_json_file(availability_file)

    # 2. Limpa JSON bruto de listings e salva
    cleaned_path = clean_json_file(listing_raw_file, listing_clean_file)

    # 3. Carrega JSON limpo de listings
    listing_data = load_json_file(cleaned_path)

    # 4. Transformação e join
    merged_df = transform_and_join(availability_data, listing_data)

    # 5. Salva o resultado final
    save_to_parquet(merged_df, output_file)

if __name__ == "__main__":
    airbnb_pipeline()    
