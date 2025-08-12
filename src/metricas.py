import os
import re
import pandas as pd
import logging
from pathlib import Path
from config import RECEBIDAS_FILE, DEVOLVIDAS_FILE
import sys

# Setup logging
log_dir = Path(__file__).resolve().parent / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def parse_tempo(tempo_str):
    try:
        parts = list(map(int, tempo_str.strip().split(":")))
        if len(parts) == 3:
            h, m, s = parts
        elif len(parts) == 2:
            h = 0
            m, s = parts
        else:
            return 0
        return h * 3600 + m * 60 + s
    except:
        return 0

def normalizar_numero(numero):
    numero_digits = re.sub(r"\D", "", str(numero))
    return numero_digits[-9:]

def analisar_chamadas(input_file=RECEBIDAS_FILE):
    try:
        df = pd.read_csv(input_file, delimiter=";")
        if os.path.exists(DEVOLVIDAS_FILE):
            df_devolvidas = pd.read_csv(DEVOLVIDAS_FILE, delimiter=";")
        else:
            logger.info(f"Ficheiro {DEVOLVIDAS_FILE} não encontrado. Continuando sem chamadas devolvidas.")
            df_devolvidas = pd.DataFrame()

        if "Tipo" not in df.columns:
            logger.info("Coluna 'Tipo' não encontrada.")
            return

        df_recebidas = df[df["Tipo"].str.strip().str.lower() == "chamada recebida"].copy()
        df_nao_recebidas = df[df["Tipo"].str.strip().str.lower() == "chamada não atendida"].copy()

        if df_recebidas.empty:
            logger.info("⚠️ Nenhuma chamada recebida encontrada.")
            return

        df_recebidas["Tempo de Espera (s)"] = df_recebidas["Tempo de Toque"].apply(parse_tempo)
        df_recebidas["Duração (s)"] = df_recebidas["Duração"].apply(parse_tempo)

        total_chamadas = len(df)
        total_chamadas_atendidas = len(df_recebidas)
        percentagem_atendidas = (total_chamadas_atendidas / total_chamadas * 100) if total_chamadas > 0 else 0
        chamadas_rapidas = (df_recebidas["Tempo de Espera (s)"] < 60).sum()
        chamadas_rapidas_20s = (df_recebidas["Tempo de Espera (s)"] < 20).sum()
        perc_rapidas = (chamadas_rapidas / total_chamadas_atendidas * 100) if total_chamadas_atendidas > 0 else 0
        duracao_media = df_recebidas["Duração (s)"].mean()
        tempo_medio_espera = df_recebidas["Tempo de Espera (s)"].mean()
        total_chamadas_nao_atendidas = len(df_nao_recebidas)
        percentagem_nao_atendidas = (total_chamadas_nao_atendidas / total_chamadas * 100) if total_chamadas > 0 else 0

        atendidas_primeira_tentativa = df_recebidas["Total Chamadas"].value_counts().get(1, 0)
        chamadas_atendidas_nrs_unicos = df_recebidas["Total Chamadas"].count()
        chamadas_nao_atendidas_nrs_unicos = df_nao_recebidas["Total Chamadas"].count()
        nr_medio_tentativas_atendidas = df_recebidas["Total Chamadas"].sum() / chamadas_atendidas_nrs_unicos if chamadas_atendidas_nrs_unicos > 0 else 0
        nr_medio_tentativas_nao_atendidas = df_nao_recebidas["Total Chamadas"].sum() / chamadas_nao_atendidas_nrs_unicos if chamadas_nao_atendidas_nrs_unicos > 0 else 0

        chamadas_devolvidas = len(df_devolvidas)
        perc_ate_3min = (df_devolvidas["Tempo até Devolução (s)"] <= 180).mean() * 100 if not df_devolvidas.empty else 0
        perc_ate_15min = (df_devolvidas["Tempo até Devolução (s)"] <= 900).mean() * 100 if not df_devolvidas.empty else 0

        chamadas_nao_devolvidas_unicas = 0
        if not df_nao_recebidas.empty:
            df_nao_recebidas["Origem_norm"] = df_nao_recebidas["Origem"].astype(str).str.strip()
            if not df_devolvidas.empty:
                df_devolvidas["Destino_norm"] = df_devolvidas["Destino Final"].apply(normalizar_numero)
                df_nao_recebidas["Origem_norm"] = df_nao_recebidas["Origem"].apply(normalizar_numero)

                numeros_nao_atendidos = df_nao_recebidas["Origem_norm"].unique()
                numeros_devolvidos = df_devolvidas["Destino_norm"].unique()

                numeros_nao_devolvidos = set(numeros_nao_atendidos) - set(numeros_devolvidos)
                chamadas_nao_devolvidas_unicas = len(numeros_nao_devolvidos)
            else:
                chamadas_nao_devolvidas_unicas = df_nao_recebidas["Origem_norm"].nunique()

        logger.info("Estatísticas das chamadas recebidas:")
        logger.info(f"- Total de chamadas: {total_chamadas}")
        logger.info(f"- Total de chamadas atendidas: {total_chamadas_atendidas}")
        logger.info(f"- Total de chamadas não atendidas: {total_chamadas_nao_atendidas}")
        logger.info(f"- Chamadas com tempo de espera < 60s: {chamadas_rapidas}")
        logger.info(f"- Chamadas com tempo de espera < 20s: {chamadas_rapidas_20s}")
        logger.info(f"- Tempo médio de espera - atendidas: {tempo_medio_espera} segundos")
        logger.info(f"- Duração média das chamadas: {duracao_media:.1f} segundos\n")

        logger.info(f"Total de Chamadas (nrs únicos): {chamadas_atendidas_nrs_unicos}\n")

        logger.info("Chamadas Atendidas")
        logger.info("------------------")
        logger.info(f"Total Chamadas Atendidas: {total_chamadas_atendidas}")
        logger.info(f"% Chamadas Atendidas: {round(percentagem_atendidas)}%")
        logger.info(f"Chamadas com tempo de espera <= 60s: {chamadas_rapidas}")
        logger.info(f"% Chamadas com tempo de espera <= 60s: {round(perc_rapidas, 2)}%")
        logger.info(f"Chamadas atendidas à primeira tentativa: {atendidas_primeira_tentativa}")
        logger.info(f"Número médio de tentativas (atendidas): {round(nr_medio_tentativas_atendidas, 1)}")
        logger.info(f"Tempo médio de espera (s): {tempo_medio_espera}")
        logger.info(f"Duração média das chamadas (atendidas): {duracao_media}\n")

        logger.info("Chamadas Não Atendidas")
        logger.info("------------------")
        logger.info(f"Total de Chamadas não atendida: {total_chamadas_nao_atendidas}")
        logger.info(f"% não atendidas: {round(percentagem_nao_atendidas)}%")
        logger.info(f"Número médio de tentativas (não atendidas): {round(nr_medio_tentativas_nao_atendidas, 1)}\n")

        logger.info("Chamadas Devolvidas")
        logger.info("------------------")
        logger.info(f"Total de Chamadas devolvidas: {chamadas_devolvidas}")
        logger.info(f"% Devolvidas até 3min: {round(perc_ate_3min, 2)}%")
        logger.info(f"% Devolvidas até 15min: {round(perc_ate_15min, 2)}%\n")

        logger.info("Chamadas Não Devolvidas")
        logger.info("------------------")
        logger.info(f"Chamadas não atendidas e não devolvidas (nrs únicos): {chamadas_nao_devolvidas_unicas}")

    except Exception as e:
        logger.error(f"Erro ao analisar chamadas: {e}")
