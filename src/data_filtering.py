import os
import pandas as pd
from pathlib import Path
from config import OUTPUT_DIR, CLEAN_OUTPUT_FILE, RECEBIDAS_FILE
from calls_counting import count_calls_within_one_hour
from return_calls import filter_returns
from utils import normalize_number

def remove_unanswered_after_received(df):
    """Remove unanswered calls that were preceded by outgoing calls to same number"""
    df = df.copy()
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    df["Origem_norm"] = df["Origem"].apply(normalize_number)
    df["Destino_norm"] = df["Destino"].apply(normalize_number)

    unanswered = df[df["Tipo"] == "Chamada Não Atendida"]
    outgoing = df[df["Tipo"] == "Chamada efetuada"]

    indices_to_remove = set()

    for idx, na_call in unanswered.iterrows():
        na_origin = na_call["Origem_norm"]
        na_time = na_call["Data de Início"]

        previous_outgoing = outgoing[
            (outgoing["Destino_norm"] == na_origin) &
            (outgoing["Data de Início"] < na_time)
        ]

        if not previous_outgoing.empty:
            indices_to_remove.add(idx)

    return df.drop(index=indices_to_remove).reset_index(drop=True)

def process_and_clean_input(input_file_path):
    """
    Process input CSV file and generate cleaned outputs
    Args:
        input_file_path (str/Path): Path to the input CSV file
    """
    try:
        print(f"🚀 Iniciando processamento de: {input_file_path}")

        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        
        df = pd.read_csv(input_file_path, delimiter=";", skiprows=2)
        df = remove_unanswered_after_received(df)
        
        df = df[df["Tipo"].isin(["Chamada recebida", "Chamada Não Atendida", "Chamada efetuada"])]
        df = df.drop_duplicates(subset="Identificador Global da Chamada").reset_index(drop=True)

        unnecessary_columns = [
            "Utilizador", "Telefone de Origem", "Número de Páginas do Fax", "Tipo de Telefone",
            "Contexto de Acesso da Chamada", "Tipo de localização", "Serviço",
            "Tempo da Fila de Espera", "País", "Identificação de chamada reencaminhada",
            "Percurso no Grupo de Atendimento", "Tipo de Encaminhamento"
        ]
        df = df.drop(columns=[col for col in unnecessary_columns if col in df.columns], errors='ignore')
        
        df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
        
        blocked_origins = ['Anónimo', '+351938116613', '+351915942292', '+351935991897']
        blocked_destinations = ['+351234246184', '90', '80', '401', '+351234246186', '+351234246185', '+351234246184' ]
        df["Origem"] = df["Origem"].str.strip().str.replace("'", "")
        df["Destino"] = df["Destino"].str.strip().str.replace("'", "")
        df = df[~df["Origem"].isin(blocked_origins)]
        df = df[~df["Destino"].isin(blocked_destinations)]

        # Count calls and save outputs
        df = df.groupby("Origem", group_keys=False).apply(count_calls_within_one_hour)
        df["Total Chamadas"] = pd.to_numeric(df["Total Chamadas"], errors="coerce").astype("Int64")
        df = df.sort_values("Data de Início", ascending=False).reset_index(drop=True)
        
        # Save cleaned data
        df.to_csv(CLEAN_OUTPUT_FILE, index=False, sep=";")
       
        # Process returned calls
        filter_returns(df, OUTPUT_DIR)

        # Save received/missed calls
        na_recebidas_df = df[df["Tipo"].isin(["Chamada Não Atendida", "Chamada recebida"])]
        na_recebidas_path = "recebidas.csv"
        na_recebidas_df.to_csv(RECEBIDAS_FILE, index=False, sep=";")

        print(f"📁 Guardado em: {RECEBIDAS_FILE}")
        print(f"📁 Ficheiros existentes no output: {list(Path(OUTPUT_DIR).glob('*'))}")

        return True
        
    except Exception as e:
        print(f"❌ Error processing file {input_file_path}: {str(e)}")
        return False