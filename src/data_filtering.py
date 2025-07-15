import os
import pandas as pd
from pathlib import Path
from config import INPUT_FILE, OUTPUT_DIR, CLEAN_OUTPUT_FILE
from calls_counting import count_calls_within_one_hour
from return_calls import filter_returns
from utils import normalize_number


def remove_unanswered_after_received(df):
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


def process_and_clean_input(input_file=INPUT_FILE, clean_output_file=CLEAN_OUTPUT_FILE):
    if not Path(input_file).exists():
        print(f"❌ Input file not found: {input_file}")
        return

    try:
        df = pd.read_csv(input_file, delimiter=";", skiprows=2)
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
        blocked_destinations = ['+351234246184']

        df["Origem"] = df["Origem"].str.strip().str.replace("'", "")
        df["Destino"] = df["Destino"].str.strip().str.replace("'", "")

        df = df[~df["Origem"].isin(blocked_origins)]
        df = df[~df["Destino"].isin(blocked_destinations)]

        df = df.groupby("Origem", group_keys=False).apply(count_calls_within_one_hour)
        df["Total Chamadas"] = pd.to_numeric(df["Total Chamadas"], errors="coerce").astype("Int64")

        df = df.sort_values("Data de Início", ascending=False).reset_index(drop=True)
        df.to_csv(clean_output_file, index=False, sep=";")
       
        filter_returns(df, OUTPUT_DIR)

        na_recebidas_df = df[df["Tipo"].isin(["Chamada Não Atendida", "Chamada recebida"])]
        na_recebidas_path = os.path.join(OUTPUT_DIR, "recebidas.csv")
        na_recebidas_df.to_csv(na_recebidas_path, index=False, sep=";")
        
    except Exception:
        print(f"Error")
