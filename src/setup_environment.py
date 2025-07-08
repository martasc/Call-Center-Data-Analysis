from datetime import timedelta
import os
import glob
import shutil
from pathlib import Path
import pandas as pd

INPUT_FILE = "../input/junho.csv"
OUTPUT_DIR = "../output"
CLEAN_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "todas.csv") #efetuadas(total - dev ou feedback) + "recebidas" + "nao atendidas"

def clear_output_directory(output_dir=OUTPUT_DIR):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Pasta criada: {output_dir}")
    else:
        for file_path in glob.glob(os.path.join(output_dir, "*")):
            try:
                os.remove(file_path)
            except IsADirectoryError:
                shutil.rmtree(file_path)
        print(f"Pasta limpa: {output_dir}")

def normalize_number(number):
    if pd.isna(number):
        return ""
    return (
        str(number)
        .strip()
        .replace("'", "")
        .replace(" ", "")
        .removeprefix("+351")
        .removeprefix("351")
    )

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

def count_calls_within_one_hour(group):
    group = group.sort_values('Data de Início', ascending=False).reset_index(drop=True)
    group['Total Chamadas'] = pd.NA
    counted_indices = set()

    for idx in group.index:
        if idx in counted_indices:
            continue

        call_type = group.loc[idx, 'Tipo']
        call_time = group.loc[idx, 'Data de Início']

        if call_type == "Chamada recebida":
            group.loc[idx, 'Total Chamadas'] = 1
            counted_indices.add(idx)
            continue

        mask_window = (
            (group['Data de Início'] <= call_time) & 
            (group['Data de Início'] >= call_time - timedelta(hours=1))
        ) & (~group.index.isin(counted_indices)) & (group["Tipo"] != "Chamada recebida")

        one_hour_window = group.loc[mask_window]


        if len(one_hour_window) == 1:
            group.loc[idx, 'Total Chamadas'] = 1
            counted_indices.add(idx)
        elif len(one_hour_window) > 1:
            group.loc[one_hour_window.index[0], 'Total Chamadas'] = len(one_hour_window)
            counted_indices.update(one_hour_window.index)

    return group

def filter_returns(df, output_dir=OUTPUT_DIR):
    """
    Identifica chamadas efetuadas que devolvem chamadas não atendidas
    e exporta para efetuadas.csv de forma limpa e rastreável.
    """
    print(f"\033[1;34m\nA iniciar a identificação de devoluções...\033[0m")

    df = df.copy()
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    df["Origem_norm"] = df["Origem"].apply(normalize_number)
    df["Destino_norm"] = df["Destino"].apply(normalize_number)

    unanswered = df[df["Tipo"] == "Chamada Não Atendida"]
    outgoing = df[df["Tipo"] == "Chamada efetuada"]

    print(f"{len(unanswered)} chamadas não atendidas encontradas")
    #print(f"{len(outgoing)} chamadas efetuadas encontradas")

    returns = []

    for idx, na_call in unanswered.iterrows():
        na_origin = na_call["Origem_norm"]
        na_dest = na_call["Destino_norm"]
        na_time = na_call["Data de Início"]

        #print(f"\n NA idx={idx} | Origem={na_origin} | Destino={na_dest} | Data={na_time}")

        matching_outgoing = outgoing[
            (outgoing["Origem_norm"] == na_dest) &
            (outgoing["Destino_norm"] == na_origin) &
            (outgoing["Data de Início"] > na_time) &
            (outgoing["Data de Início"] <= na_time + timedelta(days=3))
        ].sort_values("Data de Início")

        if not matching_outgoing.empty:
            first_return = matching_outgoing.iloc[0].copy()
            return_time = first_return["Data de Início"]
            tempo_devolucao = (return_time - na_time).total_seconds()

            #print(f" Devolução encontrada em {return_time} ({tempo_devolucao:.0f} s depois)")

            first_return["Data Chamada Não Atendida"] = na_time
            first_return["Tempo até Devolução (s)"] = tempo_devolucao
            returns.append(first_return)
        #else:
            #print(f"Nenhuma devolução encontrada para esta chamada NA.")

    returns_df = pd.DataFrame(returns)

    if not returns_df.empty:
        output_path = os.path.join(output_dir, "devolvidas.csv")
        returns_df = returns_df.sort_values("Data de Início").reset_index(drop=True)
        returns_df.to_csv(output_path, index=False, sep=";")
        #print(f"\nDevoluções exportadas para: {output_path} ({len(returns_df)} registos)")
   # else:
      #  print("\nNenhuma devolução identificada. efetuadas.csv não foi gerado.")

    return returns_df


def process_and_clean_input(input_file=INPUT_FILE, clean_output_file=CLEAN_OUTPUT_FILE):
    if not Path(input_file).exists():
        print(f"❌ Arquivo de input não encontrado: {input_file}")
        return

    try:
        df = pd.read_csv(input_file, delimiter=";", skiprows=2)

        df = remove_unanswered_after_received(df)
        df = df[df["Tipo"].isin(["Chamada recebida", "Chamada Não Atendida", "Chamada efetuada"])]

        df = df.drop_duplicates(subset="Identificador Global da Chamada").reset_index(drop=True)

        # Clean unnecessary columns
        unnecessary_columns = [
            "Utilizador", "Telefone de Origem", "Número de Páginas do Fax", "Tipo de Telefone",
            "Contexto de Acesso da Chamada", "Tipo de localização", "Serviço",
            "Tempo da Fila de Espera", "País", "Identificação de chamada reencaminhada",
            "Percurso no Grupo de Atendimento", "Tipo de Encaminhamento"
        ]
        df = df.drop(columns=[col for col in unnecessary_columns if col in df.columns], errors='ignore')

        df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")

        # Filter test numbers and Paradela 
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
        print(f"Dados limpos exportados para: {clean_output_file}")

        returns_df = filter_returns(df, OUTPUT_DIR)
    
        # Generate não atendidas + recebidas file
        na_recebidas_df = df[df["Tipo"].isin(["Chamada Não Atendida", "Chamada recebida"])]
        na_recebidas_path = os.path.join(OUTPUT_DIR, "recebidas.csv")
        na_recebidas_df.to_csv(na_recebidas_path, index=False, sep=";")
        #print(f"Ficheiro de não atendidas + recebidas exportado para: {na_recebidas_path}")

        # Generate combined (devolvidas + não atendidas + recebidas)
        #returns_path = os.path.join(OUTPUT_DIR, "efetuadas.csv")
        #returns_df.to_csv(returns_path, index=False, sep=";")

        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")

def setup_cleaning_environment():
    print("Inicio da limpeza de dados e filtragem...")
    clear_output_directory()
    process_and_clean_input()
    print("✅ Ambiente de limpeza concluído.")

