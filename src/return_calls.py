from datetime import timedelta
import pandas as pd
import os
from utils import normalize_number

def filter_returns(df, output_dir):
    df = df.copy()
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    df["Origem_norm"] = df["Origem"].apply(normalize_number)
    df["Destino_norm"] = df["Destino"].apply(normalize_number)

    unanswered = df[df["Tipo"] == "Chamada Não Atendida"]
    outgoing = df[df["Tipo"] == "Chamada efetuada"]

    returns = []

    for idx, na_call in unanswered.iterrows():
        na_origin = na_call["Origem_norm"]
        na_dest = na_call["Destino_norm"]
        na_time = na_call["Data de Início"]

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

            first_return["Data Chamada Não Atendida"] = na_time
            first_return["Tempo até Devolução (s)"] = tempo_devolucao
            returns.append(first_return)

    returns_df = pd.DataFrame(returns)

    if not returns_df.empty:
        output_path = os.path.join(output_dir, "devolvidas.csv")
        returns_df = returns_df.sort_values("Data de Início").reset_index(drop=True)
        returns_df.to_csv(output_path, index=False, sep=";")

    return returns_df
