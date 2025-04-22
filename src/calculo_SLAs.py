import pandas as pd
from datetime import timedelta
import os

def processar_dados_chamadas():
    # === Carregar ficheiros ===
    df_clean = pd.read_csv('../output/clean_data.csv', delimiter=';', quotechar="'")
    df_devolvidas = pd.read_csv('../output/chamadas_devolvidas.csv', delimiter=';', quotechar="'")
    
    # Carregar chamadas não devolvidas
    try:
        df_nao_devolvidas = pd.read_csv('../output/chamadas_nao_devolvidas.csv', delimiter=';', quotechar="'")
        nao_devolvidas_existe = not df_nao_devolvidas.empty
    except (FileNotFoundError, pd.errors.EmptyDataError):
        df_nao_devolvidas = pd.DataFrame()
        nao_devolvidas_existe = False

    # === Pre-processamento clean_data ===
    df_clean.columns = df_clean.columns.str.strip()
    df_clean['Data de Início'] = pd.to_datetime(df_clean['Data de Início'], errors='coerce')
    df_clean['Data de Fim'] = pd.to_datetime(df_clean['Data de Fim'], errors='coerce')
    df_clean['Tempo de Toque'] = pd.to_timedelta(df_clean['Tempo de Toque'], errors='coerce')
    df_clean['Duração'] = pd.to_timedelta(df_clean['Duração'], errors='coerce')
    df_clean['Dia da Semana'] = df_clean['Data de Início'].dt.day_name()
    df_clean['Hora'] = df_clean['Data de Início'].dt.hour

    # === Métricas gerais ===
    total_chamadas = len(df_clean)
    nrs_unicos = df_clean['Total Chamadas da Origem'].count()
    chamadas_atendidas = df_clean[df_clean['Tipo'] == 'Chamada recebida']
    chamadas_nao_atendidas = df_clean[df_clean['Tipo'] == 'Chamada Não Atendida']
    media_devolvidas = df_devolvidas['Total Chamadas da Origem'].mean()
    total_chamadas_recebidas = len(chamadas_atendidas) + len(chamadas_nao_atendidas)
    percentagem_atendidas = len(chamadas_atendidas) / total_chamadas_recebidas * 100 if total_chamadas_recebidas else 0

    tempo_medio_espera = df_clean['Tempo de Toque'].mean()
    duracao_media = df_clean['Duração'].mean()
    duracao_formatada = (
        str(timedelta(seconds=int(duracao_media.total_seconds())))
        if pd.notna(duracao_media)
        else "N/A"
    )

    # === Métricas específicas ===
    chamadas_efetuadas = df_clean[df_clean['Tipo'].str.contains("efetuada", case=False, na=False)]
    chamadas_recebidas = df_clean[df_clean['Tipo'].str.contains("recebida", case=False, na=False)]
    primeira_tentativa = len(
        df_clean[
            (df_clean['Total Chamadas da Origem'] == 1) & 
            (df_clean['Tipo'] == 'Chamada recebida')
        ]
    )

    tempo_medio_devolucao = df_devolvidas['Tempo Formatado'].str.replace('min', '').astype(float).mean()

    chamadas_devolvidas = len(df_devolvidas)
    chamadas_nao_devolvidas = len(df_nao_devolvidas) if nao_devolvidas_existe else 0
    chamadas_nao_devolvidas_unicas = df_nao_devolvidas['Origem'].nunique() if nao_devolvidas_existe and 'Origem' in df_nao_devolvidas.columns else 0
    
    # === % de Chamadas devolvidas até 3min e 15min ===
    try:
        df_devolvidas['Tempo Formatado (min)'] = df_devolvidas['Tempo Formatado'].str.replace('min', '').astype(float)
        total_nrs_unicos_devolvidos = df_devolvidas['Origem'].nunique()

        nrs_ate_3min = df_devolvidas[df_devolvidas['Tempo Formatado (min)'] <= 3]['Origem'].nunique()
        nrs_ate_15min = df_devolvidas[df_devolvidas['Tempo Formatado (min)'] <= 15]['Origem'].nunique()
        nrs_ate_30min = df_devolvidas[df_devolvidas['Tempo Formatado (min)'] <= 30]['Origem'].nunique()
        nrs_ate_45min = df_devolvidas[df_devolvidas['Tempo Formatado (min)'] <= 45]['Origem'].nunique()

        perc_ate_3min = (nrs_ate_3min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
        perc_ate_15min = (nrs_ate_15min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
        perc_ate_30min = (nrs_ate_30min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
        perc_ate_45min = (nrs_ate_45min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
    except Exception as e:
        print(f"Erro ao calcular % de chamadas devolvidas por tempo: {e}")
        perc_ate_3min = perc_ate_15min = 0

    # === Chamadas atendidas até 60 segundos ===
    chamadas_recebidas_rapidas = chamadas_atendidas[df_clean['Tempo de Toque'] < pd.Timedelta(seconds=60)]
    total_rapidas = len(chamadas_recebidas_rapidas)
    perc_rapidas = (total_rapidas / len(chamadas_atendidas) * 100) if len(chamadas_atendidas) else 0



    # === Retornar dicionário com resultados ===
    resultados = {
        "Total de chamadas": total_chamadas,
        "Total de chamadas recebidas": total_chamadas_recebidas,
        "Contagem de números únicos": nrs_unicos,
        "Chamadas atendidas": len(chamadas_atendidas),
        "Chamadas não atendidas": len(chamadas_nao_atendidas),
        "Chamadas efetuadas": len(chamadas_efetuadas),
        "Percentagem atendidas": f"{round(percentagem_atendidas, 2)}%",
        "Tempo médio de toque (s)": round(tempo_medio_espera.total_seconds(), 2) if pd.notna(tempo_medio_espera) else None,
        "Duração média": duracao_formatada,
        "MÉTRICAS ESPECÍFICAS": None,
        "Chamadas atentidas à primeira tentativa": primeira_tentativa,
        "Chamadas atendidas com toque < 60s": total_rapidas,
        "% Chamadas atendidas com toque < 60s": f"{round(perc_rapidas, 2)}%",

        "Número de tentativas do nrs não atendidos": media_devolvidas,
        "Tempo médio de devolução": tempo_medio_devolucao,
        "Chamadas devolvidas": chamadas_devolvidas,
        "% De Chamadas devolvidas até 3min (por nrs únicos)": f"{round(perc_ate_3min, 2)}%",
        "% De Chamadas devolvidas até 15min (por nrs únicos)": f"{round(perc_ate_15min, 2)}%",
        "% De Chamadas devolvidas até 30min (por nrs únicos)": f"{round(perc_ate_30min, 2)}%",
        "% De Chamadas devolvidas até 45min (por nrs únicos)": f"{round(perc_ate_45min, 2)}%",

        "Chamadas não devolvidas": chamadas_nao_devolvidas,
        "Chamadas não devolvidas (números únicos)": chamadas_nao_devolvidas_unicas,
        "DataFrame Limpo": df_clean,
        "DataFrame Devolvidas": df_devolvidas,
        "DataFrame Não Devolvidas": df_nao_devolvidas,
    }

    for chave, valor in resultados.items():
        if not isinstance(valor, pd.DataFrame):
            if chave == "Tempo médio de toque (s)":
                print(f"{chave}: {valor} segundos" if valor is not None else f"{chave}: N/A")
            elif chave == "Tempo médio de devolução":
                print(f"{chave}: {valor} minutos" if valor is not None else f"{chave}: N/A")
            else:
                print(f"{chave}: {valor}")
    return resultados
