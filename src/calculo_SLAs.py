import pandas as pd
from datetime import timedelta
import os

from chamadas_nao_atendidas import formatar_tempo

def tempo_formatado_para_minutos(tempo_str):
    if pd.isna(tempo_str):
        return None
    tempo_str = str(tempo_str).strip().lower()
    if tempo_str.endswith("h"):
        return float(tempo_str.replace("h", "")) * 60
    elif tempo_str.endswith("min"):
        return float(tempo_str.replace("min", ""))
    elif tempo_str.endswith("s"):
        return float(tempo_str.replace("s", "")) / 60
    return None

def processar_dados_chamadas():

    # === Carregar ficheiros ===
    df_clean = pd.read_csv('../output/clean_data.csv', delimiter=';', quotechar="'")
    try:
        df_devolvidas = pd.read_csv('../output/chamadas_devolvidas.csv', delimiter=';', quotechar="'")
    except FileNotFoundError:
        print("[!] Arquivo chamadas_devolvidas.csv não encontrado.")
        df_devolvidas = pd.DataFrame(columns=['Origem', 'Tempo até Devolução (s)'])

    try:
        df_nao_devolvidas = pd.read_csv('../output/chamadas_nao_devolvidas.csv', delimiter=';', quotechar="'")
        nao_devolvidas_existe = not df_nao_devolvidas.empty
    except (FileNotFoundError, pd.errors.EmptyDataError):
        df_nao_devolvidas = pd.DataFrame()
        nao_devolvidas_existe = False

    # === Pre-processamento ===
    df_clean.columns = df_clean.columns.str.strip()
    df_clean['Data de Início'] = pd.to_datetime(df_clean['Data de Início'], errors='coerce')
    df_clean['Data de Fim'] = pd.to_datetime(df_clean['Data de Fim'], errors='coerce')
    df_clean['Tempo de Toque'] = pd.to_timedelta(df_clean['Tempo de Toque'], errors='coerce')
    df_clean['Duração'] = pd.to_timedelta(df_clean['Duração'], errors='coerce')
    df_clean['Dia da Semana'] = df_clean['Data de Início'].dt.day_name()
    df_clean['Hora'] = df_clean['Data de Início'].dt.hour

    # === Percentagem de devolvidas apenas para números nunca atendidos ===
    df_clean['Tipo'] = df_clean['Tipo'].str.strip()
    df_clean['Origem'] = df_clean['Origem'].astype(str).str.strip()
    df_devolvidas['Origem'] = df_devolvidas['Origem'].astype(str).str.strip()

    # Agrupar por número e verificar quem nunca foi atendido
    agrupado_por_origem = df_clean.groupby('Origem')['Tipo'].unique()
    nrs_nunca_atendidos = agrupado_por_origem[agrupado_por_origem.apply(lambda tipos: all(t == 'Chamada Não Atendida' for t in tipos))].index

    # Interseção com devolvidas
    nrs_nunca_atendidos_set = set(nrs_nunca_atendidos)
    nrs_devolvidos_set = set(df_devolvidas['Origem'])
    nrs_nao_atendidos_e_devolvidos = nrs_nunca_atendidos_set & nrs_devolvidos_set

    # Percentagem correta
    if len(nrs_nunca_atendidos_set) > 0:
        percentagem_devolvidas_sobre_nao_atendidas_corrigida = len(nrs_nao_atendidos_e_devolvidos) / len(nrs_nunca_atendidos_set) * 100
    else:
        percentagem_devolvidas_sobre_nao_atendidas_corrigida = 0


    # === Métricas ===
    chamadas_atendidas = df_clean[df_clean['Tipo'] == 'Chamada recebida']
    chamadas_nao_atendidas = df_clean[df_clean['Tipo'] == 'Chamada Não Atendida']
    chamadas_efetuadas = df_clean[df_clean['Tipo'].str.contains("efetuada", case=False, na=False)]

    total_chamadas_recebidas = len(chamadas_atendidas) + len(chamadas_nao_atendidas)
    nrs_unicos = df_clean['Total Chamadas da Origem'].count() if 'Total Chamadas da Origem' in df_clean.columns else 0
    percentagem_atendidas = len(chamadas_atendidas) / total_chamadas_recebidas * 100 if total_chamadas_recebidas else 0

    chamadas_recebidas_rapidas = chamadas_atendidas[chamadas_atendidas['Tempo de Toque'] <= pd.Timedelta(seconds=60)]
    total_rapidas = len(chamadas_recebidas_rapidas)
    perc_rapidas = total_rapidas / len(chamadas_atendidas) * 100 if len(chamadas_atendidas) else 0

    primeira_tentativa = len(df_clean[(df_clean.get('Total Chamadas da Origem', 0) == 1) & (df_clean['Tipo'] == 'Chamada recebida')]) if 'Total Chamadas da Origem' in df_clean.columns else 0

    # Número médio de tentativas - corrigido
    # if 'Total Chamadas da Origem' in df_clean.columns:
    #     chamadas_nao_atendidas_raw = df_clean[
    #         (df_clean['Tipo'].str.strip().str.lower() == 'Chamada Não Atendida') &
    #         (pd.notna(df_clean['Total Chamadas da Origem']))
    #     ]
 
    #     print(chamadas_nao_atendidas_raw)
    #     media_tentativas_nao_atendidas = chamadas_nao_atendidas_raw['Total Chamadas da Origem'].mean()
        
    #     media_tentativas_atendidas = chamadas_atendidas['Total Chamadas da Origem'].mean()
    # else:
    #     media_tentativas_nao_atendidas = None
    #     media_tentativas_atendidas = None

    # if 'Total Chamadas da Origem' in df_clean.columns and 'Tipo' in df_clean.columns:
    #     chamadas_nao_atendidas_raw = df_clean[
    #         (df_clean['Tipo'].str.strip() == 'Chamada Não Atendida') &
    #         (pd.notna(df_clean['Total Chamadas da Origem']))
    #     ]
    #     if not chamadas_nao_atendidas_raw.empty:
    #         print("hello")
    #         print(chamadas_nao_atendidas_raw)
    #         media_tentativas_nao_atendidas = chamadas_nao_atendidas_raw['Total Chamadas da Origem'].mean()
    #     else:
    #         media_tentativas_nao_atendidas = None
    # else:
    #     media_tentativas_nao_atendidas = None
    if 'Total Chamadas da Origem' in df_clean.columns and 'Tipo' in df_clean.columns:
        chamadas_nao_atendidas_raw = df_clean[
            (df_clean['Tipo'].str.strip() == 'Chamada Não Atendida') &
            (pd.notna(df_clean['Total Chamadas da Origem']))
        ]
        chamadas_atendidas_raw = df_clean[
            (df_clean['Tipo'].str.strip() == 'Chamada recebida') &
            (pd.notna(df_clean['Total Chamadas da Origem']))
        ]

        media_tentativas_nao_atendidas = chamadas_nao_atendidas_raw['Total Chamadas da Origem'].mean() if not chamadas_nao_atendidas_raw.empty else None
        media_tentativas_atendidas = chamadas_atendidas_raw['Total Chamadas da Origem'].mean() if not chamadas_atendidas_raw.empty else None
    else:
        media_tentativas_nao_atendidas = None
        media_tentativas_atendidas = None



    tempo_medio_espera = df_clean['Tempo de Toque'].mean()
    tempo_medio_espera_s = round(tempo_medio_espera.total_seconds(), 2) if pd.notna(tempo_medio_espera) else 0

    duracao_media_atendidas = chamadas_atendidas['Duração'].mean()
    duracao_formatada = str(timedelta(seconds=int(duracao_media_atendidas.total_seconds()))) if pd.notna(duracao_media_atendidas) else "N/A"

    # Chamadas devolvidas - corrigido
    #chamadas_devolvidas = df_devolvidas['Destino'] if 'Origem' in df_devolvidas.columns else 0
    chamadas_devolvidas = len(df_devolvidas) if not df_devolvidas.empty else 0

    if chamadas_devolvidas > 0 and 'Duração' in df_devolvidas.columns:
        df_devolvidas['Duração'] = pd.to_timedelta(df_devolvidas['Duração'], errors='coerce')
        duracao_media_devolvidas = df_devolvidas['Duração'].mean()
        if pd.notna(duracao_media_devolvidas):
            minutos = int(duracao_media_devolvidas.total_seconds() // 60)
            segundos = int(duracao_media_devolvidas.total_seconds() % 60)
            print(f"Duração média das chamadas (devolvidas): {minutos}min e {segundos}s")
    else:
        print("Duração média das chamadas (devolvidas): N/A")

    tempo_medio_devolucao = df_devolvidas['Tempo até Devolução (s)'].mean() if 'Tempo até Devolução (s)' in df_devolvidas.columns else None
    tempo_medio_formatado = formatar_tempo(tempo_medio_devolucao) if tempo_medio_devolucao is not None else None

    if 'Tempo até Devolução (s)' in df_devolvidas.columns:
        df_devolvidas['Tempo Formatado (min)'] = df_devolvidas['Tempo até Devolução (s)'] / 60

    total_nrs_unicos_devolvidos = chamadas_devolvidas
    nrs_ate_3min = df_devolvidas[df_devolvidas['Tempo até Devolução (s)'] <= 180]['Origem'].nunique() if 'Origem' in df_devolvidas.columns and 'Tempo até Devolução (s)' in df_devolvidas.columns else 0
    nrs_ate_15min = df_devolvidas[df_devolvidas['Tempo até Devolução (s)'] <= 900]['Origem'].nunique() if 'Origem' in df_devolvidas.columns and 'Tempo até Devolução (s)' in df_devolvidas.columns else 0

    perc_ate_3min = (nrs_ate_3min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
    perc_ate_15min = (nrs_ate_15min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0

    chamadas_nao_devolvidas = len(df_nao_devolvidas) if nao_devolvidas_existe else 0
    chamadas_nao_devolvidas_unicas = df_nao_devolvidas['Origem'].nunique() if nao_devolvidas_existe and 'Origem' in df_nao_devolvidas.columns else 0

    percentagem_nao_atendidas = 100 - percentagem_atendidas
    if 'Origem' in chamadas_nao_atendidas.columns and chamadas_nao_atendidas['Origem'].nunique() > 0:
        percentagem_devolvidas_sobre_nao_atendidas = 100 * total_nrs_unicos_devolvidos / chamadas_nao_atendidas['Origem'].nunique()
    else:
        percentagem_devolvidas_sobre_nao_atendidas = 0

    print("\nTotal de Chamadas")
    print("------------------")
    print(f"Total de Chamadas Recebidas: {total_chamadas_recebidas}")
    print(f"Total de Chamadas (nrs únicos): {nrs_unicos}")
   
    print("Chamadas Atendidas")
    print("------------------")
    print(f"Total Chamadas Atendidas: {len(chamadas_atendidas)}")
    print(f"% Chamadas Atendidas: {round(percentagem_atendidas, 2)}%")
    print(f"Chamadas com tempo de espera <= 60s: {total_rapidas}")
    print(f"% Chamadas com tempo de espera <= 60s: {round(perc_rapidas, 2)}%")
    print(f"Chamadas atendidas à primeira tentativa: {primeira_tentativa}")
    print(f"Número médio de tentativas (atendidas): {round(media_tentativas_atendidas, 1) if media_tentativas_atendidas is not None else 'N/A'}")
    print(f"Tempo médio de espera (s): {tempo_medio_espera_s}s")
    print(f"Duração média das chamadas(atendidas): {duracao_formatada}\n")

    print("Chamadas Não Atendidas")
    print("------------------")
    print(f"Total de Chamadas não atendida: {len(chamadas_nao_atendidas)}")
    print(f"% não atendidas: {round(percentagem_nao_atendidas, 2)}%\n")
    print(f"Número médio de tentativas (geral): {round(media_tentativas_nao_atendidas, 1) if media_tentativas_nao_atendidas is not None else 'N/A'}\n")


    print("Chamadas Devolvidas")
    print("------------------")
    print(f"Total de Chamadas devolvidas: {chamadas_devolvidas}")
    print(f"% Devolvidas até 3min: {round(perc_ate_3min, 2)}%")
    print(f"% Devolvidas até 15min: {round(perc_ate_15min, 2)}%")
    print(f"% devolvidas sobre chamadas não atendidas (nrs únicos): {round(percentagem_devolvidas_sobre_nao_atendidas, 2)}%")
   # print(f"% devolvidas sobre chamadas não atendidas (nrs únicos - nunca atendidos): {round(percentagem_devolvidas_sobre_nao_atendidas_corrigida, 2)}%")

    if chamadas_devolvidas > 0 and 'Duração' in df_devolvidas.columns and pd.notna(duracao_media_devolvidas):
        minutos = int(duracao_media_devolvidas.total_seconds() // 60)
        segundos = int(duracao_media_devolvidas.total_seconds() % 60)
        print(f"Duração média das chamadas (devolvidas): {minutos}min e {segundos}s")
    else:
        print("Duração média das chamadas (devolvidas): N/A")
    print(f"Tempo médio entre não atendida e devolvida: {tempo_medio_formatado if tempo_medio_formatado is not None else 'N/A'}\n")

    print("Chamadas Não Devolvidas")
    print("------------------")
    print(f"Chamadas não atendidas e não devolvidas (nrs únicos): {chamadas_nao_devolvidas_unicas}")