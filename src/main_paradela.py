import sys
import pandas as pd
from datetime import datetime, timedelta
import os
from datetime import timedelta

import calls_counting


BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
OUTPUT_DIR = os.path.join(BASE_DIR, "output_paradela")
CLEAN_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "todas_paradela.csv")
DEVOLVIDAS_FILE = os.path.join(OUTPUT_DIR, "chamadas_devolvidas.csv")
NAO_DEVOLVIDAS_FILE = os.path.join(OUTPUT_DIR, "chamadas_nao_devolvidas.csv")

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

def parse_datetime(row):
    try:
        date_str = f"{row['Data']} {row['Hora']}"
        return datetime.strptime(date_str, "%d-%m-%Y %H:%M:%S")
    except:
        return pd.NaT

def normalizar_numero(numero):
    if pd.isna(numero):
        return ""
  
    numero_limpo = ''.join(filter(str.isdigit, str(numero)))

    if numero_limpo in ['234246184', '234246187']:
        return '351234246184' if numero_limpo == '234246184' else '351234246187'
    
    if len(numero_limpo) == 9 and numero_limpo.startswith(('91', '92', '93', '96')):
        return '351' + numero_limpo
    return numero_limpo



def identificar_devolvidas(df, output_dir=OUTPUT_DIR, max_minutos=30):
    df = df.copy()
    
    try:
        df['DataHora'] = pd.to_datetime(df['Data de Início'], 
                                      format='%d/%m/%y %H:%M', 
                                      errors='coerce')
    except Exception as e:
        print(f"Erro ao converter datas: {e}")
        return pd.DataFrame(), pd.DataFrame()
    

    unanswered = df[
        (df['Destino_norm'] == '351234246184') & 
        (df['Tipo'].str.strip().str.lower() == 'chamada não atendida')
    ].copy()
    
    outgoing = df[
        (df['Origem_norm'] == '351234246184') & 
        (df['Tipo'].str.strip().str.lower() == 'chamada efetuada')
    ].copy()
    
    devolvidas_list = []
    
    for idx, na_call in unanswered.iterrows():
        na_origin = na_call['Origem_norm']
        na_time = na_call['DataHora']
        
        if pd.isna(na_time):  
            continue
            

        matching_outgoing = outgoing[
            (outgoing['Destino_norm'] == na_origin) &
            (outgoing['DataHora'] > na_time) &
            (outgoing['DataHora'] <= na_time + timedelta(minutes=max_minutos))
        ].sort_values('DataHora')
        
        if not matching_outgoing.empty:
            first_return = matching_outgoing.iloc[0]
            tempo_devolucao = (first_return['DataHora'] - na_time).total_seconds()
            
 
            na_call['Devolvida'] = True
            na_call['Tempo até Devolução (s)'] = tempo_devolucao
            na_call['DataHora Devolução'] = first_return['DataHora']
            na_call['ID Chamada Devolução'] = first_return.name
            
            devolvidas_list.append(na_call)
    
    devolvidas_df = pd.DataFrame(devolvidas_list) if devolvidas_list else pd.DataFrame()
    nao_devolvidas_df = unanswered[~unanswered.index.isin(devolvidas_df.index)] if not unanswered.empty else pd.DataFrame()
    
    if not devolvidas_df.empty:
        devolvidas_df.to_csv(os.path.join(output_dir, 'chamadas_devolvidas.csv'), index=False, sep=';')
    if not nao_devolvidas_df.empty:
        nao_devolvidas_df.to_csv(os.path.join(output_dir, 'chamadas_nao_devolvidas.csv'), index=False, sep=';')
    
    return devolvidas_df, nao_devolvidas_df

def process_and_clean_paradela(input_file, clean_output_file):
    if not os.path.exists(input_file):
        return None
    print(f"📄 A tentar abrir: {input_file}")

    try:
        df = pd.read_csv(input_file, delimiter=";", skiprows=2)

        df["Destino_norm"] = df["Destino"].astype(str).apply(normalizar_numero)
        df["Origem_norm"] = df["Origem"].astype(str).apply(normalizar_numero)

        df = df[
            (df["Destino_norm"] != "351915942292") &
            (df["Origem_norm"] != "351915942292")
        ]

        
        mask = (
            ((df["Destino_norm"] == "351234246184") | (df["Origem_norm"] == "351234246184")) & 
            (~df["Tipo"].str.lower().str.contains('reencaminhada'))
        )
        df = df[mask].copy()
    
        # Guardar CSV filtrado
        if not os.path.exists(os.path.dirname(clean_output_file)):
            os.makedirs(os.path.dirname(clean_output_file))

        df.to_csv(clean_output_file, index=False, sep=";")
        return df

    except Exception as e:
        print(f"❌ Erro ao processar Paradela: {e}")
        return None
    
# def count_calls_within_one_hour(group):
#     # Convert date strings to datetime objects first
#     group['Data de Início'] = pd.to_datetime(group['Data de Início'], format='%d/%m/%y %H:%M')
    
#     group = group.sort_values('Data de Início', ascending=False).reset_index(drop=True)
#     group['Total Chamadas'] = pd.NA
#     counted_indices = set()

#     for idx in group.index:
#         if idx in counted_indices:
#             continue

#         call_type = group.loc[idx, 'Tipo']
#         call_time = group.loc[idx, 'Data de Início']

#         if call_type == "Chamada recebida":
#             group.loc[idx, 'Total Chamadas'] = 1
#             counted_indices.add(idx)
#             continue

#         mask_window = (
#             (group['Data de Início'] <= call_time) & 
#             (group['Data de Início'] >= call_time - timedelta(hours=1))
#         ) & (~group.index.isin(counted_indices)) & (group["Tipo"] != "Chamada recebida")

#         one_hour_window = group.loc[mask_window]

#         if len(one_hour_window) == 1:
#             group.loc[idx, 'Total Chamadas'] = 1
#             counted_indices.add(idx)
#         elif len(one_hour_window) > 1:
#             group.loc[one_hour_window.index[0], 'Total Chamadas'] = len(one_hour_window)
#             counted_indices.update(one_hour_window.index)

#     return group

def calculo_metricas(df, chamadas_devolvidas, chamadas_nao_devolvidas):
    if df is None or df.empty:
        print("DataFrame vazio ou inválido")
        return
    
    if 'Destino_norm' not in df.columns or 'Origem_norm' not in df.columns:
        df["Destino_norm"] = df["Destino"].apply(normalizar_numero)
        df["Origem_norm"] = df["Origem"].apply(normalizar_numero)

    df = df.groupby("Origem", group_keys=False).apply(calls_counting.count_calls_within_one_hour)


    df = df.sort_values('Data de Início', ascending=False).reset_index(drop=True)

    df_filtrado = df[~df["Tipo"].str.strip().str.lower().str.contains('efetuada')].copy()
    

    if 'Tempo de Toque' in df_filtrado.columns:
        df_filtrado["Tempo de Espera (s)"] = df_filtrado["Tempo de Toque"].apply(parse_tempo)
    
    if 'Duração' in df_filtrado.columns:
        df_filtrado["Duraçao (s)"] = df_filtrado["Duração"].apply(parse_tempo)


    df_recebidas = df_filtrado[df_filtrado["Tipo"].str.strip().str.lower() == "chamada recebida"].copy()
    df_nao_recebidas = df_filtrado[df_filtrado["Tipo"].str.strip().str.lower() == "chamada não atendida"].copy()

    total_chamadas = len(df_filtrado)
    total_chamadas_atendidas = len(df_recebidas)
    total_chamadas_nao_atendidas = len(df_nao_recebidas)
    total_chamadas_efetuadas = len(df[df["Tipo"].str.strip().str.lower() == "chamada efetuada"])

    if not df_recebidas.empty and 'Total Chamadas' in df_recebidas.columns:
        nr_medio_tentativas_atendidas = df_recebidas['Total Chamadas'].mean()
        chamadas_primeira_tentativa = (df_recebidas['Total Chamadas'] == 1).sum()
    else:
        nr_medio_tentativas_atendidas = 1.0  
        chamadas_primeira_tentativa = 0

    if not df_nao_recebidas.empty and 'Total Chamadas' in df_nao_recebidas.columns:
        nr_medio_tentativas_nao_atendidas = df_nao_recebidas['Total Chamadas'].mean()
    else:
        nr_medio_tentativas_nao_atendidas = 1.0  


    if not df_recebidas.empty:
        percentagem_atendidas = (total_chamadas_atendidas / total_chamadas * 100) if total_chamadas > 0 else 0
        chamadas_rapidas = (df_recebidas["Tempo de Espera (s)"] < 60).sum()
        perc_rapidas = (chamadas_rapidas / total_chamadas_atendidas * 100) if total_chamadas_atendidas > 0 else 0
        duracao_media = df_recebidas["Duraçao (s)"].mean()
        tempo_medio_espera = df_recebidas["Tempo de Espera (s)"].mean()
    else:
        percentagem_atendidas = 0
        chamadas_rapidas = 0
        perc_rapidas = 0
        duracao_media = 0
        tempo_medio_espera = 0

    print("\n=== Estatísticas Completas ===")
    print(f"\nTotal de chamadas (excluindo efetuadas): {total_chamadas}")
    print(f"Chamadas atendidas: {total_chamadas_atendidas} ({percentagem_atendidas:.1f}%)")
    print(f"Chamadas não atendidas: {total_chamadas_nao_atendidas}")

    print("\n=== Tentativas ===")
    print(f"Número médio de tentativas (atendidas): {nr_medio_tentativas_atendidas:.1f}")
    print(f"  - Chamadas atendidas na 1ª tentativa: {chamadas_primeira_tentativa}")
    print(f"Número médio de tentativas (não atendidas): {nr_medio_tentativas_nao_atendidas:.1f}")

    print("\n=== Tempos ===")
    print(f"Tempo médio de espera (atendidas): {tempo_medio_espera:.1f}s")
    print(f"Duração média (atendidas): {duracao_media:.1f}s")
    print(f"Chamadas atendidas em <60s: {perc_rapidas:.1f}%")

    if chamadas_devolvidas is not None and not chamadas_devolvidas.empty:
        print(f"\nChamadas devolvidas: {len(chamadas_devolvidas)}")
    if chamadas_nao_devolvidas is not None and not chamadas_nao_devolvidas.empty:
        numeros_com_atendimento = set()
        
        chamadas_atendidas = df[
            (df['Tipo'].str.strip().str.lower() == 'chamada recebida') &
            (df['Destino_norm'] == '351234246184')
        ]
        numeros_com_atendimento.update(chamadas_atendidas['Origem_norm'].unique())
        
        verdadeiras_nao_devolvidas = chamadas_nao_devolvidas[
            ~chamadas_nao_devolvidas['Origem_norm'].isin(numeros_com_atendimento)
        ]
        
        print(f"Chamadas não devolvidas (números únicos): {verdadeiras_nao_devolvidas['Origem_norm'].nunique()}")
        return df

def setup_cleaning_environment_paradela():
    print(f"🔍 Diretório atual: {os.getcwd()}")
    print(f"📂 INPUT_FILE definido como: {INPUT_FILE}")
    df = process_and_clean_paradela(INPUT_FILE, CLEAN_OUTPUT_FILE)

    if df is not None:
        chamadas_devolvidas, chamadas_nao_devolvidas = identificar_devolvidas(df)

        df_with_total_chamadas = calculo_metricas(df, chamadas_devolvidas, chamadas_nao_devolvidas)

        unnecessary_columns = [
            "Utilizador", "Telefone de Origem", "Número de Páginas do Fax", "Tipo de Telefone",
            "Contexto de Acesso da Chamada", "Tipo de localização", "Serviço",
            "Tempo da Fila de Espera", "País", "Identificação de chamada reencaminhada", "Identificação Chamada", "Identificador Global da Chamada",
            "Percurso no Grupo de Atendimento", "Tipo de Encaminhamento", 
            "Origem_norm", "Destino_norm", "Destino" 
        ]
        df_with_total_chamadas = df_with_total_chamadas.drop(
            columns=[col for col in unnecessary_columns if col in df_with_total_chamadas.columns],
            errors='ignore'
        )

        df_with_total_chamadas = df_with_total_chamadas.sort_values('Data de Início', ascending=False)
        output_file = os.path.join(OUTPUT_DIR, "todas_paradela.csv")
        df_with_total_chamadas.to_csv(output_file, index=False, sep=';')

        print(f"\nCSV final gerado em: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        INPUT_FILE = os.path.abspath(sys.argv[1])
        print(f"📥 CSV fornecido por argumento: {INPUT_FILE}")
    else:
        INPUT_FILE = os.path.join(BASE_DIR, "input", "CallsSince01Jan.csv")
        print(f"📥 Usando CSV padrão: {INPUT_FILE}")

    OUTPUT_DIR = os.path.join(BASE_DIR, "output_paradela")
    CLEAN_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "todas_paradela.csv")
    DEVOLVIDAS_FILE = os.path.join(OUTPUT_DIR, "chamadas_devolvidas.csv")
    NAO_DEVOLVIDAS_FILE = os.path.join(OUTPUT_DIR, "chamadas_nao_devolvidas.csv")

    print(f"📂 OUTPUT_DIR definido como: {OUTPUT_DIR}")
    setup_cleaning_environment_paradela()
