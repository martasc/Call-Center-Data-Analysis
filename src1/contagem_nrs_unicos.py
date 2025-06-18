import pandas as pd
import os
from datetime import datetime
from pathlib import Path

def carregar_dados(caminho_arquivo):
    """Carrega os dados do arquivo CSV"""
    try:
        df = pd.read_csv(caminho_arquivo, delimiter=';')
        print(df.head(10))  # Para depuração
        return df
    except Exception as e:
        raise ValueError(f"Erro ao carregar arquivo: {str(e)}")

def contar_chamadas_por_identificador(df):
    """Mantém uma linha por Identificador Global da Chamada, com a contagem de ocorrências"""
    df['Identificador Global da Chamada'] = df['Identificador Global da Chamada'].astype(str).str.strip()

    # Agrupa por identificador e conta quantas chamadas existem por identificador
    grupo = df.groupby('Identificador Global da Chamada')

    # Escolhe a primeira linha de cada grupo (você pode mudar para outra agregação se quiser)
    reduzido = grupo.first().copy()

    # Adiciona coluna com o número de chamadas daquele identificador
    reduzido['Total Chamadas com Mesmo Identificador'] = grupo.size()

    return reduzido.reset_index()



def processar_dados(df):
    print(f"📥 CSV carregado com {len(df)} linhas.")

    # Normalize ID column
    df['Identificador Global da Chamada'] = (
        df['Identificador Global da Chamada']
        .astype(str)
        .str.strip()
        .replace('nan', pd.NA)
    )
    df = df.dropna(subset=['Identificador Global da Chamada'])

    # Normalize dates and durations
    df['Data de Início'] = pd.to_datetime(df['Data de Início'], errors='coerce')
    df['Data de Fim'] = pd.to_datetime(df['Data de Fim'], errors='coerce')

    for col in ['Tempo de Toque', 'Duração']:
        if col in df.columns:
            df[col] = pd.to_timedelta(df[col], errors='coerce')

    df['Duração'] = df['Duração'].fillna(pd.Timedelta(seconds=0))

    rows = []
    grouped = df.groupby('Identificador Global da Chamada')

    for id_chamada, grupo in grouped:
        if grupo.empty:
            continue

        grupo = grupo.sort_values('Data de Início')
        base = grupo.iloc[-1].copy()

        # Aggregation logic
        base['Tempo de Toque'] = grupo['Tempo de Toque'].max()

        if (grupo['Tipo'].str.lower() == 'chamada atendida').any():
            base['Duração'] = grupo['Duração'].max()

        base['Duração Total'] = grupo['Duração'].sum()
        base['Tipos Envolvidos'] = ', '.join(grupo['Tipo'].dropna().unique())
        base['Total Etapas da Chamada'] = len(grupo)

        if 'Total Chamadas da Origem' in grupo.columns:
            chamadas_validas = grupo['Total Chamadas da Origem'].replace('', pd.NA).dropna()
            base['Total Chamadas da Origem'] = chamadas_validas.iloc[-1] if not chamadas_validas.empty else ''

        rows.append(base)

    df_resultado = pd.DataFrame(rows)

    # Optional: Preserve original column order
    ordered_cols = [col for col in df.columns if col in df_resultado.columns] + \
                   [col for col in df_resultado.columns if col not in df.columns]
    df_resultado = df_resultado[ordered_cols]

    print(f"✅ {len(df_resultado)} chamadas unificadas.")
    return df_resultado



def exportar_resultados(df, caminho_saida):
    """Exporta os resultados para um novo arquivo CSV"""
    try:
        os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
        df.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"Arquivo salvo em: {caminho_saida}")
    except Exception as e:
        raise ValueError(f"Erro ao exportar resultados: {str(e)}")

def main():
    PROJECT_ROOT = Path(__file__).parent.parent
    input_file = PROJECT_ROOT / 'output' / 'clean_data.csv'
    output_file = input_file

    try:
        print("\n🚀 Iniciando processamento de contagem e unificação de chamadas...")

        print(f"📥 Carregando dados de {input_file}...")
        df = carregar_dados(input_file)

        print("📊 Calculando total de chamadas por origem/hora...")
        df = contar_chamadas_por_identificador(df)

        print("🛠️ Processando unificação de chamadas por ID...")
        df_processado = processar_dados(df)

        print("💾 Salvando resultados...")
        exportar_resultados(df_processado, output_file)

        print("\n📈 Resumo do processamento:")
        print(f"- Total de registros processados: {len(df)}")
        print(f"- Total de origens únicas: {df['Origem'].nunique()}")
        print("- Amostra dos resultados:")
        print(df_processado[['Origem', 'Data de Início', 'Total Chamadas da Origem']].head())

        print("\n✅ Processo concluído com sucesso!")
        return True

    except Exception as e:
        print(f"\n❌ Erro durante o processamento: {str(e)}")
        return False

if __name__ == "__main__":
    main()
