import pandas as pd
import os
from pathlib import Path

def clean_data():
    print("🚀 Iniciando limpeza de dados...")

    # Caminho do arquivo de entrada
    arquivo_csv = "../input/calls.csv"
    
    if not Path(arquivo_csv).exists():
        print(f"❌ Arquivo não encontrado: {arquivo_csv}")
        return

    # Leitura do CSV
    try:
        df = pd.read_csv(arquivo_csv, delimiter=";", skiprows=2)
        print(f"📥 CSV carregado com {len(df)} linhas.")
    except Exception as e:
        print(f"❌ Erro ao ler o CSV: {e}")
        return

    df = df.reset_index(drop=True)

    # Verificação de colunas obrigatórias
    required_columns = ["Serviço", "Tipo de Encaminhamento"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Colunas ausentes: {missing_cols}")

    # Filtro 1: Apenas "chamada voz"
    df = df[df["Serviço"].str.strip().str.lower() == "chamada voz"]
    print(f"📞 Após filtro 'chamada voz': {len(df)} linhas")

    # Filtro 2: "Tipo de Encaminhamento" vazio ou NaN
    df = df[df["Tipo de Encaminhamento"].isna() | (df["Tipo de Encaminhamento"].str.strip() == "")]
    print(f"🧹 Após filtro 'Tipo de Encaminhamento' vazio: {len(df)} linhas")

    # Limpeza das colunas "Origem" e "Destino Final"
    df["Origem"] = df["Origem"].str.strip().str.replace(r"[^0-9]", "", regex=True)
    df["Destino Final"] = df["Destino Final"].str.strip().str.replace(r"[^0-9]", "", regex=True)

    # Remover linhas com '400', '401', ou '4' em "Origem" ou "Destino Final"
    df = df[~df["Origem"].isin(['400', '401', '4'])]
    df = df[~df["Destino Final"].isin(['400', '401', '4'])]

    # Garantir que a Data de Início seja do tipo datetime
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")

    # Ordenar por Data de Início (para garantir que estamos lidando com as chamadas na ordem certa)
    df = df.sort_values(by="Data de Início")

    # Lista para armazenar os índices das linhas a serem removidas
    to_remove_indices = []

    # Iterar sobre as linhas para verificar duplicatas
    for i in range(1, len(df)):
        # Verificar se as duas linhas consecutivas têm a mesma Data de Início
        if df.iloc[i]["Data de Início"] == df.iloc[i - 1]["Data de Início"]:
            tipo_1 = df.iloc[i - 1]["Tipo de Encaminhamento"]
            tipo_2 = df.iloc[i]["Tipo de Encaminhamento"]
            
            # Verificar se uma é "Chamada Efetuada" e a outra é "Chamada Não Atendida"
            if set([tipo_1, tipo_2]) == {"Chamada Efetuada", "Chamada Não Atendida"}:
                # Pegar a linha "Chamada Não Atendida" e "Causa de Não Atendimento"
                if tipo_1 == "Chamada Não Atendida":
                    causa_nao_atendimento = df.iloc[i - 1]["Causa de Não Atendimento"]
                    df.at[i, "Causa de Não Atendimento"] = causa_nao_atendimento
                    to_remove_indices.append(i - 1)  # Marca a linha de "Chamada Não Atendida" para remoção
                else:
                    causa_nao_atendimento = df.iloc[i]["Causa de Não Atendimento"]
                    df.at[i - 1, "Causa de Não Atendimento"] = causa_nao_atendimento
                    to_remove_indices.append(i)  # Marca a linha de "Chamada Não Atendida" para remoção

    # Remover as linhas "Chamada Não Atendida"
    df = df.drop(index=to_remove_indices)

    # Remover colunas desnecessárias
    cols_to_drop = [
        "Fuso Horário", "Número de Páginas do Fax", "Tempo da Fila de Espera",
        "Tipo de Encaminhamento", "Percurso no Grupo de Atendimento",
        "Identificação de chamada reencaminhada"
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors="ignore")

    # Salvar arquivo
    output_dir = "../output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "clean_data.csv")
    
    df.to_csv(output_file, index=False, sep=";")
    print(f"✅ Dados limpos salvos em: {output_file}")
    print(f"📊 Total final de registros: {len(df)}")

if __name__ == "__main__":
    clean_data()
