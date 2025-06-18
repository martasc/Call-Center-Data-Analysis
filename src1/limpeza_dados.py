import pandas as pd
import os
from pathlib import Path

def formatar_numero(numero):
    if pd.isna(numero):
        return numero
    numero = str(numero).strip().replace(" ", "").replace("+", "").replace("*", "").replace("'", "").replace('"', '')
    if numero.startswith("9") and len(numero) == 9:
        return f"+351{numero}"
    elif numero.startswith("351") and len(numero) >= 11:
        return f"+{numero}"
    elif len(numero) < 9:
        return f"{numero}***"
    return f"+{numero}" if numero.isdigit() else numero

def clean_data(data_inicio=None, data_fim=None):
    print("🚀 Iniciando limpeza de dados...")

    arquivo_csv = "../output/clean_data.csv"
    if not Path(arquivo_csv).exists():
        print(f"❌ Arquivo não encontrado: {arquivo_csv}")
        return

    try:
        df = pd.read_csv(arquivo_csv, delimiter=";")
        print(f"📥 CSV carregado com {len(df)} linhas.")
        print("📋 Colunas encontradas:", df.columns.tolist())

    except Exception as e:
        print(f"❌ Erro ao ler o CSV: {e}")
        return

    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    initial_count = len(df)
    df = df.dropna(subset=["Data de Início"])
    if len(df) < initial_count:
        print(f"⏰ Removidas {initial_count - len(df)} linhas com datas inválidas")

    if data_inicio is not None:
        data_inicio = pd.to_datetime(data_inicio)
        df = df[df["Data de Início"] >= data_inicio]
    if data_fim is not None:
        data_fim = pd.to_datetime(data_fim)
        df = df[df["Data de Início"] <= data_fim]

    df = df.reset_index(drop=True)

    required_columns = ["Serviço", "Tipo de Encaminhamento"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Colunas ausentes: {missing_cols}")

    df = df[df["Serviço"].str.strip().str.lower() == "chamada voz"]
    print(f"📞 Após filtro 'chamada voz': {len(df)} linhas")

    #df = df[df["Tipo de Encaminhamento"].isna() | (df["Tipo de Encaminhamento"].str.strip() == "")]
    #df = df.reset_index(drop=True)
    #print(f"🧹 Após filtro 'Tipo de Encaminhamento' vazio: {len(df)} linhas")

    df["Origem"] = df["Origem"].astype(str).str.strip().str.replace(r"[^0-9]", "", regex=True)
    df["Destino Final"] = df["Destino Final"].astype(str).str.strip().str.replace(r"[^0-9]", "", regex=True)
    print(df["Destino Final"].head(10))

    destinos_desejados = ["962878547", "962878568"]
    df = df[df["Destino Final"].isin(destinos_desejados)]
    print(f"📌 Após filtro por 'Destino Final' desejado: {len(df)} linhas")

    df = df[~df["Origem"].astype(str).str.startswith('4')]
    df = df[~df["Destino Final"].astype(str).str.startswith('4')]
    df = df[~df["Origem"].astype(str).str.startswith("35193599")]
    df = df[~df["Origem"].astype(str).str.startswith("35191244")]

    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    df = df.dropna(subset=["Data de Início"])
    df = df.sort_values(by="Data de Início")
    print(f"🔢 Dados ordenados por data. Primeira data: {df['Data de Início'].iloc[0]}, Última data: {df['Data de Início'].iloc[-1]}")

    to_remove_indices = []
    duplicate_pairs_found = 0

    for i in range(1, len(df)):
        current_time = df.iloc[i]["Data de Início"]
        previous_time = df.iloc[i-1]["Data de Início"]
        if current_time == previous_time:
            tipo_anterior = str(df.iloc[i-1]["Tipo"]).strip()
            tipo_atual = str(df.iloc[i]["Tipo"]).strip()
            if (("Chamada efetuada" in tipo_anterior and "Chamada Não Atendida" in tipo_atual) or
                ("Chamada efetuada" in tipo_atual and "Chamada Não Atendida" in tipo_anterior)):
                duplicate_pairs_found += 1
                if "Chamada Não Atendida" in tipo_anterior:
                    causa = df.iloc[i-1]["Causa de Não Atendimento"]
                    df.at[df.index[i], "Causa de Não Atendimento"] = causa
                    to_remove_indices.append(df.index[i-1])
                else:
                    causa = df.iloc[i]["Causa de Não Atendimento"]
                    df.at[df.index[i-1], "Causa de Não Atendimento"] = causa
                    to_remove_indices.append(df.index[i])

    if to_remove_indices:
        print(f"\n🗑️ Removendo {len(to_remove_indices)} linhas de 'Chamada Não Atendida'")
        df = df.drop(index=to_remove_indices)
    else:
        print("\nℹ️ Nenhuma duplicata para remover")

    print("\n🔍 Após o processamento de duplicatas:")
    print(df[["Data de Início", "Tipo de Encaminhamento", "Causa de Não Atendimento"]].head())

    cols_to_drop = [
        "Fuso Horário", "Número de Páginas do Fax", "Tempo da Fila de Espera",
        "Tipo de Encaminhamento", "Percurso no Grupo de Atendimento",
        "Identificação de chamada reencaminhada", "Contexto de Acesso da Chamada", "Tipo de Telefone", "Tipo de localização", "Utilizador", "País", "Identificador Global da Chamada", "Serviço"
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors="ignore")

    colunas_para_formatar = ['Origem', 'Destino', 'Destino Final']
    for col in colunas_para_formatar:
        if col in df.columns:
            df[col] = df[col].apply(formatar_numero)
        else:
            print(f"⚠️ Coluna '{col}' não encontrada no DataFrame.")

    print(df[['Origem', 'Destino', 'Destino Final']].head(10))

    df = df.sort_values(by="Data de Início", ascending=False).reset_index(drop=True)

    output_dir = "../output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "clean_data.csv")
    df.to_csv(output_file, index=False, sep=";")
    print(f"\n✅ Dados limpos salvos em: {output_file}")
    print(f"📊 Total final de registros: {len(df)}")

if __name__ == "__main__":
    clean_data()
