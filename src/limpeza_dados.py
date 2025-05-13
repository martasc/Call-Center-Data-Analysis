import pandas as pd
import os
from pathlib import Path
import os
import glob

def remove_output_files():
    output_folder = "../output"
    files = glob.glob(os.path.join(output_folder, "*"))

    for f in files:
        try:
            os.remove(f)
        except IsADirectoryError:
            import shutil
            shutil.rmtree(f)

def formatar_numero(numero):
    if pd.isna(numero):
        return numero

    numero = str(numero).strip().replace(" ", "").replace("+", "").replace("*", "").replace("'", "").replace('"', '')

    # Adiciona +351 se for número nacional com 9 dígitos
    if numero.startswith("9") and len(numero) == 9:
        return f"+351{numero}"

    # Adiciona + se já tiver o DDI completo
    elif numero.startswith("351") and len(numero) >= 11:
        return f"+{numero}"

    # Se for truncado (menos de 9 dígitos), adiciona ***
    elif len(numero) < 9:
        return f"{numero}***"

    # Caso contrário, devolve como está (com prefixo + se aplicável)
    return f"+{numero}" if numero.isdigit() else numero

def clean_data(data_inicio=None, data_fim=None):
    print("🚀 Iniciando limpeza de dados...")

    remove_output_files()

    # Caminho do arquivo de entrada
    arquivo_csv = "../input/maio01_13.csv"
    
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

    # Converter a coluna de data para datetime ANTES de filtrar
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    
    # Remover linhas com datas inválidas
    initial_count = len(df)
    df = df.dropna(subset=["Data de Início"])
    if len(df) < initial_count:
        print(f"⏰ Removidas {initial_count - len(df)} linhas com datas inválidas")

    # Aplicar filtros temporais se fornecidos
    if data_inicio is not None:
        data_inicio = pd.to_datetime(data_inicio)
        df = df[df["Data de Início"] >= data_inicio]
    if data_fim is not None:
        data_fim = pd.to_datetime(data_fim)
        df = df[df["Data de Início"] <= data_fim]

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
    # ⚠️ Resetar índices para garantir que iloc e at funcionem corretamente
    df = df.reset_index(drop=True)

    print(f"🧹 Após filtro 'Tipo de Encaminhamento' vazio: {len(df)} linhas")

    # Limpeza das colunas "Origem" e "Destino Final"
    df["Origem"] = df["Origem"].str.strip().str.replace(r"[^0-9]", "", regex=True)
    df["Destino Final"] = df["Destino Final"].str.strip().str.replace(r"[^0-9]", "", regex=True)

    # Remover linhas com '400', '401', ou '4' em "Origem" ou "Destino Final"
    df = df[~df["Origem"].astype(str).str.startswith('4')]
    df = df[~df["Destino Final"].astype(str).str.startswith('4')]

    # Remover linhas das chamadas Teste 
    df = df[~df["Origem"].astype(str).str.startswith("35193599")]
    df = df[~df["Origem"].astype(str).str.startswith("35191244")]

    # Garantir que a Data de Início é do tipo datetime
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")
    
    # Remover linhas com datas inválidas
    initial_count = len(df)
    df = df.dropna(subset=["Data de Início"])
    if len(df) < initial_count:
        print(f"⏰ Removidas {initial_count - len(df)} linhas com datas inválidas")

    # Ordenar por Data de Início
    df = df.sort_values(by="Data de Início")
    print(f"🔢 Dados ordenados por data. Primeira data: {df['Data de Início'].iloc[0]}, Última data: {df['Data de Início'].iloc[-1]}")

    # Lista para armazenar os índices das linhas a serem removidas
    to_remove_indices = []
    duplicate_pairs_found = 0

    # DEBUG: Mostrar primeiras linhas antes do processamento
    print("\n🔍 Antes do processamento de duplicatas:")
    print(df[["Data de Início", "Tipo de Encaminhamento", "Causa de Não Atendimento"]].head())

    # Iterar sobre as linhas para verificar duplicadas
    for i in range(1, len(df)):
        current_time = df.iloc[i]["Data de Início"]
        previous_time = df.iloc[i-1]["Data de Início"]
        
        if current_time == previous_time:
            tipo_anterior = str(df.iloc[i-1]["Tipo"]).strip()
            tipo_atual = str(df.iloc[i]["Tipo"]).strip()
            
            print(f"\n🔍 Par encontrado (linhas {i-1} e {i}):")
            print(f"   Data: {current_time}")
            print(f"   Tipo anterior: {tipo_anterior}")
            print(f"   Tipo atual: {tipo_atual}")
            
            if (("Chamada efetuada" in tipo_anterior and "Chamada Não Atendida" in tipo_atual) or
                ("Chamada efetuada" in tipo_atual and "Chamada Não Atendida" in tipo_anterior)):

                duplicate_pairs_found += 1
                print(f"   ✅ Par válido encontrado! (Total: {duplicate_pairs_found})")
                
                # Copiando a causa corretamente entre os índices reais
                if "Chamada Não Atendida" in tipo_anterior:
                    causa = df.iloc[i-1]["Causa de Não Atendimento"]
                    print(f"   ↪️ Transferindo causa '{causa}' da linha {i-1} para linha {i}")
                    df.at[df.index[i], "Causa de Não Atendimento"] = causa
                    to_remove_indices.append(df.index[i-1])
                else:
                    causa = df.iloc[i]["Causa de Não Atendimento"]
                    print(f"   ↪️ Transferindo causa '{causa}' da linha {i} para linha {i-1}")
                    df.at[df.index[i-1], "Causa de Não Atendimento"] = causa
                    to_remove_indices.append(df.index[i])

            

    # Exibir o número total de pares encontrados
    # Remover as linhas "Chamada Não Atendida"
    if to_remove_indices:
        print(f"\n🗑️ Removendo {len(to_remove_indices)} linhas de 'Chamada Não Atendida'")
        df = df.drop(index=to_remove_indices)
    else:
        print("\nℹ️ Nenhuma duplicata para remover")

    # DEBUG: Mostrar primeiras linhas após o processamento
    print("\n🔍 Após o processamento de duplicatas:")
    print(df[["Data de Início", "Tipo de Encaminhamento", "Causa de Não Atendimento"]].head())

    # Remover colunas desnecessárias
    cols_to_drop = [
        "Fuso Horário", "Número de Páginas do Fax", "Tempo da Fila de Espera",
        "Tipo de Encaminhamento", "Percurso no Grupo de Atendimento",
        "Identificação de chamada reencaminhada", "Contexto de Acesso da Chamada", "Tipo de Telefone", "Tipo de localização", "Utilizador", "País", "Identificação Chamada", "Identificador Global da Chamada", "Serviço"
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors="ignore")

    colunas_para_formatar = ['Origem', 'Destino', 'Destino Final']
    for col in colunas_para_formatar:
        if col in df.columns:
            df[col] = df[col].apply(formatar_numero)
        else:
            print(f"⚠️ Coluna '{col}' não encontrada no DataFrame.")

    print(df[['Origem', 'Destino', 'Destino Final']].head(10))


    # Reordenar do mais recente para o mais antigo
    df = df.sort_values(by="Data de Início", ascending=False).reset_index(drop=True)

    output_dir = "../output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "clean_data.csv")
    
    df.to_csv(output_file, index=False, sep=";")
    print(f"\n✅ Dados limpos salvos em: {output_file}")
    print(f"📊 Total final de registros: {len(df)}")

if __name__ == "__main__":
    clean_data()