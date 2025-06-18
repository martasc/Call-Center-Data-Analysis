from datetime import timedelta
import os
import glob
import shutil
from pathlib import Path
import pandas as pd

DADOS_COMPLETOS = "../input/junho.csv"
OUTPUT_FOLDER = "../output"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "clean_data.csv")

def remove_output_files(output_folder=OUTPUT_FOLDER):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"📁 Diretório criado: {output_folder}")
    else:
        files = glob.glob(os.path.join(output_folder, "*"))
        for f in files:
            try:
                os.remove(f)
            except IsADirectoryError:
                shutil.rmtree(f)
        print(f"🧼 Diretório limpo: {output_folder}")

def remover_nao_atendidas_apos_chamada_recebida(df):
    df = df.copy()
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors="coerce")

    def normalize(n):
        if pd.isna(n): return ""
        return str(n).strip().replace("'", "").replace(" ", "").removeprefix("+351").removeprefix("351")

    df["Origem_norm"] = df["Origem"].apply(normalize)
    df["Destino_norm"] = df["Destino"].apply(normalize)

    chamadas_nao_atendidas = df[df["Tipo"] == "Chamada Não Atendida"]
    chamadas_efetuadas = df[df["Tipo"] == "Chamada efetuada"]

    indices_para_remover = set()

    for idx, row_na in chamadas_nao_atendidas.iterrows():
        origem_na = row_na["Origem_norm"]
        data_na = row_na["Data de Início"]

        # Procurar chamadas efetuadas anteriores com destino igual à origem da NA
        houve_chamada_para_essa_origem = chamadas_efetuadas[
            (chamadas_efetuadas["Destino_norm"] == origem_na) &
            (chamadas_efetuadas["Data de Início"] < data_na)
        ]

        if not houve_chamada_para_essa_origem.empty:
            indices_para_remover.add(idx)

    return df.drop(index=indices_para_remover).reset_index(drop=True)

def contar_chamadas(grupo):
    grupo = grupo.sort_values('Data de Início', ascending=False).reset_index(drop=True)
    grupo['Total Chamadas'] = pd.NA
    usados = set()

    for idx in grupo.index:
        if idx in usados:
            continue

        tipo = grupo.loc[idx, 'Tipo']

        if tipo == "Chamada recebida":
            # Chamada atendida → não agrupa ninguém, conta sozinha
            grupo.loc[idx, 'Total Chamadas'] = 1
            usados.add(idx)
            continue

        t = grupo.loc[idx, 'Data de Início']
        # Define a janela de 1 hora anterior
        janela = grupo[
            (grupo['Data de Início'] <= t) & 
            (grupo['Data de Início'] >= t - timedelta(hours=1))
        ]
        janela = janela[~janela.index.isin(usados)]

        # Exclui chamadas atendidas da janela
        janela = janela[grupo.loc[janela.index, 'Tipo'] != "Chamada recebida"]

        if len(janela) == 1:
            grupo.loc[idx, 'Total Chamadas'] = 1
            usados.add(idx)
        elif len(janela) > 1:
            grupo.loc[janela.index[0], 'Total Chamadas'] = len(janela)
            usados.update(janela.index)

    return grupo

def filtrar_devoluções(df_filtrada, output_folder=OUTPUT_FOLDER):
    """Filtra e exporta apenas as primeiras chamadas efetuadas válidas como devolução a cada chamada não atendida."""

    df = df_filtrada.copy()
    df["Data de Início"] = pd.to_datetime(df["Data de Início"], errors='coerce')

    # Normalize numbers
    def normalize(n):
        if pd.isna(n): return ""
        return str(n).strip().replace("'", "").replace(" ", "").removeprefix("+351").removeprefix("351")

    df["Origem_norm"] = df["Origem"].apply(normalize)
    df["Destino_norm"] = df["Destino"].apply(normalize)

    chamadas_na = df[df["Tipo"] == "Chamada Não Atendida"]
    chamadas_eff = df[df["Tipo"] == "Chamada efetuada"]

    devolucoes = []

    for _, row_na in chamadas_na.iterrows():
        origem = row_na["Origem_norm"]
        destino = row_na["Destino_norm"]
        data_na = row_na["Data de Início"]

        candidatos = chamadas_eff[
            (chamadas_eff["Origem_norm"] == destino) &
            (chamadas_eff["Destino_norm"] == origem) &
            (chamadas_eff["Data de Início"] > data_na)
        ].sort_values("Data de Início")

        if not candidatos.empty:
            first_return = candidatos.iloc[0].copy()
            first_return["Data Chamada Não Atendida"] = data_na
            devolucoes.append(first_return)

    df_retornos = pd.DataFrame(devolucoes)

    # ✅ Only keep 'Chamada efetuada' rows in output
    df_retornos = df_retornos[df_retornos["Tipo"] == "Chamada efetuada"]

    output_path = os.path.join(output_folder, "chamadas_efetuadas.csv")
    df_retornos.to_csv(output_path, index=False, sep=";")
    print(f"📄 Ficheiro de chamadas efetuadas (devoluções) guardado em: {output_path}")

    return df_retornos


def copy_input_to_output(dados_completos=DADOS_COMPLETOS, output_file=OUTPUT_FILE):
    if not Path(dados_completos).exists():
        print(f"❌ Arquivo de input não encontrado: {dados_completos}")
        return

    try:
        df_completa = pd.read_csv(dados_completos, delimiter=";", skiprows=2)
        df_filtrada = pd.read_csv(dados_completos, delimiter=";", skiprows=2, header=0)

        print(df_filtrada.head(20))
        output_path = os.path.join(OUTPUT_FOLDER, "teste.csv")
        df_filtrada.to_csv(output_path, index=False, sep=";")

        # ❗ Remove inbound replies to our outbound calls
        df_filtrada = remover_nao_atendidas_apos_chamada_recebida(df_filtrada)

        tipos_desejados = ["Chamada recebida", "Chamada Não Atendida"]
        df_filtrado = df_filtrada[df_filtrada["Tipo"].isin(tipos_desejados)]

        df_final = df_filtrado.drop_duplicates(subset="Identificador Global da Chamada", keep="first").reset_index(drop=True)

        required_cols = ['Origem', 'Data de Início', 'Tipo']
        missing_cols = [col for col in required_cols if col not in df_filtrada.columns]
        if missing_cols:
            print(f"⚠️ Aviso: Colunas obrigatórias ausentes após leitura: {missing_cols}")

       


        colunas_a_remover = [
            "Utilizador",
            "Telefone de Origem",
            "Número de Páginas do Fax",
            "Tipo de Telefone",
            "Contexto de Acesso da Chamada",
            "Tipo de localização",
            "Serviço",
            "Tempo da Fila de Espera",
            "País",
            "Identificação de chamada reencaminhada",
            "Percurso no Grupo de Atendimento",
            "Tipo de Encaminhamento"
        ]

        df_final = df_final.drop(columns=[col for col in colunas_a_remover if col in df_final.columns])

        # Garantir formato datetime
        df_final['Data de Início'] = pd.to_datetime(df_final['Data de Início'], errors='coerce')

        # Função corrigida para contagem por janela de 1 hora por número de origem
      
        ###Filtro de numeros chamadas testes
        # Remove espaços em branco e aspas extras da coluna "Origem"
        df_final['Origem'] = df_final['Origem'].str.strip().str.replace("'", "", regex=False)

        valores_remover = ['Anónimo', '+351938116613', '+351915942292', '+351935991897']
        telf_paradela = ['+351234246184']

        # Remove espaços e aspas extras antes de comparar
        df_final['Destino'] = df_final['Destino'].str.strip().str.replace("'", "")

        # Remove as linhas de nrs filtrados
        df_final = df_final[~df_final['Origem'].isin(valores_remover)]
        df_final = df_final[~df_final['Destino'].isin(telf_paradela)]

        # Garante que 'Data de Início' está em datetime
        df_final['Data de Início'] = pd.to_datetime(df_final['Data de Início'])

        # Aplica por número de origem
        df_final = df_final.groupby('Origem', group_keys=False).apply(contar_chamadas)

  
        df_final['Total Chamadas'] = pd.to_numeric(df_final['Total Chamadas'], errors='coerce').astype('Int64')

        df_final = df_final.sort_values('Data de Início', ascending=False).reset_index(drop=True)

        # Exporta o resultado
        output_path = os.path.join(OUTPUT_FOLDER, "chamadas_efetuadas.csv")
        df_filtrada.to_csv(output_path, index=False, sep=";")


        df_final.to_csv(output_file, index=False, sep=";")
        print(f"📄 Ficheiro processado e copiado: {dados_completos} ➡️ {output_file}")

        devolucoes = filtrar_devoluções(df_filtrada, OUTPUT_FOLDER)

        df_combined = pd.concat([df_final, devolucoes], ignore_index=True)
        df_combined["Data de Início"] = pd.to_datetime(df_combined["Data de Início"], errors='coerce')

        # Ordenar do mais recente para o mais antigo
        df_combined = df_combined.sort_values("Data de Início", ascending=False)
        combined_output_path = os.path.join(OUTPUT_FOLDER, "combined_results.csv")
        df_combined.to_csv(combined_output_path, index=False, sep=";")
        print(f"📄 Ficheiro combinado guardado em: {combined_output_path}")

    except Exception as e:
        print(f"❌ Erro ao processar o ficheiro de input: {e}")

def setup_cleaning_environment():
    print("🧹 Preparando ambiente de limpeza...")
    remove_output_files()
    copy_input_to_output()
    print("\n🔍 Verificando arquivos gerados...")
    
    # Verifica os arquivos criados
    for fname in ["clean_data.csv", "chamadas_efetuadas.csv"]:
        path = os.path.join(OUTPUT_FOLDER, fname)
        if os.path.exists(path):
            df = pd.read_csv(path, sep=";")
            print(f"  {fname}: {len(df)} registros (Tipos: {df['Tipo'].unique()})")
        else:
            print(f"  ⚠️ {fname} não encontrado")
    
    # Processa a combinação
    print("\n🔄 Combinando arquivos...")
    copy_input_to_output()
    print("✅ Ambiente pronto.")