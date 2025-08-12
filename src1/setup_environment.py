import os
import glob
import shutil
from pathlib import Path
import pandas as pd

INPUT_FILE = "../input/março.csv"
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

def copy_input_to_output(input_file=INPUT_FILE, output_file=OUTPUT_FILE):
    if not Path(input_file).exists():
        print(f"❌ Arquivo de input não encontrado: {input_file}")
        return

    try:
        # Read CSV skipping first 2 rows (header and notes)
        df = pd.read_csv(input_file, delimiter=";", skiprows=2)
        
        # Validate important columns
        required_cols = ['Origem', 'Data de Início', 'Tipo']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"⚠️ Aviso: Colunas obrigatórias ausentes após leitura: {missing_cols}")

        # Write cleaned version to output
        df.to_csv(output_file, index=False, sep=";")
        print(f"📄 Ficheiro processado e copiado: {input_file} ➡️ {output_file}")
    except Exception as e:
        print(f"❌ Erro ao processar o ficheiro de input: {e}")

def setup_cleaning_environment():
    print("🧹 Preparando ambiente de limpeza...")
    remove_output_files()
    copy_input_to_output()
    print("✅ Ambiente pronto.")
