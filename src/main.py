import sys
from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input
from config import OUTPUT_DIR

def setup_cleaning_environment(input_file_path):
    clear_output_directory(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    success = process_and_clean_input(input_file_path)
    print(success) 
    if success:
        print("✅ Processamento concluído com sucesso")
        metricas.analisar_chamadas()
    else:
        print("❌ Falha no processamento")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python main.py <ficheiro_input.csv>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    setup_cleaning_environment(input_file_path)
    


