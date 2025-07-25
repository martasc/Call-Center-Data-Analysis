from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input
from config import OUTPUT_DIR

def setup_cleaning_environment():
    clear_output_directory(OUTPUT_DIR)
    process_and_clean_input()
    metricas.analisar_chamadas()

if __name__ == "__main__":
    setup_cleaning_environment()

    


