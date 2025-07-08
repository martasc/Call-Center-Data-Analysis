import argparse
import setup_environment
import metricas
from datetime import datetime

def run_all():
    print(f"\033[1;34m\nInicio da execução do script:\033[0m")

    setup_environment.setup_cleaning_environment()
    metricas.analisar_chamadas()


if __name__ == "__main__":
    run_all()