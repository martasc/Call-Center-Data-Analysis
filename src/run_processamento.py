import argparse
import chamadas_nao_atendidas
import setup_environment
import limpeza_dados
import calculo_SLAs
import display_SLAs
import metricas
from datetime import datetime

def run_all():
    print(f"\n🔎 Inicio da execução:")
    setup_environment.setup_cleaning_environment()
    metricas.analisar_chamadas()
 
    
    #Chamadar scripts todos

if __name__ == "__main__":
    run_all()