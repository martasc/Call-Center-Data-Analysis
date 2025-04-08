# run_processamento.py

import contagem_nrs_unicos
import kpi_chamadas_devolvidas
import limpeza_dados

def run_all():
    limpeza_dados.clean_data() 
    contagem_nrs_unicos.main()

    kpi_chamadas_devolvidas.main()  # Calling the function from kpi_chamadas_devolvidas.py
    # Calling the function from limpeza_dados.py

if __name__ == "__main__":
    run_all()



