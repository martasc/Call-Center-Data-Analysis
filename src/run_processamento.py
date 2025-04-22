import argparse
import contagem_nrs_unicos
import chamadas_nao_atendidas
import limpeza_dados
import calculo_SLAs
import display_SLAs
from datetime import datetime

def validar_data(data_str):
    """Valida o formato da data e converte para string formatada"""
    try:
        datetime.strptime(data_str, '%Y-%m-%d')
        return data_str  # Mantém como string para ser convertido depois
    except ValueError:
        raise ValueError("Formato de data inválido. Use YYYY-MM-DD")

def run_all(data_inicio=None, data_fim=None):
    print(f"\n🔎 Executando processamento com filtro:")
    print(f"• Data início: {data_inicio if data_inicio else 'Não definida'}")
    print(f"• Data fim: {data_fim if data_fim else 'Não definida'}")
    
    limpeza_dados.clean_data(data_inicio=data_inicio, data_fim=data_fim) 
    contagem_nrs_unicos.main()
    chamadas_nao_atendidas.main()  
    calculo_SLAs.processar_dados_chamadas()
    display_SLAs.main()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Processar dados de chamadas telefônicas')
    parser.add_argument('--inicio', help='Data de início no formato YYYY-MM-DD (ex: 2023-01-01)')
    parser.add_argument('--fim', help='Data de fim no formato YYYY-MM-DD (ex: 2023-12-31)')
    
    args = parser.parse_args()
    
    # Se não foram passados argumentos, perguntar interativamente
    if not args.inicio and not args.fim:
        resposta = input("Deseja filtrar por data? (s/n): ").lower()
        if resposta == 's':
            args.inicio = input("Data de início (YYYY-MM-DD): ") or None
            args.fim = input("Data de fim (YYYY-MM-DD): ") or None
    
    # Validar as datas
    data_inicio = validar_data(args.inicio) if args.inicio else None
    data_fim = validar_data(args.fim) if args.fim else None
    
    run_all(data_inicio=data_inicio, data_fim=data_fim)