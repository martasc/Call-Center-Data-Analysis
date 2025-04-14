import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import os

def carregar_dados(caminho_arquivo):
    """Carrega os dados do arquivo CSV"""
    try:
        df = pd.read_csv(caminho_arquivo, delimiter=';')
        
        # Verificar colunas obrigatórias
        colunas_obrigatorias = ['Origem', 'Data de Início', 'Tipo']
        for col in colunas_obrigatorias:
            if col not in df.columns:
                raise ValueError(f"Coluna obrigatória '{col}' não encontrada no arquivo")
                
        return df
    except Exception as e:
        raise ValueError(f"Erro ao carregar arquivo: {str(e)}")

def processar_dados(df):
    """Processa os dados de chamadas com as novas regras"""
    # Converter datas
    df['Data de Início'] = pd.to_datetime(df['Data de Início'], errors='coerce')
    
    # Filtrar apenas linhas com datas válidas
    df = df.dropna(subset=['Data de Início'])
    

    # Criar coluna de hora (sem minutos/segundos) para agrupamento
    df['Hora'] = df['Data de Início'].dt.floor('H')
    
    # Ordenar por origem e data
    df = df.sort_values(['Origem', 'Data de Início'])
    
    # Inicializar coluna de contagem
    df['Total Chamadas da Origem'] = 0
    
    # Agrupar por origem e hora
    grupos = df.groupby(['Origem', 'Hora'])
    
    for (origem, hora), grupo in grupos:
        # Filtrar apenas chamadas não atendidas (se necessário)
        chamadas_validas = grupo[grupo['Tipo'] != "Chamada efetuada"]
        

        if not chamadas_validas.empty:
            # Pegar índice da última chamada do grupo
            ultimo_idx = chamadas_validas.index[-1]
            
            # Contar chamadas (começando em 1)
            total = len(chamadas_validas)
            
            # Atribuir apenas na última chamada
            df.at[ultimo_idx, 'Total Chamadas da Origem'] = total

    # Remover coluna auxiliar de hora
    df = df.drop(columns=['Hora'])
    
    # Restaurar ordem original
    df = df.sort_index()
    
    return df

def exportar_resultados(df, caminho_saida):
    """Exporta os resultados para um novo arquivo CSV"""
    try:
        # Garantir que o diretório de saída existe
        os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
        
        df.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"Arquivo salvo em: {caminho_saida}")
    except Exception as e:
        raise ValueError(f"Erro ao exportar resultados: {str(e)}")

def main():
    # Configurar caminhos
    # Get absolute path to project root (src's parent)
    PROJECT_ROOT = Path(__file__).parent.parent

    # Then reference all files like this:
    input_file = PROJECT_ROOT / 'output' / 'clean_data.csv'
    output_file = input_file  
    
    try:
        print("\nIniciando processamento de contagem de números únicos...")
        
        # 1. Carregar dados
        print(f"Carregando dados de {input_file}...")
        df = carregar_dados(input_file)
        
        # 2. Processar dados
        print("Processando contagem de chamadas por origem...")
        df_processado = processar_dados(df)
        
        # 3. Exportar resultados
        print(f"Salvando resultados em {output_file}...")
        exportar_resultados(df_processado, output_file)
        
        # 4. Mostrar resumo
        print("\nResumo do processamento:")
        print(f"- Total de registros processados: {len(df)}")
        print(f"- Total de origens únicas: {df['Origem'].nunique()}")
        print("- Amostra dos resultados:")
        print(df_processado[['Origem', 'Data de Início', 'Total Chamadas da Origem']].head())
        
        print("\nProcesso concluído com sucesso!")
        return True
        
    except Exception as e:
        print(f"\n❌ Erro durante o processamento: {str(e)}")
        return False

if __name__ == "__main__":
    main()
