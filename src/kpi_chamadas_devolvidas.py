import pandas as pd
from datetime import datetime

def formatar_tempo(segundos):
    """Formata o tempo de forma inteligente: 2s, 10s, 1min, 6h, 24h"""
    if segundos is None:
        return None
    if segundos < 60:
        return f"{int(segundos)}s"
    elif segundos < 3600:
        return f"{int(segundos//60)}min"
    else:
        horas = segundos / 3600
        return f"{horas:.1f}h"

def parse_date(date_str):
    """Parse dates in multiple formats"""
    try:
        return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            return pd.to_datetime(date_str, format='%d/%m/%y %H:%M')
        except ValueError:
            return pd.to_datetime(date_str)

def analisar_devolucoes_final(df):
    """Analisa devoluções para chamadas efetuadas"""
    # Converter datas
    df['Data de Início'] = df['Data de Início'].apply(parse_date)
    
    # Filtrar chamadas com destino != "4**"
    df = df[~df['Destino'].astype(str).str.contains(r"'?4\*\*'?", regex=True)]
    
    # Ordenar por data
    df = df.sort_values('Data de Início')
    
    # Separar chamadas efetuadas das outras
    chamadas_efetuadas = df[df['Tipo'] == 'Chamada efetuada']
    outras_chamadas = df[df['Tipo'] != 'Chamada efetuada']
    
    # Histórico de tentativas por número
    historico_origens = outras_chamadas.groupby('Origem')['Data de Início'].apply(list).to_dict()
    total_chamadas_origem = outras_chamadas['Origem'].value_counts().to_dict()
    
    devolucoes = []

    for _, chamada in chamadas_efetuadas.iterrows():
        destino = str(chamada['Destino']).strip("'\"")
        data_devolucao = chamada['Data de Início']

        if destino in historico_origens:
            todas_chamadas = historico_origens[destino]
            chamadas_anteriores = [d for d in todas_chamadas if d < data_devolucao]
            chamadas_posteriores = [d for d in todas_chamadas if d > data_devolucao]

            ultima_tentativa = max(chamadas_anteriores) if chamadas_anteriores else None
            primeira_tentativa = min(chamadas_posteriores) if chamadas_posteriores else None
            segundos = (data_devolucao - ultima_tentativa).total_seconds() if ultima_tentativa else None

            registro = {
                'Origem': chamada['Origem'],
                'Destino': chamada['Destino'],
                'Data Devolução': data_devolucao,
                'Ultima tentativa de chamada': ultima_tentativa,
                'Primeira tentativa de chamada': primeira_tentativa,
                'Tempo até Devolução (s)': segundos,
                'Tempo Formatado': formatar_tempo(segundos),
                'Total Chamadas da Origem': total_chamadas_origem.get(destino, 0)
            }
            devolucoes.append(registro)
    
    df_devolucoes = pd.DataFrame(devolucoes)
    if not df_devolucoes.empty:
        df_devolucoes = df_devolucoes.sort_values('Data Devolução')
    
    return df_devolucoes

def main():
    try:
        print("🔍 Analisando devoluções para chamadas efetuadas...")
        
        # Carregar CSV
        df = pd.read_csv('../output/added_unique_nrs.csv', delimiter=';', quotechar="'")
        
        # Rodar análise
        devolucoes = analisar_devolucoes_final(df)
        
        if not devolucoes.empty:
            print(f"\n📊 Total de devoluções encontradas: {len(devolucoes)}\n")
            
            cols = [
                'Origem', 'Destino', 'Data Devolução',
                'Ultima tentativa de chamada', 'Primeira tentativa de chamada',
                'Tempo Formatado', 'Total Chamadas da Origem'
            ]
            print("📋 Exemplos:")
            print(devolucoes[cols].head(10).to_markdown(index=False))

            # Exportar resultado
            devolucoes.to_csv('../output/kpi_chamadas_devolvidas.csv', index=False, sep=';')
            print("\n✅ Arquivo 'kpi_chamadas_devolvidas.csv' gerado!")
        else:
            print("\n⚠️ Nenhuma devolução encontrada.")
            
    except Exception as e:
        print(f"❌ Erro: {str(e)}")

if __name__ == "__main__":
    main()
