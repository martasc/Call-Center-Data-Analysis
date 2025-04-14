import pandas as pd
from datetime import datetime
import re

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
def normalizar_numero(numero):
    if pd.isna(numero):
        return ""
    return re.sub(r"\D", "", str(numero))[-9:]  # pega apenas os últimos 9 dígitos (úteis em PT)


def analisar_devolucoes_final(df):
    """Analisa devoluções para chamadas efetuadas, reiniciando histórico após devolução"""
    df['Data de Início'] = df['Data de Início'].apply(parse_date)
    df = df[~df['Destino'].astype(str).str.contains(r"'?4\*\*'?", regex=True)]
    df = df.sort_values('Data de Início')

    devolucoes = []

    # Dicionário para armazenar tentativas por origem
    historico_origens = {}

    for _, row in df.iterrows():
        tipo = row['Tipo']
        data = row['Data de Início']
        origem = str(row['Origem']).strip()
        destino = str(row['Destino']).strip()

        # Se for uma tentativa (não "Chamada efetuada")
        if tipo != 'Chamada efetuada':
            if origem not in historico_origens:
                historico_origens[origem] = []
            historico_origens[origem].append(data)

        # Se for uma devolução (chamada efetuada para alguém que nos ligou antes)
        else:
            if destino in historico_origens and historico_origens[destino]:
                chamadas_anteriores = [d for d in historico_origens[destino] if d < data]

                if chamadas_anteriores:
                    ultima_tentativa = max(chamadas_anteriores)
                    primeira_tentativa = min(chamadas_anteriores)
                    segundos = (data - ultima_tentativa).total_seconds()

                    registro = {
                        'Origem': row['Origem'],
                        'Destino': row['Destino'],
                        'Data Devolução': data,
                        'Ultima tentativa de chamada': ultima_tentativa,
                        'Primeira tentativa de chamada': primeira_tentativa,
                        'Tempo até Devolução (s)': segundos,
                        'Tempo Formatado': formatar_tempo(segundos),
                        'Total Chamadas da Origem': len(chamadas_anteriores)
                    }

                    devolucoes.append(registro)

                # ✅ Zerar o histórico após a devolução
                historico_origens[destino] = []

    df_devolucoes = pd.DataFrame(devolucoes)
    if not df_devolucoes.empty:
        df_devolucoes = df_devolucoes.sort_values('Data Devolução')
    
    return df_devolucoes


def main():
    try:
        print("🔍 Analisando devoluções para chamadas efetuadas...")
        
        # Carregar CSV
        df = pd.read_csv('../output/clean_data.csv', delimiter=';', quotechar="'")
        
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
