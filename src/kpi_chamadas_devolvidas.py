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
    return re.sub(r"\D", "", str(numero))[-9:]  

def analisar_devolucoes_e_nao_atendidas(df):
    """Analisa devoluções para chamadas efetuadas e também chamadas não atendidas nunca devolvidas"""
    df['Data de Início'] = df['Data de Início'].apply(parse_date)
    df = df[~df['Destino'].astype(str).str.contains(r"'?4\*\*'?", regex=True)]
    df = df.sort_values('Data de Início')

    devolucoes = []
    nao_atendidas_nao_devolvidas = []

    # Dicionário para armazenar tentativas por origem
    historico_origens = {}
    # Conjunto para marcar origens que receberam devolução
    origens_com_devolucao = set()

    for _, row in df.iterrows():
        tipo = row['Tipo']
        data = row['Data de Início']
        origem = str(row['Origem']).strip()
        destino = str(row['Destino']).strip()

        # Se for uma tentativa (não "Chamada efetuada")
        if tipo != 'Chamada efetuada':
            if origem not in historico_origens:
                historico_origens[origem] = []
            historico_origens[origem].append((data, tipo))

        # Se for uma devolução (chamada efetuada para alguém que nos ligou antes)
        else:
            if destino in historico_origens and historico_origens[destino]:
                chamadas_anteriores = [d for d, t in historico_origens[destino] if d < data]

                if chamadas_anteriores:
                    ultima_tentativa = max([d for d, t in historico_origens[destino] if d < data])
                    primeira_tentativa = min([d for d, t in historico_origens[destino] if d < data])
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
                    origens_com_devolucao.add(destino)

                # Zerar o histórico após a devolução
                historico_origens[destino] = []

    # Identificar chamadas não atendidas que nunca foram devolvidas
    for origem, chamadas in historico_origens.items():
        if origem not in origens_com_devolucao:
            # Verificar se houve alguma chamada atendida para esta origem
            chamadas_atendidas = [t for d, t in chamadas if 'não atendida' not in t.lower()]
            
            # Só considerar como não devolvida se NÃO houve nenhuma chamada atendida
            if not chamadas_atendidas:
                # Filtrar apenas chamadas não atendidas
                nao_atendidas = [d for d, t in chamadas if 'não atendida' in t.lower()]
                
                if nao_atendidas:
                    ultima_data = max(nao_atendidas)
                    primeira_data = min(nao_atendidas)
                    total_chamadas = len(nao_atendidas)
                    
                    registro = {
                        'Origem': origem,
                        'Ultima tentativa': ultima_data,
                        'Primeira tentativa': primeira_data,
                        'Total Tentativas': total_chamadas,
                        'Status': 'Não atendida e não devolvida'
                    }
                    nao_atendidas_nao_devolvidas.append(registro)

    df_devolucoes = pd.DataFrame(devolucoes)
    df_nao_devolvidas = pd.DataFrame(nao_atendidas_nao_devolvidas)
    
    if not df_devolucoes.empty:
        df_devolucoes = df_devolucoes.sort_values('Data Devolução')
    
    return df_devolucoes, df_nao_devolvidas

def main():
    try:
        print("🔍 Analisando devoluções e chamadas não atendidas não devolvidas...")
        
        # Carregar CSV
        df = pd.read_csv('../output/clean_data.csv', delimiter=';', quotechar="'")
        
        # Rodar análise
        devolucoes, nao_devolvidas = analisar_devolucoes_e_nao_atendidas(df)
        
        if not devolucoes.empty or not nao_devolvidas.empty:
            # Salvar em Excel (múltiplas abas)
            with pd.ExcelWriter('../output/chamadas.xlsx') as writer:
                # Guardar Todas as chamadas
                df.to_excel(writer, sheet_name='Todas as Chamadas', index=False)
                if not devolucoes.empty:
                    print(f"\n📊 Total de devoluções encontradas: {len(devolucoes)}\n")
                    cols = [
                        'Origem', 'Destino', 'Data Devolução',
                        'Ultima tentativa de chamada', 'Primeira tentativa de chamada',
                        'Tempo Formatado', 'Total Chamadas da Origem'
                    ]
                    print("📋 Exemplos de devoluções:")
                    print(devolucoes[cols].head(10).to_markdown(index=False))
                    devolucoes.to_excel(writer, sheet_name='Chamadas Devolvidas', index=False)
                    # Salvar também como CSV
                    devolucoes.to_csv('../output/chamadas_devolvidas.csv', index=False, sep=';')
                
                if not nao_devolvidas.empty:
                    print(f"\n📊 Total de chamadas não atendidas não devolvidas: {len(nao_devolvidas)}\n")
                    cols = [
                        'Origem', 'Ultima tentativa', 'Primeira tentativa',
                        'Total Tentativas', 'Status'
                    ]
                    print("📋 Exemplos de não devolvidas:")
                    print(nao_devolvidas[cols].head(10).to_markdown(index=False))
                    nao_devolvidas.to_excel(writer, sheet_name='Não Atendidas Não Devolvidas', index=False)
                    # Salvar também como CSV
                    nao_devolvidas.to_csv('../output/chamadas_nao_devolvidas.csv', index=False, sep=';')
            
            print("\n✅ Arquivos gerados:")
            print("- chamadas.xlsx (Excel com múltiplas abas)")
            print("- chamadas_devolvidas.csv")
            print("- chamadas_nao_devolvidas.csv")
        else:
            print("\n⚠️ Nenhum dado encontrado para análise.")
            
    except Exception as e:
        print(f"❌ Erro: {str(e)}")


if __name__ == "__main__":
    main()
