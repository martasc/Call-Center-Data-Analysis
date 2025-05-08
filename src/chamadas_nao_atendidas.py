import pandas as pd
from datetime import datetime
import re

def formatar_tempo(segundos):
    """Formata o tempo de forma inteligente: 2s, 10s, 1min, 6h30min"""
    if segundos is None:
        return None
    if segundos < 60:
        return f"{int(segundos)}s"
    elif segundos < 3600:
        return f"{int(segundos // 60)}min"
    else:
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        return f"{horas}h{minutos:02d}min"


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

    # Dicionário para armazenar todas as chamadas por origem
    historico_origens = {}
    # Dicionário para armazenar a última chamada não atendida por origem
    ultimas_nao_atendidas = {}
    # Set para armazenar origens que já tiveram sua devolução contabilizada
    devolucoes_contabilizadas = set()

    for _, row in df.iterrows():
        tipo = row['Tipo'].lower()
        data = row['Data de Início']
        origem = str(row['Origem']).strip()
        destino = str(row['Destino']).strip()
        duracao = row.get('Duração', None)

        # Registrar todas as chamadas no histórico
        if origem not in historico_origens:
            historico_origens[origem] = []
        historico_origens[origem].append((data, tipo, duracao))

        # Se for uma chamada não atendida, atualizar o registro
        if 'não atendida' in tipo:
            ultimas_nao_atendidas[origem] = data
            # Resetar o status de devolução contabilizada para esta origem
            if origem in devolucoes_contabilizadas:
                devolucoes_contabilizadas.remove(origem)
        # Se for uma chamada recebida, remover da lista de pendentes (se existir)
        elif 'recebida' in tipo and origem in ultimas_nao_atendidas:
            del ultimas_nao_atendidas[origem]
            if origem in devolucoes_contabilizadas:
                devolucoes_contabilizadas.remove(origem)

        # Se for uma chamada efetuada (potencial devolução)
        elif tipo == 'chamada efetuada':
            # Verificar se é uma devolução para uma origem com chamada não atendida pendente
            if destino in ultimas_nao_atendidas and destino not in devolucoes_contabilizadas:
                ultima_na = ultimas_nao_atendidas[destino]
                
                # Garantir que a devolução é posterior à última chamada não atendida
                if data > ultima_na:
                    # Verificar se não houve chamada recebida entre a não atendida e a devolução
                    houve_chamada_recebida = any(
                        'recebida' in t and ultima_na < d < data 
                        for d, t, _ in historico_origens[destino]
                    )
                    
                    if not houve_chamada_recebida:
                        # Calcular tempo desde a última chamada não atendida
                        segundos = (data - ultima_na).total_seconds()
                        
                        # Contar quantas chamadas não atendidas essa origem fez antes da devolução
                        total_nao_atendidas = len([d for d, t, _ in historico_origens[destino] 
                                                  if 'não atendida' in t and d <= ultima_na])
                        
                        # Registrar a devolução
                        registro = {
                            'Origem': destino,
                            'Destino': origem,
                            'Data Devolução': data,
                            'Ultima tentativa de chamada': ultima_na,
                            'Primeira tentativa de chamada': min([d for d, t, _ in historico_origens[destino] 
                                                                 if 'não atendida' in t]),
                            'Tempo até Devolução (s)': segundos,
                            'Duração': duracao,
                            'Tempo Formatado': formatar_tempo(segundos),
                            'Total Chamadas da Origem': total_nao_atendidas,
                            'Status': 'Devolução atendida' if duracao and pd.notna(duracao) else 'Devolução não atendida'
                        }
                        devolucoes.append(registro)
                        
                        # Marcar que já contabilizamos uma devolução para esta origem
                        devolucoes_contabilizadas.add(destino)

    # Identificar chamadas não atendidas que nunca foram devolvidas
    for origem, chamadas in historico_origens.items():
        # Verificar se é uma origem que teve chamadas não atendidas
        if origem in ultimas_nao_atendidas:
            # Verificar se não teve devolução contabilizada
            if origem not in devolucoes_contabilizadas:
                # Verificar se houve alguma chamada atendida
                teve_chamada_atendida = any('não atendida' not in t and 'recebida' not in t for _, t, _ in chamadas)
                
                if not teve_chamada_atendida:
                    nao_atendidas = [d for d, t, _ in chamadas if 'não atendida' in t]
                    registro = {
                        'Origem': origem,
                        'Ultima tentativa': max(nao_atendidas),
                        'Primeira tentativa': min(nao_atendidas),
                        'Total Tentativas': len(nao_atendidas),
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
        
        # Correr análise
        devolucoes, nao_devolvidas = analisar_devolucoes_e_nao_atendidas(df)


        ###Aqui -TODO - Coluna estado no clean data
        # -------------------- ADICIONA COLUNA "Estado" --------------------
        # Começa com tudo vazio
        df['Estado'] = ""

        # Normalizar colunas para comparação
        df['Origem_norm'] = df['Origem'].apply(normalizar_numero)
        df['Destino_norm'] = df['Destino'].apply(normalizar_numero)

        # Marcar chamadas não atendidas e não devolvidas
        if not nao_devolvidas.empty:
            for origem in nao_devolvidas['Origem']:
                origem_norm = normalizar_numero(origem)
                cond = (
                    (df['Origem_norm'] == origem_norm) &
                    (df['Tipo'].str.contains('não atendida', case=False, na=False)) &
                    (df['Total Chamadas da Origem'] != 0)
                )
                df.loc[cond, 'Estado'] = "Não atendida e não devolvida"

        # Marcar chamadas devolvidas
        if not devolucoes.empty:
            for destino in devolucoes['Destino']:
                destino_norm = normalizar_numero(destino)
                cond = (
                    (df['Destino_norm'] == destino_norm) &
                    (df['Tipo'].str.contains('chamada efetuada', case=False, na=False)) &
                    (df['Total Chamadas da Origem'] != 0)
                )
                df.loc[cond, 'Estado'] = "Não atendida e devolvida"

        # Preencher "Atendida à primeira" onde ainda estiver vazio, mas contagem > 0
        # df.loc[
        #     (df['Estado'] == "") & (df['Total Chamadas da Origem'] != 0),
        #     'Estado'
        # ] = "Atendida à primeira"

        # Remover colunas auxiliares
        df.drop(columns=['Origem_norm', 'Destino_norm'], inplace=True)

        df.loc[df['Total Chamadas da Origem'].isna() | (df['Total Chamadas da Origem'] == 0), 'Estado'] = ""


       
        # ------------------------------------------------------------------




        
        if not devolucoes.empty or not nao_devolvidas.empty:
            with pd.ExcelWriter('../output/chamadas.xlsx') as writer:
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
