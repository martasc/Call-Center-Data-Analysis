import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def processar_dados_chamadas():
    """
    Processa dados de chamadas telefônicas a partir de arquivos CSV.
    Retorna um dicionário com métricas e os DataFrames processados.
    """
    df_clean = pd.read_csv('../output/clean_data.csv', delimiter=';', quotechar="'")
    
    # Leitura dos arquivos de chamadas devolvidas e não devolvidas
    try:
        df_devolvidas = pd.read_csv('../output/chamadas_devolvidas.csv', delimiter=';', quotechar="'")
    except FileNotFoundError:
        logging.warning("Arquivo chamadas_devolvidas.csv não encontrado.")
        df_devolvidas = pd.DataFrame(columns=['Origem', 'Tempo até Devolução (s)'])

    try:
        df_nao_devolvidas = pd.read_csv('../output/chamadas_nao_devolvidas.csv', delimiter=';', quotechar="'")
    except FileNotFoundError:
        logging.warning("Arquivo chamadas_nao_devolvidas.csv não encontrado.")
        df_nao_devolvidas = pd.DataFrame(columns=['Origem'])

    # Limpeza e transformação dos dados
    df_clean.columns = df_clean.columns.str.strip()
    df_clean['Data de Início'] = pd.to_datetime(df_clean['Data de Início'], errors='coerce')
    df_clean['Data de Fim'] = pd.to_datetime(df_clean['Data de Fim'], errors='coerce')
    df_clean['Tempo de Toque'] = pd.to_timedelta(df_clean['Tempo de Toque'], errors='coerce')
    df_clean['Duração'] = pd.to_timedelta(df_clean['Duração'], errors='coerce')
    df_clean['Dia da Semana'] = df_clean['Data de Início'].dt.day_name()
    df_clean['Hora'] = df_clean['Data de Início'].dt.hour
    df_clean['Total Chamadas da Origem'] = df_clean.groupby('Origem')['Origem'].transform('count')

    chamadas_atendidas = df_clean[df_clean['Tipo'] == 'Chamada recebida']
    chamadas_nao_atendidas = df_clean[df_clean['Tipo'] == 'Chamada Não Atendida']

    # Métricas principais
    total_chamadas = len(df_clean)
    total_chamadas_nrs_unicos = df_clean['Origem'].nunique()
    total_recebidas = len(chamadas_atendidas) + len(chamadas_nao_atendidas)
    percentagem_atendidas = len(chamadas_atendidas) / total_recebidas * 100 if total_recebidas else 0
    percentagem_nao_atendidas = 100 - percentagem_atendidas

    chamadas_nao_devolvidas = len(df_nao_devolvidas)
    chamadas_nao_devolvidas_unicas = df_nao_devolvidas['Origem'].nunique()
    chamadas_devolvidas = len(df_devolvidas)

    tempo_medio_espera = df_clean['Tempo de Toque'].mean()
    duracao_media = df_clean['Duração'].mean()
    duracao_formatada = str(timedelta(seconds=int(duracao_media.total_seconds()))) if pd.notna(duracao_media) else "N/A"

    # Tempo médio entre não atendida e devolvida
    try:
        media_em_segundos = df_devolvidas['Tempo até Devolução (s)'].mean()
        tempo_medio_devolucao = round(media_em_segundos) if pd.notna(media_em_segundos) else None
        tempo_formatado = str(timedelta(seconds=tempo_medio_devolucao)) if tempo_medio_devolucao else "N/A"
        logging.info(f"Tempo médio entre não atendida e devolvida: {tempo_formatado}")
    except Exception as e:
        logging.warning(f"Erro ao calcular tempo médio de devolução: {e}")
        tempo_formatado = "N/A"
        tempo_medio_devolucao = None

    # Chamadas rápidas
    chamadas_recebidas_rapidas = chamadas_atendidas[chamadas_atendidas['Tempo de Toque'] < pd.Timedelta(seconds=60)]
    total_rapidas = len(chamadas_recebidas_rapidas)
    perc_rapidas = (total_rapidas / len(chamadas_atendidas) * 100) if len(chamadas_atendidas) else 0

    # Devoluções rápidas
    try:
        total_nrs_unicos_devolvidos = df_devolvidas['Origem'].nunique()
        nrs_ate_3min = df_devolvidas[df_devolvidas['Tempo até Devolução (s)'] <= 180]['Origem'].nunique()
        nrs_ate_15min = df_devolvidas[df_devolvidas['Tempo até Devolução (s)'] <= 900]['Origem'].nunique()
        perc_ate_3min = (nrs_ate_3min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
        perc_ate_15min = (nrs_ate_15min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
    except:
        perc_ate_3min = perc_ate_15min = 0

    media_chamadas_por_origem = df_clean['Total Chamadas da Origem'].mean()

    # Dicionário do return
    return {
        "Total de chamadas": total_chamadas,
        "Total de nrs únicos": total_chamadas_nrs_unicos,
        "Total de chamadas atendidas": total_recebidas,
        "Chamadas atendidas": len(chamadas_atendidas),
        "Chamadas não atendidas": len(chamadas_nao_atendidas),
        "% atendidas": round(percentagem_atendidas, 2),
        "% não atendidas": round(percentagem_nao_atendidas, 2),
        "Chamadas devolvidas": chamadas_devolvidas,
        "% devolvidas sobre chamadas não atendidas (nrs únicos)": round((chamadas_devolvidas / chamadas_nao_devolvidas_unicas * 100), 2) if chamadas_nao_devolvidas_unicas else 0,
        "Chamadas não atendidas e não devolvidas": chamadas_nao_devolvidas,
        "Tempo médio de espera (s)": round(tempo_medio_espera.total_seconds(), 2) if pd.notna(tempo_medio_espera) else "N/A",
        "Duração média da chamada": duracao_formatada,
        "Chamadas atendidas com toque < 60s": total_rapidas,
        "% Chamadas atendidas com toque < 60s": round(perc_rapidas, 2),
        "% Devolvidas até 3min": round(perc_ate_3min, 2),
        "% Devolvidas até 15min": round(perc_ate_15min, 2),
        "📊 Média de chamadas por número único": round(media_chamadas_por_origem, 2),
        "df_clean": df_clean,
        "df_devolvidas": df_devolvidas
    }


def plot_graficos(df_clean):
    """
    Gera gráficos de média de chamadas únicas por dia da semana (proporcional ao total de chamadas).
    """
    # Renomear se necessário
    if 'Data de Início' in df_clean.columns:
        df_clean = df_clean.rename(columns={"Data de Início": "Data"})

    # Converter data e extrair hora e dia da semana
    df_clean['Data'] = pd.to_datetime(df_clean['Data'])
    df_clean['Hora'] = df_clean['Data'].dt.hour
    df_clean['Dia da Semana'] = df_clean['Data'].dt.day_name()

    # Filtrar chamadas que NÃO são "Chamada efetuada"
    df_filtrado = df_clean[df_clean['Tipo'] != 'Chamada efetuada'].copy()

    # Dicionário e ordem para tradução dos dias
    dias_ordem_english = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    english_to_portuguese = {
        'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta',
        'Thursday': 'Quinta', 'Friday': 'Sexta', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }

    # Garantir a ordem correta
    df_filtrado['Dia da Semana'] = pd.Categorical(df_filtrado['Dia da Semana'],
                                                  categories=dias_ordem_english,
                                                  ordered=True)

    # Total de chamadas após o filtro
    total_chamadas = len(df_filtrado)

    # Agrupar por dia da semana e contar chamadas
    chamadas_por_dia = df_filtrado.groupby('Dia da Semana')['Tipo'].count()
    medias = chamadas_por_dia / total_chamadas

    # Gráfico de barras
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x=[english_to_portuguese[dia] for dia in dias_ordem_english],
        y=[medias.get(dia, 0) for dia in dias_ordem_english],
        palette='Blues_d'
    )
    plt.title('Média de chamadas por Dia da Semana')
    plt.xlabel('Dia da Semana')
    plt.ylabel('Média de Chamadas')
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.show()


def main():
    resultados = processar_dados_chamadas()
    plot_graficos(resultados['df_clean'])


if __name__ == "__main__":
    main()
