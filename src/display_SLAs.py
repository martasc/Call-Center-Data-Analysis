import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta

def processar_dados_chamadas():
    # === Load CSVs ===
    df_clean = pd.read_csv('../output/clean_data.csv', delimiter=';', quotechar="'")
    df_devolvidas = pd.read_csv('../output/chamadas_devolvidas.csv', delimiter=';', quotechar="'")
    try:
        df_nao_devolvidas = pd.read_csv('../output/chamadas_nao_devolvidas.csv', delimiter=';', quotechar="'")
    except FileNotFoundError:
        df_nao_devolvidas = pd.DataFrame(columns=['Origem'])

    # === Pre-process ===
    df_clean.columns = df_clean.columns.str.strip()
    df_clean['Data de Início'] = pd.to_datetime(df_clean['Data de Início'], errors='coerce')
    df_clean['Data de Fim'] = pd.to_datetime(df_clean['Data de Fim'], errors='coerce')
    df_clean['Tempo de Toque'] = pd.to_timedelta(df_clean['Tempo de Toque'], errors='coerce')
    df_clean['Duração'] = pd.to_timedelta(df_clean['Duração'], errors='coerce')
    df_clean['Dia da Semana'] = df_clean['Data de Início'].dt.day_name()
    df_clean['Hora'] = df_clean['Data de Início'].dt.hour

    chamadas_atendidas = df_clean[df_clean['Tipo'] == 'Chamada recebida']
    chamadas_nao_atendidas = df_clean[df_clean['Tipo'] == 'Chamada Não Atendida']

    total_chamadas = len(df_clean)
    total_recebidas = len(chamadas_atendidas) + len(chamadas_nao_atendidas)
    percentagem_atendidas = len(chamadas_atendidas) / total_recebidas * 100 if total_recebidas else 0
    percentagem_nao_atendidas = 100 - percentagem_atendidas

    chamadas_nao_devolvidas = len(df_nao_devolvidas)
    chamadas_nao_devolvidas_unicas = df_nao_devolvidas['Origem'].nunique()
    chamadas_devolvidas = len(df_devolvidas)

    tempo_medio_espera = df_clean['Tempo de Toque'].mean()
    duracao_media = df_clean['Duração'].mean()
    duracao_formatada = str(timedelta(seconds=int(duracao_media.total_seconds()))) if pd.notna(duracao_media) else "N/A"

    tempo_medio_devolucao = None
    try:
        df_devolvidas['Tempo Formatado (min)'] = df_devolvidas['Tempo Formatado'].str.replace('min', '').astype(float)
        tempo_medio_devolucao = df_devolvidas['Tempo Formatado (min)'].mean()
    except Exception as e:
        print(f"[!] Erro ao calcular tempo médio de devolução: {e}")

    # === Chamadas atendidas <60s ===
    chamadas_recebidas_rapidas = chamadas_atendidas[df_clean['Tempo de Toque'] < pd.Timedelta(seconds=60)]
    total_rapidas = len(chamadas_recebidas_rapidas)
    perc_rapidas = (total_rapidas / len(chamadas_atendidas) * 100) if len(chamadas_atendidas) else 0

    # === % devolvidas até 3min e 15min (por nrs únicos) ===
    try:
        total_nrs_unicos_devolvidos = df_devolvidas['Origem'].nunique()
        nrs_ate_3min = df_devolvidas[df_devolvidas['Tempo Formatado (min)'] <= 3]['Origem'].nunique()
        nrs_ate_15min = df_devolvidas[df_devolvidas['Tempo Formatado (min)'] <= 15]['Origem'].nunique()
        perc_ate_3min = (nrs_ate_3min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
        perc_ate_15min = (nrs_ate_15min / total_nrs_unicos_devolvidos * 100) if total_nrs_unicos_devolvidos else 0
    except:
        perc_ate_3min = perc_ate_15min = 0

    resultados = {
        "Total de chamadas": total_chamadas,
        "Chamadas atendidas": len(chamadas_atendidas),
        "Chamadas não atendidas": len(chamadas_nao_atendidas),
        "% atendidas": round(percentagem_atendidas, 2),
        "% não atendidas": round(percentagem_nao_atendidas, 2),
        "Chamadas devolvidas": chamadas_devolvidas,
        "% devolvidas sobre chamadas não atendidas (nrs únicos)": round((chamadas_devolvidas / df_nao_devolvidas['Origem'].nunique() * 100), 2) if not df_nao_devolvidas.empty else 0,
        "Tempo médio entre não atendida e devolvida (min)": round(tempo_medio_devolucao, 2) if tempo_medio_devolucao else "N/A",
        "Chamadas não atendidas e não devolvidas": chamadas_nao_devolvidas,
        "Tempo médio de espera (s)": round(tempo_medio_espera.total_seconds(), 2) if pd.notna(tempo_medio_espera) else "N/A",
        "Duração média da chamada": duracao_formatada,
        "Chamadas atendidas com toque < 60s": total_rapidas,
        "% Chamadas atendidas com toque < 60s": round(perc_rapidas, 2),
        "% Devolvidas até 3min": round(perc_ate_3min, 2),
        "% Devolvidas até 15min": round(perc_ate_15min, 2),
        "df_clean": df_clean,
        "df_devolvidas": df_devolvidas
    }

    return resultados


def plot_graficos(df_clean, df_devolvidas, chamadas_atendidas, chamadas_nao_atendidas):
    dias_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # Gráfico de Barras
    plt.figure(figsize=(10, 6))
    vol_por_dia = df_clean['Dia da Semana'].value_counts().reindex(dias_ordem)
    sns.barplot(x=vol_por_dia.index, y=vol_por_dia.values, palette="Blues_d")
    plt.title("Volume de Chamadas por Dia da Semana")
    plt.xlabel("Dia")
    plt.ylabel("Chamadas")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Pie Chart
    """perdidas = len(chamadas_nao_atendidas) - len(df_devolvidas)
    plt.figure(figsize=(6, 6))
    labels = ['Atendidas', 'Devolvidas', 'Perdidas']
    sizes = [len(chamadas_atendidas), len(df_devolvidas), perdidas]
    colors = ['#4caf50', '#2196f3', '#f44336']
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors)
    plt.title("Distribuição de Chamadas")
    plt.axis('equal')
    plt.show()"""

    # Heatmap
    heatmap_data = df_clean.groupby(['Dia da Semana', 'Hora']).size().unstack().reindex(dias_ordem)
    plt.figure(figsize=(12, 6))
    sns.heatmap(heatmap_data, cmap='YlOrRd', linewidths=0.5, linecolor='gray')
    plt.title("Picos de Chamadas por Hora e Dia da Semana")
    plt.xlabel("Hora do Dia")
    plt.ylabel("Dia da Semana")
    plt.tight_layout()
    plt.show()


def main():
    resultados = processar_dados_chamadas()
    print("\n📋 MÉTRICAS GERAIS:")
    for chave, valor in resultados.items():
        if not isinstance(valor, pd.DataFrame):
            print(f"{chave}: {valor}")

    # Plotar gráficos
    plot_graficos(resultados['df_clean'], resultados['df_devolvidas'],
                  resultados['df_clean'][resultados['df_clean']['Tipo'] == 'Chamada recebida'],
                  resultados['df_clean'][resultados['df_clean']['Tipo'] == 'Chamada Não Atendida'])


if __name__ == "__main__":
    main()
