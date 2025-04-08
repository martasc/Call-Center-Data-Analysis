import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# Configuration - UPDATED STYLE
plt.style.use('seaborn-v0_8')  # Updated style name
sns.set_theme(style="whitegrid")  # Updated seaborn theme
plt.rcParams['figure.facecolor'] = 'white'  # Set white background

def format_timedelta(td):
    """Formats timedelta to HH:MM:SS"""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Load the file
df = pd.read_csv("../output/added_unique_nrs.csv", encoding='utf-8', sep=';')

# Convert time columns
df['Data de Início'] = pd.to_datetime(df['Data de Início'], errors='coerce')
df['Tempo de Toque'] = pd.to_timedelta(df['Tempo de Toque'], errors='coerce')
df['Duração'] = pd.to_timedelta(df['Duração'], errors='coerce')

# ========================
# CALCULATE CORE METRICS
# ========================
# 1. Basic metrics
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# Configuration - UPDATED STYLE
plt.style.use('seaborn-v0_8')  # Updated style name
sns.set_theme(style="whitegrid")  # Updated seaborn theme
plt.rcParams['figure.facecolor'] = 'white'  # Set white background

def format_timedelta(td):
    """Formats timedelta to HH:MM:SS"""
    if pd.isna(td):
        return "00:00:00"
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Load and prepare data
try:
    df = pd.read_csv("../output/added_unique_nrs.csv", encoding='utf-8', sep=';')
    
    # Convert time columns with error handling
    df['Data de Início'] = pd.to_datetime(df['Data de Início'], errors='coerce')
    df['Tempo de Toque'] = pd.to_timedelta(df['Tempo de Toque'], errors='coerce')
    df['Duração'] = pd.to_timedelta(df['Duração'], errors='coerce')
    
    # Create clean origin/destination columns
    df['Origem_limpo'] = df['Origem'].astype(str).str.extract(r'(\d+)')[0]
    df['Destino_limpo'] = df['Destino'].astype(str).str.extract(r'(\d+)')[0]

    # ========================
    # CALCULATE CORE METRICS
    # ========================

    # 1. Basic metrics
    total_chamadas = len(df)
    chamadas_atendidas = df[df['Atendida'] == "Atendida"].shape[0]
    percent_atendidas = (chamadas_atendidas / total_chamadas) * 100 if total_chamadas > 0 else 0

    # 2. First-call resolutions
    primeira_tentativa = df[
        (df['Total Chamadas da Origem'] == 1) & 
        (df['Tipo'] == 'Chamada recebida')
    ].shape[0]

    # 3. Returned calls metrics
    chamadas_efetuadas = df[df['Tipo'] == 'Chamada efetuada']
    outras_chamadas = df[df['Tipo'] != 'Chamada efetuada']

    # Create mapping of origin attempts
    attempts_map = outras_chamadas.groupby('Origem_limpo').size().to_dict()

    # Calculate return metrics
    devolucoes = []
    for _, row in chamadas_efetuadas.iterrows():
        destino = row['Destino_limpo']
        if pd.notna(destino) and destino in attempts_map:
            ultima_tentativa = outras_chamadas[
                outras_chamadas['Origem_limpo'] == destino
            ]['Data de Início'].max()
            
            if pd.notna(ultima_tentativa):
                devolucoes.append({
                    'Destino': row['Destino'],
                    'Tentativas': attempts_map[destino],
                    'Data Devolução': row['Data de Início'],
                    'Última Tentativa': ultima_tentativa
                })

    df_devolucoes = pd.DataFrame(devolucoes)
    
    if not df_devolucoes.empty:
        df_devolucoes['Tempo para Devolução'] = df_devolucoes['Data Devolução'] - df_devolucoes['Última Tentativa']
        tempo_medio_devolucao = format_timedelta(df_devolucoes['Tempo para Devolução'].mean())
        
        # Return time buckets
        devolvidas_15min = (df_devolucoes['Tempo para Devolução'] <= timedelta(minutes=15)).sum()
        devolvidas_3min = (df_devolucoes['Tempo para Devolução'] <= timedelta(minutes=3)).sum()
        percent_devolvidas_15min = (devolvidas_15min / len(df_devolucoes)) * 100 if len(df_devolucoes) > 0 else 0
        percent_devolvidas_3min = (devolvidas_3min / len(df_devolucoes)) * 100 if len(df_devolucoes) > 0 else 0
    else:
        tempo_medio_devolucao = "N/A"
        percent_devolvidas_15min = 0
        percent_devolvidas_3min = 0

    # 4. Unreturned missed calls
    origins = set(outras_chamadas['Origem_limpo'].dropna())
    destinations = set(chamadas_efetuadas['Destino_limpo'].dropna())
    nao_devolvidas = len(origins - destinations)

    # 5. Fast answered calls (<60s)
    rapidas_60s = df[
        (df['Tempo de Toque'].notna()) & 
        (df['Tempo de Toque'] <= timedelta(seconds=60))
    ].shape[0]
    percent_rapidas_60s = (rapidas_60s / chamadas_atendidas) * 100 if chamadas_atendidas > 0 else 0

    # ========================
    # VISUALIZATIONS
    # ========================

    # 1. Call volume by day of week
    df['Dia da Semana'] = df['Data de Início'].dt.day_name()
    dias = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    plt.figure(figsize=(12, 6))
    sns.countplot(data=df, x='Dia da Semana', order=dias)
    plt.title('Volume de Chamadas por Dia da Semana', pad=20)
    plt.xlabel('Dia da Semana')
    plt.ylabel('Número de Chamadas')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('../output/chamadas_por_dia.png', dpi=300)
    plt.close()

    # 2. Call status distribution
    status_counts = df['Atendida'].value_counts()
    plt.figure(figsize=(8, 8))
    plt.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%',
            startangle=90, wedgeprops={'width': 0.4})
    plt.title('Distribuição do Status das Chamadas', pad=20)
    plt.savefig('../output/distribuicao_status.png', dpi=300)
    plt.close()

    # 3. Call heatmap by hour and day
    heatmap_data = df.groupby([
        df['Data de Início'].dt.day_name().map({d: i for i, d in enumerate(dias)}).sort_values(),
        df['Data de Início'].dt.hour
    ]).size().unstack()

    plt.figure(figsize=(14, 8))
    sns.heatmap(heatmap_data, cmap='YlOrRd', annot=True, fmt='.0f',
            cbar_kws={'label': 'Número de Chamadas'})

    plt.title('Afluência de Chamadas por Hora e Dia', pad=20)
    plt.xlabel('Hora do Dia')
    plt.ylabel('Dia da Semana')
    plt.yticks(ticks=range(7), labels=dias, rotation=0)
    plt.tight_layout()
    plt.savefig('../output/heatmap_afluencia.png', dpi=300)
    plt.close()

    # ========================
    # PRINT METRICS REPORT
    # ========================

    print("📊 RELATÓRIO DE MÉTRICAS DE CHAMADAS")
    print("="*50)
    print(f"📞 Total de chamadas: {total_chamadas}")
    print(f"📱 Números únicos: {len(set(df['Origem_limpo'].dropna()))}")
    print(f"✅ Chamadas atendidas: {chamadas_atendidas} ({percent_atendidas:.1f}%)")
    print(f"🎯 Atendidas à primeira tentativa: {primeira_tentativa}")
    print(f"⏱ Chamadas atendidas em <60s: {rapidas_60s} ({percent_rapidas_60s:.1f}%)")
    print("\n🔁 MÉTRICAS DE DEVOLUÇÃO:")
    print(f"• Tempo médio para devolução: {tempo_medio_devolucao}")
    print(f"• Devolvidas em ≤15min: {devolvidas_15min} ({percent_devolvidas_15min:.1f}%)")
    print(f"• Devolvidas em ≤3min: {devolvidas_3min} ({percent_devolvidas_3min:.1f}%)")
    print(f"• Chamadas não atendidas e não devolvidas: {nao_devolvidas}")
    print("\n📊 Visualizações salvas em:")
    print("- ../output/chamadas_por_dia.png")
    print("- ../output/distribuicao_status.png")
    print("- ../output/heatmap_afluencia.png")

except FileNotFoundError:
    print("❌ Erro: Arquivo de dados não encontrado")
except Exception as e:
    print(f"❌ Erro inesperado: {str(e)}")