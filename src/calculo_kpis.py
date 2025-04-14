import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import chardet
import os

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

def main():
    try:
        # Check if file exists
        file_path = '../output/clean_data.csv'
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at {file_path}")

        # Detect encoding
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read(10000))
        
        print(f"Codificação detectada: {result['encoding']} com confiança {result['confidence']}")

        # Load data with detected encoding
        df = pd.read_csv(file_path, encoding=result['encoding'], sep=';')

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

        # Rest of your metrics calculations...
        # [Keep all your existing metrics code here]

        # ========================
        # VISUALIZATIONS
        # ========================

        # Create output directory if it doesn't exist
        os.makedirs('../output', exist_ok=True)

        # [Keep all your visualization code here]

        # ========================
        # PRINT METRICS REPORT
        # ========================
        print("📊 RELATÓRIO DE MÉTRICAS DE CHAMADAS")
        print("="*50)
        # [Keep all your print statements here]

    except FileNotFoundError as e:
        print(f"❌ Erro: {str(e)}")
    except pd.errors.EmptyDataError:
        print("❌ Erro: O arquivo CSV está vazio ou malformado")
    except Exception as e:
        print(f"❌ Erro inesperado: {str(e)}")
        print(f"Tipo do erro: {type(e).__name__}")

if __name__ == "__main__":
    main()