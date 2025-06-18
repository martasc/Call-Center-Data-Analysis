import pandas as pd

# Caminho para o ficheiro processado anteriormente
INPUT_FILE = "../output/clean_data.csv"

def parse_tempo(tempo_str):
    """Converte tempo no formato HH:MM:SS ou MM:SS para segundos"""
    try:
        parts = list(map(int, tempo_str.strip().split(":")))
        if len(parts) == 3:
            h, m, s = parts
        elif len(parts) == 2:
            h = 0
            m, s = parts
        else:
            return 0
        return h * 3600 + m * 60 + s
    except:
        return 0

def analisar_chamadas(input_file=INPUT_FILE):
    try:
        df = pd.read_csv(input_file, delimiter=";")

        if "Tipo" not in df.columns:
            print("❌ Coluna 'Tipo' não encontrada.")
            return

        # Filtra chamadas recebidas
        df_recebidas = df[df["Tipo"].str.strip().str.lower() == "chamada recebida"]
        df_nao_recebidas = df[df["Tipo"].str.strip().str.lower() == "chamada não atendida"]


        if df_recebidas.empty:
            print("⚠️ Nenhuma chamada recebida encontrada.")
            return

        # Converte colunas de tempo para segundos
        df_recebidas["Tempo de Espera (s)"] = df_recebidas["Tempo de Toque"].apply(parse_tempo)
        df_recebidas["Duração (s)"] = df_recebidas["Duração"].apply(parse_tempo)

        # Cálculos
        total_chamadas = len(df)
        total_chamadas_atendidas = len(df_recebidas)
        chamadas_rapidas = (df_recebidas["Tempo de Espera (s)"] < 60).sum()
        duracao_media = df_recebidas["Duração (s)"].mean()
        tempo_medio_espera = df_recebidas["Tempo de Espera (s)"].mean()
        total_chamadas_nao_atendidas = len(df_nao_recebidas)
                            
        print("📊 Estatísticas das chamadas recebidas:")
        print(f"- Total de chamadas: {total_chamadas}")
        print(f"- Total de chamadas atendidas: {total_chamadas_atendidas}")
        print(f"- Total de chamadas não atendidas: {total_chamadas_nao_atendidas}")
        print(f"- Chamadas com tempo de espera < 60s: {chamadas_rapidas}")
        print(f"- Tempo médio de espera - atendidas: {tempo_medio_espera} segundos")
        print(f"- Duração média das chamadas: {duracao_media:.1f} segundos")

    except Exception as e:
        print(f"❌ Erro ao analisar chamadas: {e}")

if __name__ == "__main__":
    analisar_chamadas()
