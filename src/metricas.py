import re
import pandas as pd

# Caminho para o ficheiro processado anteriormente
INPUT_FILE = "../output/recebidas.csv"
DEVOLVIDAS_FILE = "../output/devolvidas.csv"


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
def normalizar_numero(numero):
    numero_digits = re.sub(r"\D", "", str(numero))  # mantém só dígitos
    return numero_digits[-9:]  # usa apenas os últimos 9 dígitos

def analisar_chamadas(input_file=INPUT_FILE):
    try:
        df = pd.read_csv(input_file, delimiter=";")
        df_devolvidas = pd.read_csv(DEVOLVIDAS_FILE, delimiter=";")

        if "Tipo" not in df.columns:
            print("Coluna 'Tipo' não encontrada.")
            return

        # Filtra chamadas recebidas
        df_recebidas = df[df["Tipo"].str.strip().str.lower() == "chamada recebida"].copy()
        df_nao_recebidas = df[df["Tipo"].str.strip().str.lower() == "chamada não atendida"].copy()


        if df_recebidas.empty:
            print("⚠️ Nenhuma chamada recebida encontrada.")
            return

        # Converte colunas de tempo para segundos
        df_recebidas.loc[:, "Tempo de Espera (s)"] = df_recebidas["Tempo de Toque"].apply(parse_tempo)
        df_recebidas.loc[:, "Duração (s)"] = df_recebidas["Duração"].apply(parse_tempo)


        # Cálculos
        total_chamadas = len(df)
        total_chamadas_atendidas = len(df_recebidas)
        percentagem_atendidas = (total_chamadas_atendidas / total_chamadas * 100) if total_chamadas > 0 else 0
        chamadas_rapidas = (df_recebidas["Tempo de Espera (s)"] < 60).sum()
        perc_rapidas = (chamadas_rapidas / total_chamadas_atendidas * 100) if total_chamadas_atendidas > 0 else 0
        duracao_media = df_recebidas["Duração (s)"].mean()
        tempo_medio_espera = df_recebidas["Tempo de Espera (s)"].mean()
        total_chamadas_nao_atendidas = len(df_nao_recebidas)
        percentagem_nao_atendidas = (total_chamadas_nao_atendidas / total_chamadas * 100) if total_chamadas > 0 else 0

        atendidas_primeira_tentativa = df_recebidas["Total Chamadas"].value_counts().get(1, 0)
        chamadas_atendidas_nrs_unicos = df_recebidas["Total Chamadas"].count()
        chamadas_nao_atendidas_nrs_unicos = df_nao_recebidas["Total Chamadas"].count()
        nr_medio_tentativas_atendidas = df_recebidas["Total Chamadas"].sum()/chamadas_atendidas_nrs_unicos if chamadas_atendidas_nrs_unicos > 0 else 0
        nr_medio_tentativas_nao_atendidas = df_nao_recebidas["Total Chamadas"].sum()/chamadas_nao_atendidas_nrs_unicos if chamadas_nao_atendidas_nrs_unicos > 0 else 0
                       
        chamadas_devolvidas = len(df_devolvidas) 
        #####AQUI
        #confirmar que coluna existe
        perc_ate_3min = (df_devolvidas["Tempo até Devolução (s)"] <= 180).mean() * 100 if not df_devolvidas.empty else 0
        perc_ate_15min = (df_devolvidas["Tempo até Devolução (s)"] <= 900).mean() * 100 if not df_devolvidas.empty else 0

        # Calcular chamadas não devolvidas (nrs únicos)
        chamadas_nao_devolvidas_unicas = 0

        if not df_nao_recebidas.empty:
            # Normaliza números
            
            df_nao_recebidas["Origem_norm"] = df_nao_recebidas["Origem"].astype(str).str.strip()
            
            if not df_devolvidas.empty:
                df_devolvidas["Destino_norm"] = df_devolvidas["Destino"].astype(str).str.strip()
                df_devolvidas["Destino_norm"] = df_devolvidas["Destino Final"].apply(normalizar_numero)
                df_nao_recebidas["Origem_norm"] = df_nao_recebidas["Origem"].apply(normalizar_numero)

                # Números de clientes que ligaram e não foram atendidos
                numeros_nao_atendidos = df_nao_recebidas["Origem_norm"].unique()

                # Números para os quais a empresa ligou depois
                numeros_devolvidos = df_devolvidas["Destino_norm"].unique()

                # Diferença entre eles = não devolvidos
                numeros_nao_devolvidos = set(numeros_nao_atendidos) - set(numeros_devolvidos)
                chamadas_nao_devolvidas_unicas = len(numeros_nao_devolvidos)

                print("Números de chamadas não atendidas e não devolvidas:")
                for numero in sorted(numeros_nao_devolvidos):
                    print(f"- {numero}")
            else:
                print("Nenhuma chamada devolvida encontrada. Todos os não atendidos são considerados não devolvidos.")
                chamadas_nao_devolvidas_unicas = df_nao_recebidas["Origem_norm"].nunique()
                for numero in sorted(df_nao_recebidas["Origem_norm"].unique()):
                    print(f"- {numero}")

        df_recebidas = df_recebidas.copy()
        df_nao_recebidas = df_nao_recebidas.copy()
        df_devolvidas = df_devolvidas.copy()

        print()

        print("📊 Estatísticas das chamadas recebidas:")
        print(f"- Total de chamadas: {total_chamadas}")
        print(f"- Total de chamadas atendidas: {total_chamadas_atendidas}")
        print(f"- Total de chamadas não atendidas: {total_chamadas_nao_atendidas}")
        print(f"- Chamadas com tempo de espera < 60s: {chamadas_rapidas}")
        print(f"- Tempo médio de espera - atendidas: {tempo_medio_espera} segundos")
        print(f"- Duração média das chamadas: {duracao_media:.1f} segundos")



        print(f"Total de Chamadas (nrs únicos): {chamadas_atendidas_nrs_unicos}")
    
        print("Chamadas Atendidas")
        print("------------------")
        print(f"Total Chamadas Atendidas: {total_chamadas_atendidas}")
        print(f"% Chamadas Atendidas: {round(percentagem_atendidas)}%")
        print(f"Chamadas com tempo de espera <= 60s: {chamadas_rapidas}")
        print(f"% Chamadas com tempo de espera <= 60s: {round(perc_rapidas, 2)}%")
        print(f"Chamadas atendidas à primeira tentativa: {atendidas_primeira_tentativa}")
        print(f"Número médio de tentativas (atendidas): {round(nr_medio_tentativas_atendidas, 1) if nr_medio_tentativas_atendidas is not None else 'N/A'}")
        print(f"Tempo médio de espera (s): {tempo_medio_espera}s")
        print(f"Duração média das chamadas(atendidas): {duracao_media}\n")

        print("Chamadas Não Atendidas")
        print("------------------")
        print(f"Total de Chamadas não atendida: {total_chamadas_nao_atendidas}")
        print(f"% não atendidas: {round(percentagem_nao_atendidas)}%")

        print(f"Número médio de tentativas (não atendidas): {round(nr_medio_tentativas_nao_atendidas, 1) if nr_medio_tentativas_nao_atendidas is not None else 'N/A'}\n")


        print("Chamadas Devolvidas")
        print("------------------")
        print(f"Total de Chamadas devolvidas: {chamadas_devolvidas}")
        print(f"% Devolvidas até 3min: {round(perc_ate_3min, 2)}%")
        print(f"% Devolvidas até 15min: {round(perc_ate_15min, 2)}%")
    #     print(f"% devolvidas sobre chamadas não atendidas (nrs únicos): {round(percentagem_devolvidas_sobre_nao_atendidas, 2)}%")
    # # print(f"% devolvidas sobre chamadas não atendidas (nrs únicos - nunca atendidos): {round(percentagem_devolvidas_sobre_nao_atendidas_corrigida, 2)}%")

    #     if chamadas_devolvidas > 0 and 'Duração' in df_devolvidas.columns and pd.notna(duracao_media_devolvidas):
    #         minutos = int(duracao_media_devolvidas.total_seconds() // 60)
    #         segundos = int(duracao_media_devolvidas.total_seconds() % 60)
    #         print(f"Duração média das chamadas (devolvidas): {minutos}min e {segundos}s")
    #     else:
    #         print("Duração média das chamadas (devolvidas): N/A")
    #     print(f"Tempo médio entre não atendida e devolvida: {tempo_medio_formatado if tempo_medio_formatado is not None else 'N/A'}\n")



        print("Chamadas Não Devolvidas")
        print("------------------")
        print(f"Chamadas não atendidas e não devolvidas (nrs únicos): {chamadas_nao_devolvidas_unicas}")


    except Exception as e:
        print(f"❌ Erro ao analisar chamadas: {e}")

if __name__ == "__main__":
    analisar_chamadas()
