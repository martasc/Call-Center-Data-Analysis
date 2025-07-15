from datetime import timedelta
import pandas as pd

def count_calls_within_one_hour(group):
   # group['Data de Início'] = pd.to_datetime(group['Data de Início'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    group['Data de Início'] = pd.to_datetime(
    group['Data de Início'],
    format='%d/%m/%y %H:%M',
    errors='coerce'
)

    group = group.sort_values('Data de Início').reset_index(drop=True)
    group['Total Chamadas'] = pd.NA

    filtered_group = group[group['Tipo'].str.strip().str.lower() != 'chamada efetuada']

    for idx in filtered_group.index:
        call_time = group.loc[idx, 'Data de Início']

        janela_inicio = call_time - timedelta(hours=1)
        same_origin_mask = (
            (group['Origem'] == group.loc[idx, 'Origem']) &
            (group['Tipo'].str.strip().str.lower() != 'chamada efetuada') &
            (group['Data de Início'] >= janela_inicio) &
            (group['Data de Início'] <= call_time)
        )

        total_chamadas = same_origin_mask.sum()

        if total_chamadas > 0:
            group.loc[idx, 'Total Chamadas'] = total_chamadas

    return group
