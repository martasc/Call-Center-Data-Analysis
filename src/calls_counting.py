from datetime import timedelta
import pandas as pd

def count_calls_within_one_hour(group):
    group = group.sort_values('Data de Início', ascending=False).reset_index(drop=True)
    group['Total Chamadas'] = pd.NA
    counted_indices = set()

    for idx in group.index:
        if idx in counted_indices:
            continue

        call_type = group.loc[idx, 'Tipo']
        call_time = group.loc[idx, 'Data de Início']

        if call_type == "Chamada recebida":
            group.loc[idx, 'Total Chamadas'] = 1
            counted_indices.add(idx)
            continue

        mask_window = (
            (group['Data de Início'] <= call_time) & 
            (group['Data de Início'] >= call_time - timedelta(hours=1))
        ) & (~group.index.isin(counted_indices)) & (group["Tipo"] != "Chamada recebida")

        one_hour_window = group.loc[mask_window]

        if len(one_hour_window) == 1:
            group.loc[idx, 'Total Chamadas'] = 1
            counted_indices.add(idx)
        elif len(one_hour_window) > 1:
            group.loc[one_hour_window.index[0], 'Total Chamadas'] = len(one_hour_window)
            counted_indices.update(one_hour_window.index)

    return group
