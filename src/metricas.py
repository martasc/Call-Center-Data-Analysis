import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

# Configuração do dashboard
plt.style.use('default')
plt.rcParams['figure.facecolor'] = '#f8f9fa'  # Fundo cinza claro

# Dados das métricas
metricas = [
    {'titulo': 'Total de Chamadas', 'valor': 43},
    {'titulo': 'Números Únicos', 'valor': 14},
    {'titulo': 'Chamadas Atendidas', 'valor': '6 (16,6%)'},
    {'titulo': 'Chamada Atendidas à Primeira Tentativa', 'valor': 5}
]

# Criar figura
fig = plt.figure(figsize=(10, 4), facecolor='#f8f9fa')
fig.suptitle('Dashboard de Métricas de Chamadas', fontsize=14, y=1.05, color='#333333', fontweight='bold')

# Criar cards quadrados
for i, metrica in enumerate(metricas):
    ax = fig.add_subplot(1, 4, i+1, aspect='equal')
    
    # Remover bordas e eixos
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    
    # Adicionar card branco com borda verde e cantos arredondados
    retangulo = FancyBboxPatch((0.05, 0.05), 0.9, 0.9,
                             transform=ax.transAxes,
                             facecolor='white',
                             edgecolor='#2e8b57',  # Verde floresta
                             linewidth=2.5,
                             boxstyle='round,pad=0.2')
    ax.add_patch(retangulo)
    
    # Adicionar título (quebra em 2 linhas se necessário)
    titulo = metrica['titulo'].replace(' ', '\n') if len(metrica['titulo']) > 12 else metrica['titulo']
    ax.text(0.5, 0.65, titulo, fontsize=11, ha='center', va='center', 
           transform=ax.transAxes, color='#333333', fontweight='bold')
    
    # Adicionar valor
    ax.text(0.5, 0.35, str(metrica['valor']), fontsize=18, ha='center', 
           va='center', transform=ax.transAxes, color='#2e8b57', fontweight='bold')

# Ajustar layout
plt.tight_layout(pad=2.0)

# Salvar e mostrar
plt.savefig('dashboard_metricas.png', dpi=300, bbox_inches='tight', facecolor=fig.get_facecolor())
plt.show()