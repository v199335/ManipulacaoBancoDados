# 1. Importação das bibliotecas necessárias
import pandas as pd
import matplotlib.pyplot as plt
import calplot

import pandas as pd

caminho_arquivo = "./flights.csv" 
df = pd.read_csv(caminho_arquivo, low_memory=False)

# --- ESTATÍSTICAS SUFICIENTES ---
# Inicialize as variáveis globais (se ainda não existirem)
total_voos = 0
total_atrasados = 0

def processar_bloco(df):
    #Processa um bloco de dados de voos para calcular o total de voos e voos atrasados.

    global total_voos
    global total_atrasados

    voos = len(df)
    atrasados = df['ARRIVAL_DELAY'].gt(10).sum() # Conta voos com atraso > 10, ignorando NaN

    total_voos += voos
    total_atrasados += atrasados

# Exemplo de uso (assumindo que 'df' já foi carregado)
processar_bloco(df)

# Para ver os resultados após chamar a função:
print(f"Total de voos processados: {total_voos}")
print(f"Total de voos atrasados: {total_atrasados}")

# 2. Definição da função getStats
def getStats(input_df, pos=None):

    # Lista das companhias aéreas de interesse
    cias_aereas_interesse = ["AA", "DL", "UA", "US"]
    
    # Colunas de interesse para a análise
    colunas_de_interesse = ["ARRIVAL_DELAY", "AIRLINE", "DAY", "MONTH", "YEAR"]

    # a. Filtra o DataFrame para conter apenas as observações das Cias. Aéreas de interesse.
    #    Em pandas, usamos o método .isin() para filtrar linhas com base em uma lista de valores.
    df_filtrado = input_df[input_df['AIRLINE'].isin(cias_aereas_interesse)]

    # b. Remove observações que tenham valores faltantes (NA) nos campos de interesse.
    #    O método .dropna() com um subconjunto de colunas remove as linhas com NA nessas colunas específicas.
    df_sem_na = df_filtrado.dropna(subset=colunas_de_interesse)

    # c. Agrupa o conjunto de dados resultante por dia, mês e cia. aérea.
    #    O método .groupby() é usado para agrupar os dados.
    agrupado = df_sem_na.groupby(['YEAR', 'MONTH', 'DAY', 'AIRLINE'])

    # d. Para cada grupo, determina as estatísticas suficientes.
    #    Utilizamos o método .agg() para aplicar múltiplas funções de agregação de uma só vez.
    #    - 'total_voos' é a contagem de observações em cada grupo (size).
    #    - 'voos_atrasados' é a soma condicional (ARRIVAL_DELAY > 10).
    estatisticas = agrupado.agg(
        total_voos=('ARRIVAL_DELAY', 'size'),
        voos_atrasados=('ARRIVAL_DELAY', lambda x: (x > 10).sum())
    ).reset_index() # .reset_index() transforma os agrupamentos de volta em colunas.

    return estatisticas

stats_result = getStats(df, 0)
print(stats_result.head())

# 3. Leitura do arquivo de forma fragmentada (chunked)
#    Especifique o caminho correto para o seu arquivo.
#    NOTA: O arquivo `flights.csv` original contém dados do ano de 2015.
#    A coluna 'YEAR' é importante para criar datas corretas.

# Define o tamanho do lote (chunk) para 100 mil registros
tamanho_lote = 100000

# Define as colunas que serão lidas do arquivo para otimizar o uso de memória.
# Isso equivale ao argumento `col_types` do R.
colunas_para_ler = ['YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ARRIVAL_DELAY']

# Lista para armazenar os resultados de cada lote processado
lista_de_resultados = []

# Cria um iterador para ler o arquivo CSV em lotes (chunks)
# O `TextFileReader` é um objeto iterável que retorna um DataFrame para cada lote.
with pd.read_csv(caminho_arquivo, chunksize=tamanho_lote, usecols=colunas_para_ler, encoding='utf-8') as reader:
    for chunk in reader:
        # Aplica a função `getStats` a cada lote
        stats_lote = getStats(chunk)
        # Adiciona o resultado do lote à lista de resultados
        lista_de_resultados.append(stats_lote)

# Combina todos os DataFrames da lista em um único DataFrame
estatisticas_brutas = pd.concat(lista_de_resultados, ignore_index=True)

# Como um mesmo dia/mês/cia pode aparecer em lotes diferentes,
# precisamos reagrupar os resultados combinados e somar as estatísticas.
estatisticas_finais = estatisticas_brutas.groupby(['YEAR', 'MONTH', 'DAY', 'AIRLINE']).sum().reset_index()

print(estatisticas_finais.head())

# 4. Definição da função computeStats
def computeStats(stats):
    
    # Cria uma cópia para evitar o SettingWithCopyWarning
    df = stats.copy()

    # Cria a coluna 'Data' a partir das colunas de ano, mês e dia.
    # pd.to_datetime é otimizado para essa conversão.
    df['Data'] = pd.to_datetime(df[['YEAR', 'MONTH', 'DAY']])
    
    # Calcula o percentual de atraso como um número real entre [0, 1]
    df['Perc'] = df['voos_atrasados'] / df['total_voos']
    
    # Renomeia a coluna 'AIRLINE' para 'Cia'
    df.rename(columns={'AIRLINE': 'Cia'}, inplace=True)
    
    # Seleciona e retorna apenas as colunas de interesse
    return df[['Cia', 'Data', 'Perc']]

# Aplica a função para obter o resultado final formatado
resultado_tidy = computeStats(estatisticas_finais)

# Exibe as primeiras linhas do resultado
print("Resultado Final Processado:")
print(resultado_tidy.head())
print("\n")


# 5. Produção do mapa de calor em formato de calendário

# A biblioteca `calplot` não usa um objeto de paleta separado como o ggplot2.
# A paleta de cores é definida diretamente na chamada da função de plotagem
# através do argumento `cmap`. Criaremos um `cmap` equivalente.

# 6. Definição da função baseCalendario
def baseCalendario(stats, cia):
   
    # a. Cria um subconjunto de dados para a Cia. Aérea especificada.
    dados_cia = stats[stats['Cia'] == cia].copy()

    # b. Para usar o calplot, precisamos de uma Series com a data como índice
    #    e o valor a ser plotado (percentual de atraso) como os dados.
    dados_para_plot = dados_cia.set_index('Data')['Perc']
    
    # c. Cria o gráfico de calendário.
    #    O `calplot.calplot` retorna os objetos Figure e Axes do matplotlib.
    #    A paleta de cores é definida com o argumento `cmap`.
    #    As cores equivalentes são encontradas em paletas como 'coolwarm_r' ou 'RdBu_r'.
    fig, ax = calplot.calplot(dados_para_plot, 
                              edgecolor='white', # Cor da borda dos dias
                              linewidth=2,       # Largura da borda
                              cmap='RdYlBu_r',   # Paleta de cores (Reversed Red-Yellow-Blue)
                              suptitle=f"Mapa de Calor de Atrasos - Cia. Aérea {cia}",
                              suptitle_kws={'y': 1.02}) # Ajusta a posição do título
    
    # O `calplot` já adiciona o título com `suptitle`, mas poderíamos usar o `ax` para mais customizações.
    # fig.suptitle(f"Mapa de Calor de Atrasos - Cia. Aérea {cia}", y=1.05, fontsize=16)

# 7. Execução e exibição dos gráficos para cada Cia. Aérea
cias_aereas = ["AA", "DL", "UA", "US"]
for cia_aerea in cias_aereas:
    print(f"Gerando gráfico para a companhia aérea: {cia_aerea}")
    baseCalendario(resultado_tidy, cia_aerea)

# Exibe todos os gráficos gerados
plt.show()