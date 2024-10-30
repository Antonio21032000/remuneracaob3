import pandas as pd
import csv
from datetime import datetime
import yfinance as yf
from fuzzywuzzy import process



def analisar_csv(caminho_arquivo):
    try:
        df = pd.read_csv(caminho_arquivo, encoding='latin1', sep=';', on_bad_lines='skip')
        print("Arquivo lido com sucesso usando pandas.")
        print(f"Dimensões do DataFrame: {df.shape}")
        return df
    except Exception as e:
        print(f"Erro ao ler com pandas: {str(e)}")
        return None

# Uso do script
caminho_arquivo = r'M:\VS Code\remtotal.csv'
df_resultado = analisar_csv(caminho_arquivo)

if df_resultado is not None:
    df_resultado = df_resultado.drop(columns=['CNPJ_Companhia','Data_Referencia','Versao','ID_Documento','Salario','Beneficios_Diretos_Indiretos','Participacoes_Comites','Outros_Valores_Fixos','Outros_Valores_Fixos','Descricao_Outros_Remuneracoes_Fixas','Bonus','Participacao_Resultados','Outros_Valores_Variaveis','Comissoes','Descricao_Outros_Remuneracoes_Variaveis','Pos_emprego','Cessacao_Cargo','Baseada_Acoes','Observacao,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,','Participacao_Reunioes'])

    # Função para converter para float, preservando o valor original se falhar
    def converter_para_float(valor):
        if pd.isna(valor):
            return valor
        try:
            return float(str(valor).replace(',', '.'))
        except:
            return valor

    # Aplicando a conversão
    df_resultado['Total_Remuneracao'] = df_resultado['Total_Remuneracao'].apply(converter_para_float)
    df_resultado['Total_Remuneracao_Orgao'] = df_resultado['Total_Remuneracao_Orgao'].apply(converter_para_float)
    df_resultado['Numero_Membros'] = df_resultado['Numero_Membros'].apply(converter_para_float)
    df_resultado['Numero_Membros_Remunerados'] = df_resultado['Numero_Membros_Remunerados'].apply(converter_para_float)


    # Convertendo as datas
    def converter_data(data):
        try:
            return pd.to_datetime(data)
        except:
            return pd.NaT

    df_resultado['Data_Inicio_Exercicio_Social'] = df_resultado['Data_Inicio_Exercicio_Social'].apply(converter_data)
    df_resultado['Data_Fim_Exercicio_Social'] = df_resultado['Data_Fim_Exercicio_Social'].apply(converter_data)

    df_resultado.drop (columns=['Numero_Membros'], inplace =True)
    
    #df_resultado ["Remuneração média por órgão"] = df_resultado ["Total_Remuneracao_Orgao"].apply (converter_para_float) / df_resultado ["Numero_Membros_Remunerados"]
    

    print(df_resultado.head())
    print(df_resultado.info())
else:
    print("Não foi possível processar o arquivo.")


df_resultado = df_resultado [df_resultado ["Data_Inicio_Exercicio_Social"]=="2024-01-01"]


df_resultado ['Total_Remuneracao'] = df_resultado ['Total_Remuneracao']

# ordena por total_remuneracao
df_resultado = df_resultado.sort_values(by='Total_Remuneracao', ascending=False)

# quero uma coluna de Total_Membros_Remunerados que soma o Numero_Membros_Remunerados de cada Nome_Companhia
df_resultado ['Total_Membros_Remunerados'] = df_resultado.groupby('Nome_Companhia')['Numero_Membros_Remunerados'].transform('sum')

# 1. Primeiro, vamos criar um novo dataframe só com remuneração por órgão
df_orgaos = df_resultado.copy()

# 2. Criar as novas colunas com base nos órgãos
df_novo = pd.DataFrame()

# 3. Para cada empresa, pegar os valores de cada órgão
for empresa in df_resultado['Nome_Companhia'].unique():
    # Pegar os dados apenas dessa empresa
    dados_empresa = df_resultado[df_resultado['Nome_Companhia'] == empresa]
    
    # Criar uma linha nova com todos os dados
    nova_linha = {
        'Nome_Companhia': empresa,
        'Data_Inicio_Exercicio_Social': dados_empresa['Data_Inicio_Exercicio_Social'].iloc[0],
        'Data_Fim_Exercicio_Social': dados_empresa['Data_Fim_Exercicio_Social'].iloc[0],
        'Total_Remuneracao': dados_empresa['Total_Remuneracao'].iloc[0],
        
        # Remuneração por órgão
        'Remuneracao_Conselho_Fiscal': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']['Total_Remuneracao_Orgao'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']) > 0 else 0,
        'Remuneracao_Diretoria': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']['Total_Remuneracao_Orgao'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']) > 0 else 0,
        'Remuneracao_Conselho_Adm': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']['Total_Remuneracao_Orgao'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']) > 0 else 0,
        
        # Número de membros por órgão
        'Membros_Conselho_Fiscal': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']['Numero_Membros_Remunerados'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']) > 0 else 0,
        'Membros_Diretoria': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']['Numero_Membros_Remunerados'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']) > 0 else 0,
        'Membros_Conselho_Adm': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']['Numero_Membros_Remunerados'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']) > 0 else 0
    }
    
    # Adicionar a linha ao novo dataframe
    df_novo = pd.concat([df_novo, pd.DataFrame([nova_linha])], ignore_index=True)

# 4. Ordenar por remuneração total
df_novo = df_novo.sort_values('Total_Remuneracao', ascending=False)


# criar coluna de total de membros remunerados
df_novo ['Total_Membros_Remunerados'] = df_novo ['Membros_Conselho_Fiscal'] + df_novo ['Membros_Diretoria'] + df_novo ['Membros_Conselho_Adm']

# criar coluna de remuneração média por órgão
df_novo ["Remuneração média da Companhia"] = df_novo ["Total_Remuneracao"] / df_novo ["Total_Membros_Remunerados"]

# criar coluna de remuneracao media por diretoria
df_novo ["Remuneração média da Diretoria"] = df_novo ["Remuneracao_Diretoria"] / df_novo ["Membros_Diretoria"]

# criar coluna de remuneracao media por conselho de adm
df_novo ["Remuneração média do Conselho de Administração"] = df_novo ["Remuneracao_Conselho_Adm"] / df_novo ["Membros_Conselho_Adm"]

# deleta qualquer linha que tenha "Total_Remuneracao" igual a 0
df_novo = df_novo[df_novo['Total_Remuneracao'] != 0]
#deleta qualquer linha que Total_Remuneracao nao tenha valor 
df_novo = df_novo[df_novo['Total_Remuneracao'].notna()]

# deleta qualquer linha que Total_Remuneracao tenha valor abaixo de 1000000
df_novo = df_novo[df_novo['Total_Remuneracao'] >= 1000000]

# deleta as linhas com os seguintes nomes na Nome_Companhia

df_bbg = pd.read_excel(r'M:\VS Code\dadosbbg.xlsm', usecols=['NM_TICKER', 'DATA', 'NM_FIELD', 'VL_FIELD'], sheet_name='netinc') # quero especificar as colunas que eh para pegar
# gostaria que 12M_NET_INC seja coluna com seus respectivos valores
#df_bbg = df_bbg.pivot(index='DATA', columns='NM_TICKER', values='VL_FIELD')

df_bbg.rename(columns={'VL_FIELD': 'Net Income LTM'}, inplace=True)
df_bbg.drop(columns=['NM_FIELD'], inplace=True)


# importa o auxiliarbbg
df_auxiliarbbg = pd.read_excel(r'M:\VS Code\auxiliarbbg.xlsx')
df_auxiliarbbg.rename(columns={'Ticker': 'NM_TICKER'}, inplace=True)

#pega o Nome_Companhia do auxiliarbbg e coloca no df_bgg fazendo um merge
df_bbg = pd.merge(df_bbg, df_auxiliarbbg, on='NM_TICKER', how='left')

# faz um merger do df_novo com o df_bbg usando o Nome_Companhia e puxando apenas o Net Income LTM
df_novo = pd.merge(df_novo, df_bbg[['Nome_Companhia', 'Net Income LTM', 'NM_TICKER']], on='Nome_Companhia', how='left')


df_novo ["Net Income LTM"] = df_novo ["Net Income LTM"] *1000000

# criar coluna de  % da remuneração total da companhia sobre o Net Income LTM
df_novo ["% da Remuneração Total sobre o Net Income LTM"] = df_novo ["Total_Remuneracao"] / df_novo ["Net Income LTM"]


# importando a 2a aba do df_bbg
df_bbg2 = pd.read_excel('dadosbbg.xlsm', sheet_name='Sheet2', usecols=['NM_TICKER', 'DATA', 'NM_FIELD', 'VL_FIELD'])

df_bbg2.rename(columns={'VL_FIELD': 'EBITDA'}, inplace=True)
df_bbg2.drop(columns=['NM_FIELD'], inplace=True)

# faz merger com o df_auxiliarbbg
df_bbg2 = pd.merge(df_bbg2, df_auxiliarbbg, on='NM_TICKER', how='left')

# faz merge com o df_novo usando o Nome_Companhia e traz o EBITDA
df_novo = pd.merge(df_novo, df_bbg2[['Nome_Companhia', 'EBITDA']], on='Nome_Companhia', how='left')

df_novo ["EBITDA"] = df_novo ["EBITDA"] * 1000000


# criar coluna de % da remuneração total sobre o EBITDA
df_novo ["% da Remuneração Total sobre o EBITDA"] = df_novo ["Total_Remuneracao"] / df_novo ["EBITDA"]


# DF_BBG3 puxando Sheet3
df_bbg3 = pd.read_excel('dadosbbg.xlsm', sheet_name='Sheet3', usecols=['NM_TICKER', 'DATA', 'NM_FIELD', 'VL_FIELD'])

# deleta a coluna NM_FIELD
df_bbg3.drop(columns=['NM_FIELD'], inplace=True)

# renomeia a coluna VL_FIELD para Market Cap
df_bbg3.rename(columns={'VL_FIELD': 'Market Cap'}, inplace=True)

# faz merge com df_auxiliarbbg
df_bbg3 = pd.merge(df_bbg3, df_auxiliarbbg, on='NM_TICKER', how='left')

# faz merge com df_novo usando o Nome_Companhia e traz o Market Cap
df_novo = pd.merge(df_novo, df_bbg3[['Nome_Companhia', 'Market Cap']], on='Nome_Companhia', how='left')

df_novo ["Market Cap"] = df_novo ["Market Cap"] * 1000000

# criar coluna de % da remuneração total sobre o Market Cap
df_novo ["% da Remuneração Total sobre o Market Cap"] = df_novo ["Total_Remuneracao"] / df_novo ["Market Cap"]

# ordenar por % da remuneração total sobre o Market Cap
df_novo = df_novo.sort_values('% da Remuneração Total sobre o Market Cap', ascending=False)

#quero deixar sem empresas repetidas em Nome_Companhia
df_novo = df_novo.drop_duplicates(subset='Nome_Companhia')



df_novo.to_excel('remtotal2024_novo.xlsx', index=False)


print(df_novo.head())
print(df_novo.info())

print(df_bbg.head())
print(df_bbg.info())

print(df_bbg2.head())
print(df_bbg2.info())

print(df_bbg3.head())
print(df_bbg3.info())