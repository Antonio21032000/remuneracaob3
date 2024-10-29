import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime

# Configuração básica do Streamlit
st.set_page_config(layout="wide", page_title="Análise de Remuneração")

# Título principal
st.title("Análise de Remuneração de Executivos")

try:
    # Leitura dos arquivos
    @st.cache_data
    def carregar_dados():
        # Arquivo principal
        df_resultado = pd.read_csv('remtotal.csv', encoding='latin1', sep=';', on_bad_lines='skip')
        
        # Dados Bloomberg
        df_bbg = pd.read_excel('dadosbbg.xlsm', sheet_name='netinc', 
                              usecols=['NM_TICKER', 'DATA', 'NM_FIELD', 'VL_FIELD'])
        df_bbg2 = pd.read_excel('dadosbbg.xlsm', sheet_name='Sheet2', 
                               usecols=['NM_TICKER', 'DATA', 'NM_FIELD', 'VL_FIELD'])
        df_bbg3 = pd.read_excel('dadosbbg.xlsm', sheet_name='Sheet3', 
                               usecols=['NM_TICKER', 'DATA', 'NM_FIELD', 'VL_FIELD'])
        df_auxiliarbbg = pd.read_excel('auxiliarbbg.xlsx')
        
        return df_resultado, df_bbg, df_bbg2, df_bbg3, df_auxiliarbbg

    # Carregamento dos dados
    df_resultado, df_bbg, df_bbg2, df_bbg3, df_auxiliarbbg = carregar_dados()

    # Processamento do DataFrame principal
    def processar_df_resultado(df):
        # Remoção de colunas desnecessárias
        colunas_remover = ['CNPJ_Companhia', 'Data_Referencia', 'Versao', 'ID_Documento', 
                          'Salario', 'Beneficios_Diretos_Indiretos', 'Participacoes_Comites', 
                          'Outros_Valores_Fixos', 'Descricao_Outros_Remuneracoes_Fixas', 'Bonus', 
                          'Participacao_Resultados', 'Outros_Valores_Variaveis', 'Comissoes', 
                          'Descricao_Outros_Remuneracoes_Variaveis', 'Pos_emprego', 
                          'Cessacao_Cargo', 'Baseada_Acoes', 'Observacao,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,',
                          'Participacao_Reunioes', 'Numero_Membros']
        df = df.drop(columns=colunas_remover)

        # Conversão de valores para float
        colunas_float = ['Total_Remuneracao', 'Total_Remuneracao_Orgao', 
                        'Numero_Membros_Remunerados']
        for col in colunas_float:
            df[col] = df[col].apply(lambda x: float(str(x).replace(',', '.')) 
                                   if pd.notna(x) else x)

        # Conversão de datas
        df['Data_Inicio_Exercicio_Social'] = pd.to_datetime(df['Data_Inicio_Exercicio_Social'])
        df['Data_Fim_Exercicio_Social'] = pd.to_datetime(df['Data_Fim_Exercicio_Social'])

        # Filtro por data
        df = df[df['Data_Inicio_Exercicio_Social'] == '2024-01-01']

        return df

    df_resultado = processar_df_resultado(df_resultado)

    # Criação do DataFrame final
    def criar_df_novo(df_resultado):
        df_novo = pd.DataFrame()
        
        for empresa in df_resultado['Nome_Companhia'].unique():
            dados_empresa = df_resultado[df_resultado['Nome_Companhia'] == empresa]
            
            nova_linha = {
                'Nome_Companhia': empresa,
                'Data_Inicio_Exercicio_Social': dados_empresa['Data_Inicio_Exercicio_Social'].iloc[0],
                'Data_Fim_Exercicio_Social': dados_empresa['Data_Fim_Exercicio_Social'].iloc[0],
                'Total_Remuneracao': dados_empresa['Total_Remuneracao'].iloc[0],
                
                'Remuneracao_Conselho_Fiscal': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']['Total_Remuneracao_Orgao'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']) > 0 else 0,
                'Remuneracao_Diretoria': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']['Total_Remuneracao_Orgao'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']) > 0 else 0,
                'Remuneracao_Conselho_Adm': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']['Total_Remuneracao_Orgao'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']) > 0 else 0,
                
                'Membros_Conselho_Fiscal': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']['Numero_Membros_Remunerados'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho Fiscal']) > 0 else 0,
                'Membros_Diretoria': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']['Numero_Membros_Remunerados'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Diretoria Estatutária']) > 0 else 0,
                'Membros_Conselho_Adm': dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']['Numero_Membros_Remunerados'].iloc[0] if len(dados_empresa[dados_empresa['Orgao_Administracao'] == 'Conselho de Administração']) > 0 else 0
            }
            
            df_novo = pd.concat([df_novo, pd.DataFrame([nova_linha])], ignore_index=True)
            
        return df_novo

    df_novo = criar_df_novo(df_resultado)

    # Processamento dos dados Bloomberg
    def processar_dados_bloomberg(df_novo, df_bbg, df_bbg2, df_bbg3, df_auxiliarbbg):
        # Preparação do DataFrame auxiliar
        df_auxiliarbbg = df_auxiliarbbg.rename(columns={'Ticker': 'NM_TICKER'})
        
        # Net Income
        df_bbg = df_bbg.rename(columns={'VL_FIELD': 'Net Income LTM'})
        df_bbg = df_bbg.drop(columns=['NM_FIELD'])
        df_bbg = pd.merge(df_bbg, df_auxiliarbbg, on='NM_TICKER', how='left')
        
        # EBITDA
        df_bbg2 = df_bbg2.rename(columns={'VL_FIELD': 'EBITDA'})
        df_bbg2 = df_bbg2.drop(columns=['NM_FIELD'])
        df_bbg2 = pd.merge(df_bbg2, df_auxiliarbbg, on='NM_TICKER', how='left')
        
        # Market Cap
        df_bbg3 = df_bbg3.rename(columns={'VL_FIELD': 'Market Cap'})
        df_bbg3 = df_bbg3.drop(columns=['NM_FIELD'])
        df_bbg3 = pd.merge(df_bbg3, df_auxiliarbbg, on='NM_TICKER', how='left')
        
        # Merges finais
        df_novo = pd.merge(df_novo, df_bbg[['Nome_Companhia', 'Net Income LTM', 'NM_TICKER']], 
                          on='Nome_Companhia', how='left')
        df_novo = pd.merge(df_novo, df_bbg2[['Nome_Companhia', 'EBITDA']], 
                          on='Nome_Companhia', how='left')
        df_novo = pd.merge(df_novo, df_bbg3[['Nome_Companhia', 'Market Cap']], 
                          on='Nome_Companhia', how='left')
        
        # Conversões e cálculos
        df_novo['Net Income LTM'] = df_novo['Net Income LTM'] * 1000000
        df_novo['EBITDA'] = df_novo['EBITDA'] * 1000000
        df_novo['Market Cap'] = df_novo['Market Cap'] * 1000000
        
        return df_novo

    df_novo = processar_dados_bloomberg(df_novo, df_bbg, df_bbg2, df_bbg3, df_auxiliarbbg)

    # Cálculos finais
    df_novo['Total_Membros_Remunerados'] = (df_novo['Membros_Conselho_Fiscal'] + 
                                           df_novo['Membros_Diretoria'] + 
                                           df_novo['Membros_Conselho_Adm'])
    
    df_novo['Remuneração média da Companhia'] = (df_novo['Total_Remuneracao'] / 
                                                df_novo['Total_Membros_Remunerados'])
    
    df_novo['Remuneração média da Diretoria'] = (df_novo['Remuneracao_Diretoria'] / 
                                                df_novo['Membros_Diretoria'])
    
    df_novo['Remuneração média do Conselho de Administração'] = (df_novo['Remuneracao_Conselho_Adm'] / 
                                                                df_novo['Membros_Conselho_Adm'])
    
    df_novo['% da Remuneração Total sobre o Net Income LTM'] = (df_novo['Total_Remuneracao'] / 
                                                               df_novo['Net Income LTM'])
    
    df_novo['% da Remuneração Total sobre o EBITDA'] = (df_novo['Total_Remuneracao'] / 
                                                       df_novo['EBITDA'])
    
    df_novo['% da Remuneração Total sobre o Market Cap'] = (df_novo['Total_Remuneracao'] / 
                                                           df_novo['Market Cap'])

    # Filtros finais
    df_novo = df_novo[df_novo['Total_Remuneracao'] >= 1000000]
    df_novo = df_novo[df_novo['Total_Remuneracao'].notna()]
    df_novo = df_novo.drop_duplicates(subset='Nome_Companhia')

    # Interface do Streamlit
    st.sidebar.header("Filtros")

    # Filtros
    company_filter = st.sidebar.multiselect(
        "Filtrar por Empresa",
        options=df_novo["Nome_Companhia"].unique(),
        default=[]
    )

    min_rem, max_rem = st.sidebar.slider(
        "Faixa de Remuneração Total (R$)",
        float(df_novo["Total_Remuneracao"].min()),
        float(df_novo["Total_Remuneracao"].max()),
        (float(df_novo["Total_Remuneracao"].min()), 
         float(df_novo["Total_Remuneracao"].max()))
    )

    # Aplicação dos filtros
    filtered_df = df_novo.copy()
    if company_filter:
        filtered_df = filtered_df[filtered_df["Nome_Companhia"].isin(company_filter)]
    filtered_df = filtered_df[
        (filtered_df["Total_Remuneracao"] >= min_rem) &
        (filtered_df["Total_Remuneracao"] <= max_rem)
    ]

    # Visualizações
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Empresas por Remuneração Total")
        fig1 = px.bar(
            filtered_df.head(10),
            x="Nome_Companhia",
            y="Total_Remuneracao",
            title="Top 10 - Remuneração Total"
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("Remuneração Média por Órgão")
        fig2 = px.bar(
            filtered_df.head(10),
            x="Nome_Companhia",
            y=["Remuneração média da Diretoria", 
               "Remuneração média do Conselho de Administração"],
            title="Comparativo de Remuneração Média por Órgão",
            barmode="group"
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Métricas principais
    col3, col4, col5 = st.columns(3)
    with col3:
        st.metric(
            "Média de Remuneração Total",
            f"R$ {filtered_df['Total_Remuneracao'].mean():,.2f}"
        )
    with col4:
        st.metric(
            "Média de Membros Remunerados",
            f"{filtered_df['Total_Membros_Remunerados'].mean():.1f}"
        )
    with col5:
        st.metric(
            "Número de Empresas",
            len(filtered_df)
        )

    # Tabela detalhada
    st.subheader("Dados Detalhados")
    
    columns_to_display = [
        "Nome_Companhia",
        "Total_Remuneracao",
        "Remuneração média da Companhia",
        "Remuneração média da Diretoria",
        "Remuneração média do Conselho de Administração",
        "Total_Membros_Remunerados",
        "% da Remuneração Total sobre o Net Income LTM",
        "% da Remuneração Total sobre o EBITDA",
        "% da Remuneração Total sobre o Market Cap"
    ]

formatted_df = filtered_df[columns_to_display].copy()

 # Formatação das colunas monetárias
    currency_columns = [
        "Total_Remuneracao",
        "Remuneração média da Companhia",
        "Remuneração média da Diretoria",
        "Remuneração média do Conselho de Administração"
    ]
    for col in currency_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: f"R$ {x:,.2f}")

    # Formatação das colunas percentuais
    percentage_columns = [
        "% da Remuneração Total sobre o Net Income LTM",
        "% da Remuneração Total sobre o EBITDA",
        "% da Remuneração Total sobre o Market Cap"
    ]
    for col in percentage_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.2%}")

    # Exibição da tabela formatada
    st.dataframe(formatted_df, use_container_width=True)

    # Botão de download
    csv = filtered_df[columns_to_display].to_csv(index=False)
    st.download_button(
        label="Download dados em CSV",
        data=csv,
        file_name="analise_remuneracao.csv",
        mime="text/csv"
    )

except Exception as e:
    st.error(f"Ocorreu um erro ao processar os dados: {str(e)}")
    st.write("Por favor, verifique se todos os arquivos necessários estão disponíveis e no formato correto.")


