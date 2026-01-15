import pandas as pd
import streamlit as st
import numpy as np 
import os
from typing import List, Union 

st.set_page_config(
    page_title="Análise de Verbas",
    layout="wide",
    initial_sidebar_state="expanded"
)


DATA_FILE = 'dados_acompanhamento_verba.csv'
DATA_FILE_DEVOLUCAO = 'dados_acompanhamento_verba_devolucao.csv'


class DataFremeAggregator:
    
    def __init__(self, df: pd.DataFrame): 
        self.df = df


    def formatar_moeda(self, valor: float) -> str:

        if pd.isna(valor):
            return 'R$ 0,00'
        valor = float(valor)

        return f'R$ {valor:,.2f}'.replace(',', 'x').replace('.',',').replace('x','.')
    
    
    def formata_coluna_moeda(self, df: pd.DataFrame, colunas: list) -> pd.DataFrame:

        df_formatado = df.copy()
        for coluna in colunas:
            if coluna in df_formatado.columns:
                try:
                    numeric_series = pd.to_numeric(df_formatado[coluna].astype(str).str.replace('R$', '', regex=False).str.replace('.', ',', regex=False).str.replace(',', '.', regex=False), errors='coerce')
                    df_formatado[coluna] = numeric_series.apply(self.formatar_moeda)
                except Exception as e:
                    st.warning(f'Não foi possivel formatar a coluna {coluna} como moeda: {e}')
        return df_formatado

    def somar_coluna(self, coluna: str) -> float:

        if coluna not in self.df.columns:

            return 0.0
        
        try:
            soma = pd.to_numeric(self.df[coluna], errors='coerce').sum()
            return soma
        except Exception as e:
            st.error(f'A coluna {coluna} não pode ser convertida para um tipo numérico para soma: {e}')
            return 0.0
    
    
    def agrupar_somar(self, coluna_agrupamento: Union[str, List[str]], coluna_soma: str) -> pd.DataFrame:

        if self.df.empty:
            return pd.DataFrame()

        agrupamento_list = [coluna_agrupamento] if isinstance(coluna_agrupamento, str) else coluna_agrupamento
        
        # Colunas a serem verificadas no DataFrame
        colunas_verificar = agrupamento_list + [coluna_soma]

        # Validação de colunas
        if not all(col in self.df.columns for col in colunas_verificar):
            missing = [c for c in colunas_verificar if c not in self.df.columns]
            return pd.DataFrame()
        
        # Execução do agrupamento
        resultado_agregacao = (self.df
                             .groupby(agrupamento_list, dropna=False) # Adicionado dropna=False para incluir NaNs no agrupamento
                             [coluna_soma]
                             .sum()
                             .reset_index())
        
        resultado_agregacao.rename(columns={coluna_soma: f'Soma_de_{coluna_soma}'}, inplace=True)
        return resultado_agregacao
    
    #Não esta sendo ultilizado
    def filtrar_agrupar_somar(self, coluna_filtro: str, criterio: any, coluna_agrupamento: Union[str, List[str]], coluna_soma: str) -> pd.DataFrame:

        if self.df.empty:
            return pd.DataFrame()
            
        # Garante que coluna_agrupamento seja uma lista
        agrupamento_list = [coluna_agrupamento] if isinstance(coluna_agrupamento, str) else coluna_agrupamento
        
        # Colunas necessárias
        required_cols = [coluna_filtro, coluna_soma] + agrupamento_list
        
        # Validação de colunas
        if not all(col in self.df.columns for col in required_cols):
            return pd.DataFrame()
        
        # 1. Filtro
        df_filtrado = self.df[self.df[coluna_filtro] == criterio]

        if df_filtrado.empty: 
            st.warning(f'Atenção: Nenhum registro encontrado para o critério **"{criterio}"** na coluna: **{coluna_filtro}**')
            return pd.DataFrame()
        
        # 2. Agrupamento e Soma
        resultado_agregacao = (df_filtrado
                             .groupby(agrupamento_list)
                             [coluna_soma]
                             .sum()
                             .reset_index())

        resultado_agregacao.rename(columns={coluna_soma: f'Soma_condicional_de_{coluna_soma}'}, inplace=True)
        return resultado_agregacao


@st.cache_data
def load_data(file_path):
    try:
        df = pd.read_csv(file_path, sep=';', decimal=',')
        return df
    except FileNotFoundError:
        st.error(f"❌ ERRO FATAL: O arquivo '{file_path}' não foi encontrado em: {os.getcwd()}")
        return None
    except Exception as e:
        st.error(f"❌ ERRO FATAL ao carregar o arquivo: {e}")
        return None

def main():
    st.title("💸 Dashboard de Agregação de Verbas")

    df = load_data(DATA_FILE)
    df_dev = load_data(DATA_FILE_DEVOLUCAO)
    
    if df is None or df_dev is None:
        return 

    # --- 1. PREPARAÇÃO DE DADOS ---
    try:
        # Prepara df principal
        for col in ['VALORDEBITO', 'VALORCREDITO', 'VALOR_VERBA']:
              df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['VLR_RECEBER'] = df['VALORDEBITO'] + df['VALORCREDITO']
        
        # GARANTE A EXISTÊNCIA DA COLUNA ANO_EMISSAO
        df['ANO_EMISSAO'] = 0 
        
        # Extração do Ano da Data de Cadastro
        if 'DATACADASTRO' in df.columns:
            df['DATACADASTRO'] = pd.to_datetime(df['DATACADASTRO'], errors='coerce')
            df['ANO_EMISSAO'] = df['DATACADASTRO'].dt.year.fillna(0).astype(int)
        
        # Prepara df de devolução
        df_dev['VALOR_VERBA_DEVOLUCAO'] = pd.to_numeric(df_dev['VALOR_VERBA_DEVOLUCAO'], errors='coerce').fillna(0)
                
    except Exception as e:
        st.error(f"❌ ERRO AO PREPARAR DADOS: Não foi possível realizar conversões e cálculos iniciais. Erro: {e}")
        return 
    
    
    # --- 2. FILTRAGEM GLOBAL DE FILIAL (PRIMEIRO NÍVEL) ---
    
    filiais_unicas = sorted([f for f in df['CODIGOFILIAL'].unique() if not pd.isna(f) and f is not None])
    opcoes_filial = ['Todas', '1 a 7', '8 e 9'] + [str(int(f)) for f in filiais_unicas]
    grupos_filial = {
        '1 a 7': [f for f in filiais_unicas if f >= 1 and f <= 7],
        '8 e 9': [f for f in filiais_unicas if f == 8 or f == 9]
    }
    
    filial_selecionada = st.sidebar.selectbox(
        'Selecione a Filial ou Grupo de Filiais para análise:',
        opcoes_filial,
        key='filial_global_filter'
    )
    
    filiais_para_filtrar = []
    display_filial_receber = ""

    if filial_selecionada == 'Todas':
        filiais_para_filtrar = filiais_unicas
        display_filial_receber = 'Todas as Filiais'
    elif filial_selecionada in grupos_filial:
        filiais_para_filtrar = grupos_filial[filial_selecionada]
        display_filial_receber = f'Filiais {filial_selecionada}'
    else:
        try:
            filial_id = float(filial_selecionada)
            filiais_para_filtrar = [filial_id]
            display_filial_receber = f'Filial {filial_selecionada}'
        except ValueError:
            st.error("Seleção de Filial Inválida.")
            return

    # Filtra os DataFrames principais e de devolução pela Filial
    df_main_filtered_filial = df[df['CODIGOFILIAL'].isin(filiais_para_filtrar)].copy()
    df_dev_filtered_filial = df_dev[df_dev['FILIAL'].isin(filiais_para_filtrar)].copy()
    
    st.sidebar.markdown("---")
    
    
    # --- 3. FILTRAGEM GLOBAL DE CLASSIFICAÇÃO (SEGUNDO NÍVEL) ---
    coluna_classificacao = 'CLASSIFICACAO'
    classificacao_selecionada = 'Todos'
    
    if coluna_classificacao in df_main_filtered_filial.columns:
        # Obtém classificações únicas do DF já filtrado pela Filial
        # Preenche NaNs com um valor para que o filtro 'Todos' funcione corretamente, se for o caso
        df_main_filtered_filial[coluna_classificacao] = df_main_filtered_filial[coluna_classificacao].fillna('Não Classificado')
        
        classificacoes = df_main_filtered_filial[coluna_classificacao].unique().tolist()
        classificacoes = sorted([str(c) for c in classificacoes if c is not None])
        classificacoes.insert(0, 'Todos') 
        
        classificacao_selecionada = st.sidebar.selectbox(
            'Selecione a Classificação (Filtro Global):',
            classificacoes,
            key='classificacao_verba'
        )
    else:
        st.sidebar.warning(f"Coluna '{coluna_classificacao}' não encontrada no arquivo principal. O filtro será ignorado.")

    st.sidebar.markdown("---")


    # --- 4. FILTRAGEM GLOBAL DE ANO (TERCEIRO NÍVEL) ---
    coluna_ano = 'ANO_EMISSAO'
    ano_selecionado = 'Todos'
    default_index = 0 # Default para 'Todos'
    
    # Obtém anos únicos do DF já filtrado pela Filial e com Ano de Emissão calculado
    if coluna_ano in df_main_filtered_filial.columns:
        
        anos_disponiveis = df_main_filtered_filial[coluna_ano].unique().tolist()
        # Filtra para incluir apenas anos válidos (> 0, excluindo NaNs/datas inválidas)
        anos_validos = sorted([a for a in anos_disponiveis if a > 0], reverse=True) # Ordena do mais recente para o mais antigo
        
        if anos_validos:
            # Opções: 'Todos' + Anos (mais recente primeiro)
            opcoes_ano = ['Todos'] + [str(a) for a in anos_validos]
            
            # Seleciona o ano específico para filtrar
            ano_selecionado = st.sidebar.selectbox(
                'Filtrar por Ano Específico (Emissão):', 
                opcoes_ano,
                index=default_index, # Default: 'Todos'
                key='ano_filtro'
            )
        else:
            st.sidebar.warning("Nenhum registro com data de emissão válida (Ano > 0) encontrado para esta filial.")
    else:
        st.sidebar.warning(f"Coluna '{coluna_ano}' não encontrada ou sem dados válidos.")

    st.sidebar.markdown("---")

    # --- 5. APLICAÇÃO DOS FILTROS FINAIS (CLASSIFICAÇÃO E ANO) ---
    df_main_filtered_final = df_main_filtered_filial.copy()
    df_dev_filtered_final = df_dev_filtered_filial.copy()
    display_classificacao_receber = classificacao_selecionada
    display_ano_receber = ano_selecionado 
    
    # 5.1 Aplica filtro de Classificação
    if classificacao_selecionada != 'Todos':
        if coluna_classificacao in df_main_filtered_final.columns:
            df_main_filtered_final = df_main_filtered_final[df_main_filtered_final[coluna_classificacao] == classificacao_selecionada]
        
        # Aplica filtro ao DF de devolução (SE a coluna CLASSIFICACAO existir nele)
        if coluna_classificacao in df_dev_filtered_final.columns:
            df_dev_filtered_final[coluna_classificacao] = df_dev_filtered_final[coluna_classificacao].fillna('Não Classificado')
            df_dev_filtered_final = df_dev_filtered_final[df_dev_filtered_final[coluna_classificacao] == classificacao_selecionada]
    
    # 5.2 Aplica filtro de Ano (somente se não for 'Todos')
    if ano_selecionado != 'Todos' and coluna_ano in df_main_filtered_final.columns:
        try:
            ano_alvo = int(ano_selecionado) # Renomeado para 'ano_alvo'
            # Filtra o DF principal: apenas anos IGUAIS ao ano selecionado
            df_main_filtered_final = df_main_filtered_final[df_main_filtered_final[coluna_ano] == ano_alvo]
            
            # Atualiza o texto de exibição do ano para mostrar o ano específico
            display_ano_receber = str(ano_alvo) # Agora exibe o ano específico
            
            # NOTA: O filtro de ano não será aplicado ao df_dev, pois ele não possui a coluna 'ANO_EMISSAO' do arquivo principal.
            
        except ValueError:
            st.error("Erro ao converter ano selecionado para número.")


    # --- 6. RE-INICIALIZAÇÃO DOS AGREGADORES COM OS DATAFRAMES FILTRADOS GLOBALMENTE ---
    # Os DataFrames aqui já passaram pelos filtros de Filial, Classificação e Ano (o principal)
    aggregator = DataFremeAggregator(df_main_filtered_final)
    aggregator_dev = DataFremeAggregator(df_dev_filtered_final) # Este DF só está filtrado por Filial e Classificação

    st.sidebar.info(f"Analisando: **{display_filial_receber}**\n\nClassificação: **{display_classificacao_receber}**\n\nAno: **{display_ano_receber}**")
    
    
    st.markdown(f"**Análise Global Filtrada por Filial: {display_filial_receber}, Classificação: {display_classificacao_receber} e Ano: {display_ano_receber}**")
    st.write("---")
    
    
    # --- 7. EXECUÇÃO DAS ANÁLISES (AGORA EM DADOS JÁ FILTRADOS) ---
    
    # Verifica se há dados após o filtro
    if aggregator.df.empty and aggregator_dev.df.empty:
        st.warning("⚠️ Nenhum dado disponível para o grupo de filiais, classificação e ano selecionados após a filtragem.")
        return

    try:
        st.header("💰 1. Somas Individuais (Métricas Totais)")
        
        col1, col2, col3 = st.columns(3) 
        col4, col5, col6 = st.columns(3)
        
        # As somas agora são feitas no DF filtrado por Filial, Classificação E Ano (para DF principal)
        soma_valor_verba = aggregator.somar_coluna('VALOR_VERBA')
        soma_valordebito = aggregator.somar_coluna('VALORDEBITO')
        soma_valorcredito = aggregator.somar_coluna('VALORCREDITO')
        total_receber = aggregator.somar_coluna('VLR_RECEBER') 
        soma_valor_devolucao = aggregator_dev.somar_coluna('VALOR_VERBA_DEVOLUCAO')
        
        with col1:
            st.metric(label="Total 'VALOR_VERBA'", value=aggregator.formatar_moeda(soma_valor_verba))
        
        with col2:
            st.metric(label="Total 'VALORDEBITO'", value=aggregator.formatar_moeda(soma_valordebito))

        with col3:
            st.metric(label="Total 'VALORCREDITO'", value=aggregator.formatar_moeda(soma_valorcredito))
        
        with col4:
            st.metric(label='Total Devolução', value=aggregator_dev.formatar_moeda(soma_valor_devolucao))

        with col5:
            st.metric(label='Valor Areceber (Débito + Crédito)', value=aggregator.formatar_moeda(total_receber))

        with col6:
            total_aplicar = soma_valor_verba + (soma_valorcredito)
            st.metric(label='Saldo Aplicar (Verba + Crédito)', value=aggregator.formatar_moeda(total_aplicar))
        
        st.write("---")

        st.header("📊 2. Agrupamento de VLR_RECEBER por FORNECEDOR e ANO") # Título Atualizado
        
        # Usa DF filtrado, agrupando por FORNECEDOR e ANO_EMISSAO
        agrupa_somar_df = aggregator.agrupar_somar(['FORNECEDOR', 'ANO_EMISSAO'], 'VLR_RECEBER')
        
        # Ordena por Fornecedor e Ano (crescente) para ver as pendências mais antigas primeiro
        if not agrupa_somar_df.empty:
            agrupa_somar_df = agrupa_somar_df.sort_values(by=['FORNECEDOR', 'ANO_EMISSAO'], ascending=[True, True])
            
            # --- FILTRO NOVO: Excluir linhas onde a soma é 0 (ou seja, Valor a Receber é 0)
            agrupa_somar_df = agrupa_somar_df[agrupa_somar_df['Soma_de_VLR_RECEBER'] != 0].copy()


        # --- CAMPO DE BUSCA PARA FORNECEDOR ---
        termo_busca = st.text_input(
            "🔎 Buscar Fornecedor (Busca Parcial/Exata):", 
            key='search_fornecedor',
            placeholder="Digite o nome ou parte do nome do fornecedor..."
        )
        
        df_para_exibir = agrupa_somar_df.copy()

        if termo_busca:
            # Filtra o DataFrame agrupado pelo termo de busca (case-insensitive)
            # Garante que a coluna 'FORNECEDOR' é string para aplicar .str.contains
            df_para_exibir = df_para_exibir[
                df_para_exibir['FORNECEDOR'].astype(str).str.contains(termo_busca, case=False, na=False)
            ]

        # Formatação e Exibição - Coluna de Soma atualizada para 'Soma_de_VLR_RECEBER'
        df_formatado_2 = aggregator.formata_coluna_moeda(df_para_exibir, ['Soma_de_VLR_RECEBER'])
        st.dataframe(df_formatado_2)
        
        st.write("---")
        st.header("➕ 3. Detalhe de Valores a Receber (VLR_RECEBER)")
        
        # Usa DF filtrado
        df_filtrado_nonezore = aggregator.df[aggregator.df['VLR_RECEBER'] > 0].copy()

        if df_filtrado_nonezore.empty:
            st.info('Nenhum dado encontrado')
        else:
            colunas_formatadas = ['VALORDEBITO', 'VALORCREDITO', 'VLR_RECEBER']
            df_formatado_3 = aggregator.formata_coluna_moeda(df_filtrado_nonezore, colunas_formatadas)
            st.dataframe(df_formatado_3[['CODIGOFILIAL', 'FORNECEDOR'] + colunas_formatadas])
        
        st.write("---")
        
        st.header("🔎 4. Agrupamento de VALOR_VERBA por FILIAL e COMPRADOR")
        
        coluna_agrupamento_4 = ['CODIGOFILIAL','COMPRADOR']
        coluna_soma_4 = 'VALOR_VERBA'

        # Usa DF filtrado globalmente (Filial + Classificação + Ano)
        agrupado_4 = aggregator.agrupar_somar(coluna_agrupamento_4, coluna_soma_4)
        coluna_para_formatar_4 = f'Soma_de_{coluna_soma_4}'

        df_formatado_4 = aggregator.formata_coluna_moeda(agrupado_4, [coluna_para_formatar_4])
        
        st.write(f' Verba agrupada por **FILIAL e COMPRADOR**:')
        st.dataframe(df_formatado_4)

        st.write("---")
        st.write("---")

        st.header("🔄 5. Análise de Verbas de Devolução")

        st.subheader("Agrupamento de Devolução por FORNECEDOR")
        # Usa DF de devolução filtrado (Filial e Classificação)
        agrupa_dev_fornecedor = aggregator_dev.agrupar_somar('FORNECEDOR', 'VALOR_VERBA_DEVOLUCAO')
        
        df_formatado_5a = aggregator_dev.formata_coluna_moeda(agrupa_dev_fornecedor, ['Soma_de_VALOR_VERBA_DEVOLUCAO'])
        st.dataframe(df_formatado_5a)

        st.write("---")
        
        st.subheader("Devolução por CLASSIFICACAO (Agrupamento Simples)")
        
        coluna_agrupamento_5 = 'CLASSIFICACAO'
        coluna_soma_5 = 'VALOR_VERBA_DEVOLUCAO'

        # Usa DF de devolução filtrado (Filial e Classificação)
        dev_filtrada_agrupada = aggregator_dev.agrupar_somar(coluna_agrupamento_5, coluna_soma_5)
        coluna_para_formatar_5 = f'Soma_de_{coluna_soma_5}'

        df_formatado_5b = aggregator_dev.formata_coluna_moeda(dev_filtrada_agrupada, [coluna_para_formatar_5])
        st.write(f'Verba de Devolução agrupada por **CLASSIFICACAO**:')
        st.dataframe(df_formatado_5b)


        st.header("💰 6. Agregação de Valor a Receber por Classificação")
        st.subheader(f'Valor a Receber total por Classificação')
        
        coluna_agrupamento_6 = 'CLASSIFICACAO'
        coluna_soma_6 = 'VLR_RECEBER' 

        # Usa DF filtrado globalmente (Filial + Classificação + Ano)
        df_filtrada_agrupada = aggregator.agrupar_somar(coluna_agrupamento_6, coluna_soma_6)
        coluna_para_fomatar_6 = f'Soma_de_{coluna_soma_6}'
        
        df_formatado_6 = aggregator.formata_coluna_moeda(df_filtrada_agrupada, [coluna_para_fomatar_6])
        st.write(f'Valor a Receber agrupado por **CLASSIFICACAO**:')
        st.dataframe(df_formatado_6)
        
        
        st.write("---")
        st.header("📅 7. Análise de Valor a Receber por Ano de Emissão (Data de Cadastro)")
        st.subheader(f'Valor a Receber total agrupado por Ano')
        
        coluna_agrupamento_7 = 'ANO_EMISSAO'
        coluna_soma_7 = 'VLR_RECEBER' 

        # Usa DF filtrado globalmente (Filial + Classificação + Ano)
        df_agrupado_por_ano = aggregator.agrupar_somar(coluna_agrupamento_7, coluna_soma_7)
        coluna_para_fomatar_7 = f'Soma_de_{coluna_soma_7}'
        
        if df_agrupado_por_ano.empty:
             st.info("Nenhum dado com a coluna 'ANO_EMISSAO' ou 'VLR_RECEBER' encontrado para o agrupamento.")
             return

        # Verifica se todas as datas são inválidas (Ano 0)
        if 'ANO_EMISSAO' in df_agrupado_por_ano.columns and all(df_agrupado_por_ano['ANO_EMISSAO'] == 0):
            st.warning("⚠️ Atenção: Todas as datas na coluna 'DATACADASTRO' podem estar em formato incorreto ou faltando, resultando em Ano 0.")
            st.warning("Verifique se o formato da data é 'YYYY-MM-DD'. Tentando exibir os valores agrupados (incluindo Ano 0) para depuração:")
            
            df_formatado_7_debug = aggregator.formata_coluna_moeda(df_agrupado_por_ano, [coluna_para_fomatar_7])
            st.dataframe(df_formatado_7_debug)
            
            return

        # Aplica filtro para remover ano 0 e ordena
        df_agrupado_valid = df_agrupado_por_ano[df_agrupado_por_ano['ANO_EMISSAO'] > 0] 

        if df_agrupado_valid.empty:
            st.info("Nenhum dado com data de emissão válida encontrado para o agrupamento por ano (após exclusão do Ano 0).")
            return

        df_agrupado_valid = df_agrupado_valid.sort_values(by='ANO_EMISSAO', ascending=False)
        df_formatado_7 = aggregator.formata_coluna_moeda(df_agrupado_valid, [coluna_para_fomatar_7])
        st.dataframe(df_formatado_7)


    except Exception as e:
        st.error(f"\n❌ ERRO FATAL na execução das funções: {e}")


if __name__ == "__main__":
    main()