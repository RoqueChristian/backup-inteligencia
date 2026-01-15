import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import date
import numpy as np 

# --------------------------------------------------------
# 1. FUNÇÕES DE PRÉ-PROCESSAMENTO E CÁLCULO DE SALDOS
# --------------------------------------------------------

DATA_FILE = 'dados_acompanhamento_verba.csv'


def formatar_moeda(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@st.cache_data
def carregar_e_analisar_verbas():
    """Carrega o CSV, faz o pré-processamento e calcula os novos saldos."""
    try:
        df = pd.read_csv(
            DATA_FILE, 
            sep=';', 
            decimal=',', 
            encoding='utf-8-sig'
        )
    except FileNotFoundError:
        st.error(f"ERRO: O arquivo '{DATA_FILE}' não foi encontrado.")
        st.stop() 

    # --- 1. Renomeação de Colunas (para consistência) ---
    colunas_para_renomear = {
        'CODIGOFILIAL': 'FILIAL',
        'NUMEROVERBA': 'NUMERO_VERBA',
        'DATACADASTRO': 'DATA_CADASTRO',
        'DATAVENCIMENTO': 'DATA_VENCIMENTO',
        'VALOR_VERBA': 'VALORVERBA',
        'VALORAPLICADO': 'VALOR_APLICADO_TOTAL',
        'VALORDEBITO': 'VALOR_DEBITO',
        'VALORCREDITO': 'VALOR_CREDITO',
        'SITUACAO': 'STATUS_VERBA'
    }
    df.rename(columns=colunas_para_renomear, inplace=True)
    
    # Filtro de Classificação (Exclui 'OUTROS')
    if 'CLASSIFICACAO' in df.columns:
        df = df[df['CLASSIFICACAO'] != 'OUTROS'].copy() 

    # --- 2. Conversão e Tratamento de Nulos ---
    
    # Datas
    date_cols = ['DATA_VENCIMENTO', 'DATA_CADASTRO']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

    df['ANOCADASTRO'] = df['DATA_CADASTRO'].dt.year.fillna(0).astype(int) 
    
    # Valores
    num_cols = ['VALORVERBA', 'VALOR_APLICADO_TOTAL', 'VALOR_DEBITO', 'VALOR_CREDITO']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0.0 

    # Strings
    df['COMPRADOR'] = df['COMPRADOR'].fillna('NÃO DEFINIDO')
    df['FORNECEDOR'] = df['FORNECEDOR'].fillna('NÃO DEFINIDO')
    df['CLASSIFICACAO'] = df['CLASSIFICACAO'].fillna('NÃO CLASSIFICADO')
    
    # --- 3. CÁLCULO DOS NOVOS PARÂMETROS ---
    
    data_hoje = pd.to_datetime(date.today()) 
    
    # SALDO_A_RECEBER é Débito + Crédito
    df['SALDO_A_RECEBER'] = df['VALOR_DEBITO'] + df['VALOR_CREDITO']
    df['DIFERENCA_DIAS'] = (data_hoje - df['DATA_VENCIMENTO']).dt.days
    df['DIAS_VENCIDOS'] = df['DIFERENCA_DIAS'].apply(lambda x: x if x > 0 else 0)
    df['STATUS_VENCIMENTO'] = np.select(
        [
            (df['DIFERENCA_DIAS'] > 0) & (df['SALDO_A_RECEBER'] > 0),
            df['SALDO_A_RECEBER'] == 0 
        ],
        [
            'VENCIDA', 
            'LIQUIDADA'
        ],
        default='A VENCER' 
    )
    
    df = df.drop(columns=['DIFERENCA_DIAS'])
    
    return df


# --------------------------------------------------------
# 2. FUNÇÕES DE VISUALIZAÇÃO PLOTLY
# --------------------------------------------------------

def plot_saldo_aplicar_por_comprador(df):
    """Cria um gráfico de barras com o Saldo a Aplicar por Comprador e Classificação."""
    
    df_filtrado = df.copy()
    
    analise_aplicar = df_filtrado.groupby(['CLASSIFICACAO', 'COMPRADOR'])['SALDO_A_APLICAR'].sum().reset_index()
    analise_aplicar = analise_aplicar.sort_values(by='SALDO_A_APLICAR', ascending=False).head(15)

    fig = px.bar(
        analise_aplicar,
        x='COMPRADOR',
        y='SALDO_A_APLICAR',
        color='CLASSIFICACAO',
        title='Top 15 Compradores por Saldo Total **A Aplicar** (Valor Cheio)',
        hover_data={'SALDO_A_APLICAR': ':.2f'},
        template='plotly_white'
    )
    fig.update_layout(yaxis_title='Saldo A Aplicar (R$)', xaxis_title='Comprador', legend_title='Classificação')
    fig.update_traces(hovertemplate='Comprador: %{x}<br>Saldo: R$ %{y:,.2f}<extra></extra>')
    return fig

def plot_saldo_vencido_por_classificacao(df):
    
    df_vencidas = df[
        (df['STATUS_VERBA'] == 'ATIVA') & 
        (df['STATUS_VENCIMENTO'] == 'VENCIDA') & 
        (df['SALDO_A_RECEBER'] > 0)
    ].copy()
    
    if df_vencidas.empty:
        return None 
    
    analise_vencidas = df_vencidas.groupby('CLASSIFICACAO')['SALDO_A_RECEBER'].sum().reset_index()

    fig = px.pie(
        analise_vencidas,
        values='SALDO_A_RECEBER',
        names='CLASSIFICACAO',
        title='Distribuição do **Saldo A Receber** (Verbas Vencidas e Ativas)',
        hole=.3,
        template='plotly_white'
    )
    fig.update_traces(
        textinfo='percent+value', 
        texttemplate='R$ %{value:,.2f}<br>(%{percent})',
        hoverinfo='label+value+percent'
    )
    return fig

def plot_evolucao_saldo_por_ano(df):

    df_status_ano = df.groupby('ANOCADASTRO').agg(
        VENCIDA=('SALDO_A_RECEBER', lambda x: x[df.loc[x.index, 'STATUS_VENCIMENTO'] == 'VENCIDA'].sum()),
        AVENCER=('SALDO_A_RECEBER', lambda x: x[df.loc[x.index, 'STATUS_VENCIMENTO'] == 'A VENCER'].sum())
    ).reset_index()
    
    df_melted_ano = df_status_ano.melt(
        id_vars=['ANOCADASTRO'], 
        value_vars=['VENCIDA', 'AVENCER'], 
        var_name='STATUS', 
        value_name='SALDO_PENDENTE'
    )

    df_melted_ano = df_melted_ano[df_melted_ano['SALDO_PENDENTE'] > 0]
    df_melted_ano['SALDO_FMT'] = df_melted_ano['SALDO_PENDENTE'].apply(formatar_moeda)

    fig_ano = px.bar(
        df_melted_ano, 
        x='ANOCADASTRO', 
        y='SALDO_PENDENTE', 
        color='STATUS', 
        title='Evolução do Saldo Pendente por Ano de Cadastro',
        labels={'ANOCADASTRO': 'Ano de Cadastro', 'SALDO_PENDENTE': 'Saldo Pendente (R$)'},
        color_discrete_map={'VENCIDA': "#CC0A0A", 'AVENCER': "#0E00CC"},
        barmode='group',
        text='SALDO_FMT'
    )
    fig_ano.update_traces(textposition='outside')
    
    return fig_ano, df_status_ano[['ANOCADASTRO', 'VENCIDA', 'AVENCER']]

# --------------------------------------------------------
# !!! NOVA FUNÇÃO: SALDO A RECEBER POR CLASSIFICAÇÃO E ANO !!!
# --------------------------------------------------------

def plot_saldo_a_receber_por_classificacao_ano(df):
    """
    Cria um gráfico de barras ano a ano para o Saldo A Receber, 
    separado por CLASSIFICACAO, a partir de 2022.
    """
    
    # 1. Filtra a partir de 2022 e onde o saldo a receber é positivo
    df_filtrado = df[
        (df['ANOCADASTRO'] >= 2022) & 
        (df['SALDO_A_RECEBER'] > 0)
    ].copy()
    
    if df_filtrado.empty:
        return None
    
    # 2. Agrupamento e Soma
    analise_saldo = df_filtrado.groupby(['ANOCADASTRO', 'CLASSIFICACAO'])['SALDO_A_RECEBER'].sum().reset_index()
    
    # 3. Formatação da moeda para o texto do gráfico
    analise_saldo['SALDO_FMT'] = analise_saldo['SALDO_A_RECEBER'].apply(formatar_moeda)

    # 4. Geração do Gráfico
    fig = px.bar(
        analise_saldo,
        x='ANOCADASTRO',
        y='SALDO_A_RECEBER',
        color='CLASSIFICACAO',
        title='Evolução Anual do **Saldo A Receber** por Classificação (A partir de 2022)',
        labels={'ANOCADASTRO': 'Ano de Cadastro', 'SALDO_A_RECEBER': 'Saldo A Receber (R$)', 'CLASSIFICACAO': 'Classificação'},
        barmode='group', # Agrupa as barras lado a lado por ano
        text='SALDO_FMT',
        template='plotly_white'
    )
    
    fig.update_layout(xaxis={'type': 'category'}) # Garante que o ano é tratado como categoria
    fig.update_traces(textposition='outside', textangle=0, hovertemplate='Classificação: %{color}<br>Ano: %{x}<br>Saldo: R$ %{y:,.2f}<extra></extra>')
    
    return fig, analise_saldo.drop(columns=['SALDO_FMT'])


# --------------------------------------------------------
# 3. CONFIGURAÇÃO DO STREAMLIT
# --------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Dashboard de Acompanhamento de Verbas",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("💰 Acompanhamento de Verbas (FARMA/HB)")
    st.markdown("Análise de Saldo a Receber e Saldo a Aplicar.")

    df = carregar_e_analisar_verbas()

    if df.empty:
        return

    # ----------------------------------------
    # SIDEBAR - FILTROS GLOBAIS
    # ----------------------------------------
    st.sidebar.header("Filtros de Análise")
    situacoes = df['STATUS_VERBA'].unique()
    situacao_selecionada = st.sidebar.multiselect(
        "Filtrar por Situação da Verba:", 
        options=situacoes, 
        default=situacoes
    )
    
    classificacoes = df['CLASSIFICACAO'].unique()
    classificacao_selecionada = st.sidebar.multiselect(
        "Filtrar por Classificação:", 
        options=classificacoes, 
        default=classificacoes
    )

    df_filtrado_global = df[
        (df['STATUS_VERBA'].isin(situacao_selecionada)) &
        (df['CLASSIFICACAO'].isin(classificacao_selecionada))
    ].copy() 
    
    
    # *** CÁLCULOS CRÍTICOS APÓS A FILTRAGEM ***
    
    # SALDO A APLICAR é VALORVERBA + VALOR_CREDITO (Valor Cheio)
    df_filtrado_global['SALDO_A_APLICAR'] = df_filtrado_global['VALORVERBA'] + df_filtrado_global['VALOR_CREDITO']

    # 2. KPIs
    total_verba = df_filtrado_global['VALORVERBA'].sum()
    saldo_receber = df_filtrado_global['SALDO_A_RECEBER'].sum()
    saldo_aplicar = df_filtrado_global['SALDO_A_APLICAR'].sum()
    
    # 3. KPI: Saldo Vencido
    saldo_vencido = df_filtrado_global[
        (df_filtrado_global['STATUS_VENCIMENTO'] == 'VENCIDA')
    ]['SALDO_A_RECEBER'].sum()
    
    # ----------------------------------------
    # ABAS DO DASHBOARD
    # ----------------------------------------

    tab_dashboard, tab_analise_detalhada, tab_tabela = st.tabs([
        "Dashboard de Resumo", 
        "Análise Detalhada",
        "Tabela de Dados"
    ])

    # ----------------------------------------
    # ABA 1: DASHBOARD DE RESUMO
    # ----------------------------------------
    with tab_dashboard:
        st.header("Indicadores Chave (KPIs)")
        
        col1, col2, col3 = st.columns(3)

        col1.metric("**Saldo A Aplicar** (Valor Cheio)", formatar_moeda(saldo_aplicar))
        col2.metric("Saldo A Receber (Pendência)", formatar_moeda(saldo_receber))
        col3.metric(
            "Saldo Vencido", 
            formatar_moeda(saldo_vencido),
            delta_color="inverse"
        )
        
        st.markdown("---")

        st.header("Visualizações de Saldo")
        
        col_a, col_b = st.columns([7, 3]) 

        with col_a:
            st.plotly_chart(plot_saldo_aplicar_por_comprador(df_filtrado_global), use_container_width=True)

        with col_b:
            fig_pie = plot_saldo_vencido_por_classificacao(df_filtrado_global)
            if fig_pie:
                 st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Nenhum saldo vencido para exibir no filtro atual.")
                
    # ----------------------------------------
    # ABA 2: ANÁLISE DETALHADA (EVOLUÇÃO E STATUS)
    # ----------------------------------------
    with tab_analise_detalhada:
        st.header("Análise de Evolução e Status")
        
        # --- NOVO GRÁFICO: Saldo a Receber por Classificação/Ano (a partir de 2022) ---
        st.subheader("Saldo A Receber: Evolução Anual por Classificação (a partir de 2022) 📊")
        fig_classificacao_ano, df_classificacao_ano = plot_saldo_a_receber_por_classificacao_ano(df_filtrado_global)
        
        if fig_classificacao_ano:
            st.plotly_chart(fig_classificacao_ano, use_container_width=True)
            
            st.caption("Detalhes do Saldo A Receber por Ano e Classificação (R$):")
            st.dataframe(df_classificacao_ano.pivot(
                index='ANOCADASTRO', 
                columns='CLASSIFICACAO', 
                values='SALDO_A_RECEBER'
            ).fillna(0).style.format(formatar_moeda), use_container_width=True)
            
        else:
            st.info("Nenhum saldo A Receber a partir de 2022 para exibir no filtro atual.")
            
        st.markdown("---")
        
        # --- GRÁFICO ORIGINAL: Saldo Pendente Total por Ano ---
        fig_ano, df_ano = plot_evolucao_saldo_por_ano(df_filtrado_global)
        
        ##st.subheader("Saldo Pendente Total: Evolução por Ano de Cadastro (Vencida vs. A Vencer)")
        ##st.plotly_chart(fig_ano, use_container_width=True)
        
        # st.caption("Detalhes do Saldo por Ano de Cadastro (R$):")
        # st.dataframe(df_ano.style.format({
        #     'VENCIDA': formatar_moeda, 
        #     'AVENCER': formatar_moeda,
        # }), use_container_width=True)
        
    # ----------------------------------------
    # ABA 3: TABELA DE DADOS
    # ----------------------------------------
    with tab_tabela:
        st.header("Tabela de Dados Detalhada")

        st.sidebar.subheader("Filtros da Tabela")
        

        comprador_selecionado = st.sidebar.multiselect(
            "Comprador (Tabela):",
            options=df_filtrado_global['COMPRADOR'].unique(),
            default=[]
        )
        status_venc_selecionado = st.sidebar.multiselect(
            "Status Vencimento (Tabela):",
            options=df_filtrado_global['STATUS_VENCIMENTO'].unique(),
            default=[]
        )
        
        df_tabela = df_filtrado_global.copy()
        
        if comprador_selecionado:
            df_tabela = df_tabela[df_tabela['COMPRADOR'].isin(comprador_selecionado)]
        
        if status_venc_selecionado:
            df_tabela = df_tabela[df_tabela['STATUS_VENCIMENTO'].isin(status_venc_selecionado)]
        

        st.dataframe(
            df_tabela[[
                'CLASSIFICACAO', 'FORNECEDOR', 'COMPRADOR', 'NUMERO_VERBA', 
                'VALORVERBA', 'VALOR_APLICADO_TOTAL', 'VALOR_DEBITO', 
                'VALOR_CREDITO', 
                'SALDO_A_APLICAR', 'SALDO_A_RECEBER', 'DATA_VENCIMENTO', 
                'STATUS_VENCIMENTO', 'STATUS_VERBA', 'DIAS_VENCIDOS'
            ]],
            use_container_width=True,
            column_config={
                "VALORVERBA": st.column_config.NumberColumn(format="R$ %.2f"),
                "VALOR_APLICADO_TOTAL": st.column_config.NumberColumn(format="R$ %.2f"),
                "VALOR_DEBITO": st.column_config.NumberColumn(format="R$ %.2f"),
                "VALOR_CREDITO": st.column_config.NumberColumn(format="R$ %.2f"), 
                "SALDO_A_APLICAR": st.column_config.NumberColumn(format="R$ %.2f"),
                "SALDO_A_RECEBER": st.column_config.NumberColumn(format="R$ %.2f"),
            }
        )


if __name__ == '__main__':
    main()