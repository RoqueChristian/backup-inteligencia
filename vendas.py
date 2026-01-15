import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import gc

# Aumentar limite de células para renderização de estilos
pd.set_option("styler.render.max_elements", 1000000)

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Executive Intelligence | Customer LifeCycle",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilização via CSS para visual "Premium"
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 28px; font-weight: bold; color: #1E3A8A; }
    .main { background-color: #F8FAFC; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; white-space: pre-wrap; background-color: #FFFFFF; 
        border-radius: 5px 5px 0px 0px; padding: 10px 20px; color: #1E3A8A;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 1. FUNÇÕES DE SUPORTE (ENGINE) ---

def formatar_moeda(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor

def formata_coluna_moeda(df: pd.DataFrame, colunas: list) -> pd.DataFrame:
    df_formatado = df.copy()
    for coluna in colunas:
        if coluna in df_formatado.columns:
            try:
                df_formatado[coluna] = df_formatado[coluna].apply(formatar_moeda)
            except Exception as e:
                st.warning(f'Não foi possivel formatar a coluna {coluna} como moeda: {e}')
    return df_formatado

@st.cache_data(show_spinner=False)
def load_and_clean_dim(path, id_col, colunas_uteis):
    if not os.path.exists(path): return pd.DataFrame()
    df = pd.read_parquet(path, columns=[c.upper() for c in colunas_uteis])
    df.columns = [c.lower() for c in df.columns]
    id_col = id_col.lower()
    df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(np.int32)
    df = df.drop_duplicates(subset=[id_col])
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('N/I').str.strip().str.upper().astype('category')
    return df

@st.cache_data(show_spinner=False)
def processar_base_completa():
    if not os.path.exists('fato_venda.parquet'): return pd.DataFrame()
    
    # Otimização de Memória: Carregar apenas colunas necessárias
    cols_load = ['COD_FILIAL', 'DATA_MOVIMENTACAO', 'NUM_PEDIDO', 'COD_VENDEDOR', 
                 'COD_SUPERVISOR', 'COD_CLIENTE', 'COD_PRODUTO', 'QT_VENDIDA', 
                 'VALOR_LIQUIDO', 'ORIGEM_PEDIDO']
    try:
        df = pd.read_parquet('fato_venda.parquet', columns=cols_load)
    except:
        df = pd.read_parquet('fato_venda.parquet')
        df.columns = [c.upper() for c in df.columns]
        df = df[[c for c in cols_load if c in df.columns]]
        gc.collect()

    df.columns = [c.upper() for c in df.columns]
    
    # Garantir colunas opcionais que podem não vir da query SQL
    for col in ['COD_SUPERVISOR', 'ORIGEM_PEDIDO']:
        if col not in df.columns:
            df[col] = 0 if 'COD' in col else 'N/I'
    
    df['DATA_MOVIMENTACAO'] = pd.to_datetime(df['DATA_MOVIMENTACAO'], errors='coerce')
    df['ANO'] = df['DATA_MOVIMENTACAO'].dt.year.fillna(0).astype(np.int16)
    df['MES'] = df['DATA_MOVIMENTACAO'].dt.month.fillna(0).astype(np.int8)
    
    # Tratamento Financeiro
    for col in ['QT_VENDIDA', 'VALOR_LIQUIDO']:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.float32)
    
    # IDs
    for col in ['COD_FILIAL', 'NUM_PEDIDO', 'COD_VENDEDOR', 'COD_SUPERVISOR', 'COD_CLIENTE', 'COD_PRODUTO']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.int32)

    # Merges Sequenciais para Otimização de RAM
    df_p = load_and_clean_dim('dim_produto.parquet', 'cod_produto', ['cod_produto', 'nm_produto', 'categoria', 'secao'])
    df = df.merge(df_p, left_on='COD_PRODUTO', right_on='cod_produto', how='left')
    del df_p
    
    df_c = load_and_clean_dim('dim_cliente.parquet', 'cod_cliente', ['cod_cliente', 'nm_cliente'])
    df = df.merge(df_c, left_on='COD_CLIENTE', right_on='cod_cliente', how='left')
    del df_c
    
    df_v = load_and_clean_dim('dim_vendedor.parquet', 'cod_vendedor', ['cod_vendedor', 'nm_vendedor'])
    df = df.merge(df_v, left_on='COD_VENDEDOR', right_on='cod_vendedor', how='left')
    del df_v

    # Higienização de Strings Final
    cols_str = ['nm_produto', 'categoria', 'secao', 'nm_cliente', 'nm_vendedor', 'ORIGEM_PEDIDO']
    for col in cols_str:
        if col in df.columns:
            df[col] = df[col].astype(object).fillna('NAO CADASTRADO').astype(str).str.upper().astype('category')
    
    return df

# --- 2. LOGICA DE NEGÓCIO ---

df_base = processar_base_completa()
hoje = df_base['DATA_MOVIMENTACAO'].max()

# Sidebar Profissional
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3222/3222800.png", width=80)
    st.title("Filtros Executivos")
    
    vendedores = sorted(df_base['nm_vendedor'].unique())
    f_vendedor = st.multiselect("Filtrar por Vendedor", options=vendedores)
    
    st.markdown("---")
    st.caption(f"Dados sincronizados até: {hoje.strftime('%d/%m/%Y')}")

# Aplicação de Filtros
df_f = df_base if not f_vendedor else df_base[df_base['nm_vendedor'].isin(f_vendedor)]

# --- 3. DASHBOARD UI ---

tab1, tab2, tab3, tab4 = st.tabs(["🏛️ Gestão de Carteira", "🔍 Raio-X do Cliente", "🎯 Sugestão de Mix", "📅 Evolução de Itens"])

# --- ABA 1: VISÃO MACRO (EXECUTIVE SUMMARY) ---
with tab1:
    st.subheader("Performance Consolidada 2024-2025")
    
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Faturamento Líquido", formatar_moeda(df_f['VALOR_LIQUIDO'].sum()))
    with k2:
        st.metric("Clientes Ativos (Total)", df_f['COD_CLIENTE'].nunique())
    with k3:
        fat_25 = df_f[df_f['ANO'] == 2025]['VALOR_LIQUIDO'].sum()
        fat_24 = df_f[df_f['ANO'] == 2024]['VALOR_LIQUIDO'].sum()
        delta = ((fat_25 / fat_24) - 1) * 100 if fat_24 > 0 else 0
        st.metric("Faturamento 2025", formatar_moeda(fat_25), f"{delta:.1f}% YoY")
    with k4:
        st.metric("Mix Ativo", f"{df_f['nm_produto'].nunique()} SKUs")

    st.divider()
    
    c_left, c_right = st.columns([2, 1])
    with c_left:
        st.markdown("**Sazonalidade Comparativa**")
        evol = df_f.groupby(['ANO', 'MES'])['VALOR_LIQUIDO'].sum().reset_index()
        evol['VALOR_FMT'] = evol['VALOR_LIQUIDO'].apply(formatar_moeda)
        fig_evol = px.line(evol, x='MES', y='VALOR_LIQUIDO', color='ANO', markers=True, 
                           color_discrete_map={2024: '#94A3B8', 2025: '#2563EB'},
                           custom_data=['VALOR_FMT'], text='VALOR_FMT')
        fig_evol.update_layout(plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0, r=0, t=30, b=0))
        fig_evol.update_traces(hovertemplate="Mês: %{x}<br>Valor: %{customdata[0]}", textposition="top center")
        st.plotly_chart(fig_evol, use_container_width=True)
        
    with c_right:
        st.markdown("**Top 10 Categorias**")
        cat_data = df_f.groupby('categoria')['VALOR_LIQUIDO'].sum().reset_index().sort_values('VALOR_LIQUIDO', ascending=False).head(10)
        cat_data['VALOR_FMT'] = cat_data['VALOR_LIQUIDO'].apply(formatar_moeda)
        fig_cat = px.bar(cat_data, x='VALOR_LIQUIDO', y='categoria', orientation='h', 
                         color_continuous_scale='Blues', color='VALOR_LIQUIDO', text='VALOR_FMT')
        fig_cat.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_cat, use_container_width=True)

    st.divider()
    st.markdown("**Distribuição por Origem do Pedido**")
    origem_data = df_f.groupby('ORIGEM_PEDIDO')['VALOR_LIQUIDO'].sum().reset_index()
    origem_data['VALOR_FMT'] = origem_data['VALOR_LIQUIDO'].apply(formatar_moeda)
    
    fig_tree = px.treemap(origem_data, path=['ORIGEM_PEDIDO'], values='VALOR_LIQUIDO',
                          color='VALOR_LIQUIDO', color_continuous_scale='Blues',
                          custom_data=['VALOR_FMT'])
    fig_tree.update_traces(hovertemplate='Origem: %{label}<br>Faturamento: %{customdata[0]}')
    st.plotly_chart(fig_tree, use_container_width=True)

# --- ABA 2: VISÃO MICRO (CUSTOMER DRILL-DOWN) ---
with tab2:
    clientes_list = sorted(df_f['nm_cliente'].unique())
    col_sel, col_empty = st.columns([1, 2])
    cliente_sel = col_sel.selectbox("Selecione o Cliente para Auditoria:", options=clientes_list)
    
    if cliente_sel:
        df_c = df_f[df_f['nm_cliente'] == cliente_sel]
        
        # Header do Cliente
        st.markdown(f"### 👤 {cliente_sel}")
        st.caption(f"Atendido por: {df_c['nm_vendedor'].iloc[0]}")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("LTV (Total)", formatar_moeda(df_c['VALOR_LIQUIDO'].sum()))
        m2.metric("Ticket Médio", formatar_moeda(df_c['VALOR_LIQUIDO'].sum() / df_c['NUM_PEDIDO'].nunique()))
        
        ult_data = df_c['DATA_MOVIMENTACAO'].max()
        inatividade = (hoje - ult_data).days
        color_recency = "normal" if inatividade < 30 else "inverse"
        m3.metric("Última Compra", ult_data.strftime('%d/%m/%Y'), f"{inatividade} dias", delta_color=color_recency)
        m4.metric("Nº Pedidos", df_c['NUM_PEDIDO'].nunique())
        
        st.divider()
        
        t_l, t_r = st.columns([2, 1])
        with t_l:
            st.markdown("**Performance de SKUs (Top 100)**")
            itens = df_c.groupby(['nm_produto', 'categoria']).agg(
                Qtd=('QT_VENDIDA', 'sum'),
                Total_RS=('VALOR_LIQUIDO', 'sum'),
                Ultima_Vez=('DATA_MOVIMENTACAO', 'max')
            ).reset_index().sort_values('Total_RS', ascending=False).head(100)
            
            itens['Status'] = itens['Ultima_Vez'].apply(lambda x: '🔵 ATIVO' if x.year == 2025 else '🔴 CHURN')
            itens['Ultima_Vez'] = itens['Ultima_Vez'].dt.strftime('%d/%m/%Y')
            itens = formata_coluna_moeda(itens, ['Total_RS'])
            st.dataframe(itens, use_container_width=True, hide_index=True)
            
        with t_r:
            st.markdown("**Share of Wallet por Categoria**")
            # Filtrar valores > 0 e preparar dados
            df_sun = df_c[df_c['VALOR_LIQUIDO'] > 0].copy()
            
            if not df_sun.empty:
                df_sun['VALOR_FMT'] = df_sun['VALOR_LIQUIDO'].apply(formatar_moeda)
                
                fig_sun = px.sunburst(df_sun, path=['categoria', 'secao'], values='VALOR_LIQUIDO', 
                                      color='VALOR_LIQUIDO', color_continuous_scale='Blues',
                                      custom_data=['VALOR_FMT'])
                
                fig_sun.update_traces(hovertemplate='<b>%{label}</b><br>Venda: %{customdata[0]}<br>Share: %{percentRoot:.1%}',
                                      textinfo='label+percent entry')
                fig_sun.update_layout(margin=dict(t=0, l=0, r=0, b=0))
                st.plotly_chart(fig_sun, use_container_width=True)
            else:
                st.info("Sem dados suficientes para exibir o gráfico.")

# --- ABA 3: SUGESTÃO DE MIX (OPPORTUNITY FINDER) ---
with tab3:
    st.subheader("🎯 Inteligência Comercial: Cross-Selling")
    
    cliente_mix = st.selectbox("Selecione o Cliente para Sugestão de Venda:", options=clientes_list, key="mix_sel")
    
    if cliente_mix:
        # Lógica Sênior: GAP de Categorias
        todas_categorias = set(df_f['categoria'].unique())
        atuais_cli = set(df_f[df_f['nm_cliente'] == cliente_mix]['categoria'].unique())
        gap = todas_categorias - atuais_cli
        
        col_mix_l, col_mix_r = st.columns([1, 2])
        
        with col_mix_l:
            st.info(f"O cliente **{cliente_mix}** consome **{len(atuais_cli)}** categorias de um total de **{len(todas_categorias)}**.")
            st.markdown("#### ✅ Categorias Atuais")
            for c in sorted(atuais_cli):
                if c != 'NAO CADASTRADO': st.write(f"• {c}")
        
        with col_mix_r:
            st.markdown("#### 💡 Sugestões de Expansão (Porta de Entrada)")
            if gap:
                # Pegar o Top 1 SKU de cada categoria faltante (baseado na venda geral da empresa)
                top_vendas = df_f.groupby(['categoria', 'nm_produto'])['VALOR_LIQUIDO'].sum().reset_index()
                top_vendas = top_vendas.sort_values(['categoria', 'VALOR_LIQUIDO'], ascending=[True, False]).drop_duplicates('categoria')
                
                sugestoes = top_vendas[top_vendas['categoria'].isin(gap)].head(8)
                
                st.dataframe(sugestoes[['categoria', 'nm_produto']].rename(columns={'categoria':'Categoria Faltante', 'nm_produto':'Produto Isca (Mais Vendido)'}), 
                             use_container_width=True, hide_index=True)
                
                st.success("DICA: Use estes produtos 'Isca' para abrir novas categorias no cliente.")
            else:
                st.balloons()
                st.success("Este cliente já consome todas as categorias do seu portfólio!")

    st.divider()
    st.markdown("#### 🚩 Alertas de Erosão de Mix")
    # Clientes que tinham mix maior em 2024 do que em 2025
    mix_24 = df_f[df_f['ANO'] == 2024].groupby('nm_cliente')['categoria'].nunique()
    mix_25 = df_f[df_f['ANO'] == 2025].groupby('nm_cliente')['categoria'].nunique()
    erosao = (mix_24 - mix_25).reset_index()
    erosao.columns = ['Cliente', 'Perda_de_Mix']
    st.write("Clientes que reduziram a variedade de categorias compradas (Risco de Abandono):")
    st.dataframe(erosao[erosao['Perda_de_Mix'] > 0].sort_values('Perda_de_Mix', ascending=False).head(20), use_container_width=True, hide_index=True)

# --- ABA 4: EVOLUÇÃO DE ITENS (ITEM HISTORY) ---
with tab4:
    st.subheader("📅 Histórico de Compras: Item x Mês")
    
    clientes_list_4 = sorted(df_f['nm_cliente'].unique())
    cliente_sel_4 = st.selectbox("Selecione o Cliente:", options=clientes_list_4, key="tab4_cliente")
    
    if cliente_sel_4:
        df_c_4 = df_f[df_f['nm_cliente'] == cliente_sel_4]
        
        # Pivot Table: Produtos x Meses (Quantidade)
        pivot = df_c_4.pivot_table(index='nm_produto', columns=['ANO', 'MES'], values='QT_VENDIDA', aggfunc='sum', fill_value=0)
        
        # Formatando colunas para MM/AAAA
        pivot.columns = [f"{m:02d}/{y}" for y, m in pivot.columns]
        
        # Adicionando Total e Ordenando
        pivot['Total'] = pivot.sum(axis=1)
        pivot = pivot.sort_values('Total', ascending=False)
        
        # OTIMIZAÇÃO: Limitar visualização para evitar travamento (Top 150 itens)
        limit = 150
        pivot_view = pivot.head(limit)
        
        c_title, c_down = st.columns([3, 1])
        with c_title:
            st.markdown(f"**Matriz de Quantidade Vendida - {cliente_sel_4}** (Top {limit})")
        with c_down:
            st.download_button("📥 Baixar Completo", pivot.to_csv().encode('utf-8-sig'), f"historico_{cliente_sel_4}.csv", "text/csv")
        
        styler = pivot_view.style.format("{:,.0f}")
        try:
            import matplotlib # Verifica explicitamente se a lib existe
            styler = styler.background_gradient(cmap="Blues", axis=1)
        except ImportError:
            pass # Matplotlib não instalado, segue sem gradiente
            
        st.dataframe(styler, use_container_width=True)
        
        if len(pivot) > limit:
            st.caption(f"ℹ️ A visualização foi limitada aos {limit} itens mais relevantes para manter a velocidade. Use o botão de download para ver tudo.")