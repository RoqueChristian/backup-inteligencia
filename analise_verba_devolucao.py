import pandas as pd
from datetime import date

DATA_FILE_DEVOLUCAO = 'dados_acompanhamento_verba_devolucao.csv'


def carregar_analisar_verba_devolucao():

    try:
        df = pd.read_csv(
            DATA_FILE_DEVOLUCAO,
            sep=';',
            decimal=',',
            encoding='utf-8-sig'
        )

    except FileNotFoundError:
        print(f"ERRO: O arquivo '{DATA_FILE_DEVOLUCAO}' não foi encontrado.")
        return pd.DataFrame() 

    # --- Conversão de Datas ---
    # Usando %Y para 4 dígitos do ano, se seus dados estiverem em 20/09/2024 (dia/mês/ano)
    df['DATA_VENCIMENTO'] = pd.to_datetime(df['DATA_VENCIMENTO'], format='%d/%m/%Y', errors='coerce')
    df['DATA_EMISSAO'] = pd.to_datetime(df['DTEMISSAO'], format='%d/%m/%Y', errors='coerce')
    df['DATA_PAGAMENTO'] = pd.to_datetime(df['DATA_PAGAMENTO'], format='%d/%m/%Y', errors='coerce')

    # --- LÓGICA DE STATUS: QUITADA vs. PENDENTE ---
    # Uma verba é QUITADA se houver uma DATA_PAGAMENTO.
    df['STATUS_VERBA'] = df['DATA_PAGAMENTO'].apply(
        lambda x: 'QUITADA' if pd.notna(x) else 'PENDENTE'
    )
    
    # --- CÁLCULO DE DIAS e CLASSIFICAÇÃO (VENCIDA / A VENCER) ---
    data_hoje = pd.to_datetime(date.today())
    df['DIFERENCA_DIAS'] = (data_hoje - df['DATA_VENCIMENTO']).dt.days

    def classificar_status_vencidos(row):
        # 1. Se estiver QUITADA, não precisa de análise de vencimento
        if row['STATUS_VERBA'] == 'QUITADA':
            return 'QUITADA'
        
        # 2. Se for PENDENTE, verifica o vencimento:
        if row['DIFERENCA_DIAS'] > 0:
            return 'VENCIDA'
        else:
            return 'A VENCER'

    df['STATUS_VENCIDOS'] = df.apply(classificar_status_vencidos, axis=1)

    # --- Cálculo de DIAS_VENCIDOS ---
    df['DIAS_VENCIDOS'] = df.apply(
        lambda row: row['DIFERENCA_DIAS'] if row['STATUS_VENCIDOS'] == 'VENCIDA' else 0,
        axis=1
    )
    
    # Limpeza
    df = df.drop(columns=['DIFERENCA_DIAS'])
    
    return df

# As funções kpi_devolucao e resumo_fornecedor_devolucao foram simplificadas para usar STATUS_VERBA

def kpi_devolucao(df: pd.DataFrame) -> dict:
    # Filtra apenas o que está PENDENTE para o KPI geral
    kpi_devolucao = df[df['STATUS_VERBA'] == 'PENDENTE']['VALOR_VERBA_DEVOLUCAO'].sum()
    return {'SALDO_RECEBER' : kpi_devolucao}


def resumo_fornecedor_devolucao (df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    
    # Filtra apenas PENDENTE antes de agrupar
    df_pendente = df[df['STATUS_VERBA'] == 'PENDENTE'].copy()
    
    collunas_agrupamento = ['FILIAL', 'CLASSIFICACAO', 'FORNECEDOR', 'COMPRADOR']

    df_resumo = df_pendente.groupby(collunas_agrupamento).agg(
        VALOR_VERBA_DEVOLUCAO=('VALOR_VERBA_DEVOLUCAO', 'sum')
    ).reset_index()

    return df_resumo.sort_values(by='VALOR_VERBA_DEVOLUCAO', ascending=False).reset_index(drop=True)