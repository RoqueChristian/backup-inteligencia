import pandas as pd
import sys
import os

# Adiciona o diretório pai para importação das funções de análise
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analise_verba import carregar_e_analisar_verbas
from analise_verba_devolucao import carregar_analisar_verba_devolucao


def criar_verba_unificada():
    """
    Carrega e unifica os DataFrames de Verbas de Acompanhamento e Devolução.

    Retorna:
        pd.DataFrame: Um DataFrame unificado com as colunas chave.
    """
    
    # 1. Carregar e processar o DataFrame de Acompanhamento (Verbas a Aplicar)
    # df_acompanhamento possui as colunas de status: STATUS_VERBA (QUITADO/PENDENTE) e STATUS_VENCIMENTO (VENCIDA/A VENCER)
    df_acompanhamento = carregar_e_analisar_verbas()

    if df_acompanhamento.empty:
        print("Aviso: DataFrame de Acompanhamento vazio.")
    
    # Selecionar e renomear colunas para padronização
    df_acompanhamento_final = df_acompanhamento.rename(columns={
        'VALOR_PENDENTE': 'VALOR_PENDENTE_UNIFICADO',
        'STATUS_VENCIMENTO': 'STATUS_UNIFICADO', # VENCIDA / A VENCER
        'DATA_VENCIMENTO': 'DATA_VENCIMENTO_UNIFICADO',
    })
    
    # Adicionar coluna de TIPO
    df_acompanhamento_final['TIPO_VERBA'] = 'ACOMPANHAMENTO'


    # 2. Carregar e processar o DataFrame de Devolução
    # df_devolucao possui as colunas de status: STATUS_VERBA (QUITADA/PENDENTE) e STATUS_VENCIDOS (QUITADA/VENCIDA/A VENCER)
    df_devolucao = carregar_analisar_verba_devolucao()

    if df_devolucao.empty:
        print("Aviso: DataFrame de Devolução vazio.")

    # Filtrar apenas as Devoluções PENDENTES
    df_devolucao_pendente = df_devolucao[df_devolucao['STATUS_VERBA'] == 'PENDENTE'].copy()

    # Selecionar e renomear colunas para padronização
    df_devolucao_final = df_devolucao_pendente.rename(columns={
        'VALOR_VERBA_DEVOLUCAO': 'VALOR_PENDENTE_UNIFICADO',
        'STATUS_VENCIDOS': 'STATUS_UNIFICADO', # VENCIDA / A VENCER (excluindo o 'QUITADA' pelo filtro)
        'DATA_VENCIMENTO': 'DATA_VENCIMENTO_UNIFICADO',
    })
    
    # Adicionar coluna de TIPO
    df_devolucao_final['TIPO_VERBA'] = 'DEVOLUCAO'

    # 3. Concatenar os DataFrames
    # Mapeamento de colunas para inclusão final
    COLUNAS_SELECIONADAS = [
        'TIPO_VERBA',
        'CLASSIFICACAO',
        'COMPRADOR',
        'FORNECEDOR',
        'FILIAL',
        'STATUS_UNIFICADO', 
        'DIAS_VENCIDOS',
        'DATA_VENCIMENTO_UNIFICADO',
        'VALOR_PENDENTE_UNIFICADO',
    ]
    
    # Filtra e padroniza as colunas de cada DF
    df_acompanhamento_final = df_acompanhamento_final[COLUNAS_SELECIONADAS]
    # O df de devolução precisa de uma pequena correção na coluna 'STATUS_UNIFICADO',
    # pois a função de análise de devolução retorna 'QUITADA' na coluna STATUS_VENCIDOS.
    # No entanto, como filtramos apenas PENDENTES, os valores são 'VENCIDA' ou 'A VENCER'.
    df_devolucao_final = df_devolucao_final[COLUNAS_SELECIONADAS]

    df_unificado = pd.concat([df_acompanhamento_final, df_devolucao_final], ignore_index=True)

    # 4. Cálculos e Limpezas Finais
    
    # Remove qualquer linha onde o VALOR_PENDENTE_UNIFICADO seja 0 após a unificação (para limpeza)
    df_unificado = df_unificado[df_unificado['VALOR_PENDENTE_UNIFICADO'] > 0]
    
    df_unificado['DATA_VENCIMENTO_UNIFICADO'] = df_unificado['DATA_VENCIMENTO_UNIFICADO'].dt.strftime('%d/%m/%Y')

    return df_unificado


def resumo_unificado_por_comprador(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria um resumo consolidado do saldo pendente por comprador, classificando 
    em Acompanhamento vs. Devolução e Vencido vs. A Vencer.
    """
    if df.empty:
        return pd.DataFrame()
    
    df_resumo = df.groupby(['CLASSIFICACAO', 'COMPRADOR', 'TIPO_VERBA', 'STATUS_UNIFICADO']).agg(
        SALDO_PENDENTE=('VALOR_PENDENTE_UNIFICADO', 'sum'),
        QTD_REGISTROS=('TIPO_VERBA', 'size')
    ).reset_index()
    
    df_resumo = df_resumo.sort_values(by='SALDO_PENDENTE', ascending=False)
    
    return df_resumo


if __name__ == '__main__':
    print("--- INICIANDO ANÁLISE DE VERBAS UNIFICADA ---")
    df_unificado = criar_verba_unificada()
    
    if not df_unificado.empty:
        print("Dados Unificados e Processados com sucesso.")
        print("\n--- Saldo Total Pendente ---")
        saldo_total = df_unificado['VALOR_PENDENTE_UNIFICADO'].sum()
        print(f"R$ {saldo_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        print("\n--- Resumo Top 5 (Verba/Devolução) ---")
        df_resumo = resumo_unificado_por_comprador(df_unificado)
        print(df_resumo.head(5))
        
        # Opcional: Salvar o resultado unificado para uso em um novo dashboard Streamlit
        df_unificado.to_csv('dados_verbas_unificadas.csv', index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print("\nArquivo unificado salvo como 'dados_verbas_unificadas.csv'")

    else:
        print("Análise abortada devido a erro no carregamento ou dados vazios.")