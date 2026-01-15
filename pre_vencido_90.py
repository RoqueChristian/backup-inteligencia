import pandas as pd
import oracledb
from dotenv import load_dotenv
import os

try:
    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_9")
except Exception as e:
    print(f'Aviso: Não foi possível inicializar o cliente oracle: {e}')


load_dotenv()
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
dsn = '192.168.0.1/wint'


sql_pre_vencido_90 = """
    WITH DADOS_ESTOQUE AS (
        SELECT
            E.CODFILIAL,
            CASE WHEN f.classificacao = 'F' THEN 'Farma'
            WHEN f.classificacao = 'H' THEN 'HB'
            ELSE 'Outros' END AS classificacao,
            F.FORNECEDOR,
            S.CODPROD,
            P.DESCRICAO,
            -- SI.NUMLOTE, <--- LINHA REMOVIDA
            CASE
                WHEN (SI.QT IS NOT NULL) AND (P.ESTOQUEPORLOTE = 'S') THEN NVL(SI.QT, 0)
                WHEN (SI.DTVAL IS NOT NULL) AND (SI.QT IS NOT NULL)
                AND (PF.ESTOQUEPORDTVALIDADEPK = 'S') AND (SI.NUMLOTE = '1')
                AND (E.TIPOENDER = 'AP') THEN
                    (SELECT NVL(SUM(QT), 0) FROM PCESTENDERECOI
                    WHERE CODPROD = S.CODPROD AND CODENDERECO = S.CODENDERECO
                    AND NUMLOTE = '1' AND DTVAL = SI.DTVAL)
                ELSE NVL(S.QT, 0)
            END AS QT,
            ROUND(ES.CUSTOULTENT, 2) AS VALOR_ULTIMA_ENTRADA,
            TO_CHAR(NVL(SI.DTVAL, S.DTVAL), 'DD/MM/YYYY') AS DTVAL
        FROM
            PCESTENDERECO S 
        INNER JOIN PCENDERECO E ON S.CODENDERECO = E.CODENDERECO
        INNER JOIN PCPRODUT P ON S.CODPROD = P.CODPROD 
        INNER JOIN PCPRODFILIAL PF ON P.CODPROD = PF.CODPROD 
        INNER JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC 
        LEFT JOIN PCESTENDERECOI SI ON S.CODENDERECO = SI.CODENDERECO
                                            AND S.CODPROD = SI.CODPROD 
        LEFT JOIN PCMARCA M ON P.CODMARCA = M.CODMARCA 
        LEFT JOIN PCEST ES ON ES.CODPROD = P.CODPROD AND ES.CODFILIAL = E.CODFILIAL 
        WHERE
            S.QT > 0
            AND NVL(SI.DTVAL, S.DTVAL) BETWEEN TO_DATE('01/11/2025', 'DD/MM/YYYY') AND TO_DATE('31/07/2026', 'DD/MM/YYYY')
    )
    SELECT
        D.CODFILIAL, D.CLASSIFICACAO, D.FORNECEDOR, D.CODPROD, D.DESCRICAO,
        D.QT AS QUANTIDADE_REALIZADA, D.VALOR_ULTIMA_ENTRADA,
        ROUND(D.QT * D.VALOR_ULTIMA_ENTRADA, 2) AS TOTAL_REALIZADO
    FROM DADOS_ESTOQUE D
    WHERE D.CLASSIFICACAO <> 'Outros' AND D.QT > 0 
    GROUP BY
        D.CODFILIAL, D.CLASSIFICACAO, D.FORNECEDOR, D.CODPROD, D.DESCRICAO,
        D.QT, D.VALOR_ULTIMA_ENTRADA
"""

sql_pre_vencido = """
    WITH DADOS_ESTOQUE AS (
        SELECT
            E.CODFILIAL,
            CASE WHEN f.classificacao = 'F' THEN 'Farma'
            WHEN f.classificacao = 'H' THEN 'HB'
            ELSE 'Outros' END AS classificacao,
            F.FORNECEDOR,
            S.CODPROD,
            P.DESCRICAO,
            SI.NUMLOTE,
            CASE
                WHEN (SI.QT IS NOT NULL) AND (P.ESTOQUEPORLOTE = 'S') THEN NVL(SI.QT, 0)
                WHEN (SI.DTVAL IS NOT NULL) AND (SI.QT IS NOT NULL)
                AND (PF.ESTOQUEPORDTVALIDADEPK = 'S') AND (SI.NUMLOTE = '1')
                AND (E.TIPOENDER = 'AP') THEN
                    (SELECT NVL(SUM(QT), 0) FROM PCESTENDERECOI
                        WHERE CODPROD = S.CODPROD AND CODENDERECO = S.CODENDERECO
                        AND NUMLOTE = '1' AND DTVAL = SI.DTVAL)
                ELSE NVL(S.QT, 0)
            END AS QT,
            ROUND(ES.CUSTOULTENT, 2) AS VALOR_ULTIMA_ENTRADA,
            TO_CHAR(NVL(SI.DTVAL, S.DTVAL), 'DD/MM/YYYY') AS DTVAL
        FROM
            PCESTENDERECO S 
        INNER JOIN PCENDERECO E ON S.CODENDERECO = E.CODENDERECO
        INNER JOIN PCPRODUT P ON S.CODPROD = P.CODPROD 
        INNER JOIN PCPRODFILIAL PF ON P.CODPROD = PF.CODPROD 
        INNER JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC 
        LEFT JOIN PCESTENDERECOI SI ON S.CODENDERECO = SI.CODENDERECO
                                            AND S.CODPROD = SI.CODPROD 
        LEFT JOIN PCMARCA M ON P.CODMARCA = M.CODMARCA 
        LEFT JOIN PCEST ES ON ES.CODPROD = P.CODPROD AND ES.CODFILIAL = E.CODFILIAL 
        WHERE
            S.QT > 0
            AND NVL(SI.DTVAL, S.DTVAL) BETWEEN TO_DATE('01/11/2025', 'DD/MM/YYYY') AND TO_DATE('31/10/2026', 'DD/MM/YYYY')
    )
    SELECT
        D.CODFILIAL, D.CLASSIFICACAO, D.FORNECEDOR, D.CODPROD, D.DESCRICAO, D.NUMLOTE,
        D.QT AS QUANTIDADE_REALIZADA, D.VALOR_ULTIMA_ENTRADA,
        ROUND(D.QT * D.VALOR_ULTIMA_ENTRADA, 2) AS TOTAL_REALIZADO
    FROM DADOS_ESTOQUE D
    WHERE D.CLASSIFICACAO <> 'Outros' AND D.QT > 0 
    GROUP BY
        D.CODFILIAL, D.CLASSIFICACAO, D.FORNECEDOR, D.CODPROD, D.DESCRICAO, D.NUMLOTE,
        D.QT, D.VALOR_ULTIMA_ENTRADA
"""


def verificar_e_apagar_csv(nome_arquivo):
    if os.path.exists(nome_arquivo):
        try:
            os.remove(nome_arquivo)
            print(f' Arquivo antigo {nome_arquivo} apagado com sucesso.')
        except OSError as e:
            print(f' Erro ao apagar o arquivo {nome_arquivo}: {e}')
    else:
        print(f' Arquivo {nome_arquivo} não encontrado. Criando novo...')


try:
    with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn) as connection:
        print('Conexão com sucesso.')

        df_pre_vencidos_90 = pd.read_sql(sql_pre_vencido_90, con=connection)
        
        NOME_ARQUIVO_VERBA = 'dados_pre_vencidos_90.csv'
        verificar_e_apagar_csv(NOME_ARQUIVO_VERBA)
        df_pre_vencidos_90.to_csv(NOME_ARQUIVO_VERBA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print(f' Relatório de Acompanhamento de Verba salvo como {NOME_ARQUIVO_VERBA}.')

        # df_pre_vencidos = pd.read_sql(sql_pre_vencido, con=connection)
        
        # NOME_ARQUIVO_VERBA = 'dados_pre_vencidos.csv'
        # verificar_e_apagar_csv(NOME_ARQUIVO_VERBA)
        # df_pre_vencidos.to_csv(NOME_ARQUIVO_VERBA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        # print(f' Relatório de Acompanhamento de Verba salvo como {NOME_ARQUIVO_VERBA}.')


except oracledb.Error as e:
    error_obj = e.args[0]
    print(f'Erro ao se conectar ou executar a query no banco Oracle: {error_obj.code}:{error_obj.message}')
    print('Verificar as credenciais, DNS e o status do servidor.')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {e}')