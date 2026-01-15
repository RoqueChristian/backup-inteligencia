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

sql_vlultent = """
    SELECT DISTINCT
        p.codprod,
        e.valorultent,
        e.codfilial,
        CASE
        WHEN f.classificacao = 'F' THEN 'FARMA'
        WHEN f.classificacao = 'H' THEN 'HB'
        ELSE  'Outros'
        END as classificacao
    FROM pcprodut p
    LEFT JOIN pcfornec f
        ON p.codfornec = f.codfornec
    LEFT JOIN pcest e
        ON p.codprod = e.codprod
"""


sql_acompanhamento_verba = """
    SELECT 
        k1.CODIGOFILIAL,
        K1.CLASSIFICACAO,
        TRUNC(k1.DATACADASTRO) AS DATACADASTRO,
        k1.CODIGOFORNECEDOR,
        K1.FORNECEDOR,
        k1.CODCOMPRADOR,
        K1.COMPRADOR,
        k1.CODIGOCONTA,
        k1.NUMEROVERBA,
        k1.NUMNOTA,
        k1.NUMEROTRANSVENDA,
        TRUNC(k1.DATAVENCIMENTO) AS DATAVENCIMENTO,
        k1.REFERENCIA,
        k1.REFERENCIA1,
        k1.SITUACAO,
        k1.VALOR AS VALOR_VERBA,
        k2.VALORAPLICADO,
        k2.ESTORNOAPLIC,
        k4.VALORDEBITO,
        k4.VALORCREDITO,
        k4.ESTORNOVERBA
    FROM (
        SELECT 
            PCVERBA.CODFILIAL AS CODIGOFILIAL,
            CASE
                WHEN PCFORNEC.CLASSIFICACAO = 'F' THEN 'FARMA'
                WHEN PCFORNEC.CLASSIFICACAO = 'H' THEN 'HB'
                ELSE 'OUTROS'
            END AS CLASSIFICACAO,
            PCVERBA.DTCADASTRO AS DATACADASTRO,     
            PCVERBA.CODFORNEC AS CODIGOFORNECEDOR,
            PCFORNEC.FORNECEDOR AS FORNECEDOR,
            PCFORNEC.CODCOMPRADOR AS CODCOMPRADOR,
            PCEMPR.NOME AS COMPRADOR,
            TO_NUMBER(PCVERBA.CODCONTA) AS CODIGOCONTA,
            PCVERBA.NUMVERBA AS NUMEROVERBA,
            PCVERBA.NUMNOTA AS NUMNOTA,
            PCVERBA.NUMTRANSENTDEVFORNEC AS NUMEROTRANSVENDA,
            PCVERBA.DTVENC AS DATAVENCIMENTO,
            NVL(PCVERBA.REFERENCIA, ' ') AS REFERENCIA,
            NVL(PCVERBA.REFERENCIA1, ' ') AS REFERENCIA1,
            CASE 
                WHEN PCVERBA.DTCANCEL IS NULL THEN 'ATIVA'
                ELSE 'CANCELADA'
            END AS SITUACAO,
            PCVERBA.VALOR
        FROM PCVERBA
        LEFT JOIN PCFORNEC 
            ON PCFORNEC.CODFORNEC = PCVERBA.CODFORNEC
        LEFT JOIN PCEMPR
            ON PCEMPR.MATRICULA = PCFORNEC.CODCOMPRADOR
        WHERE PCFORNEC.CLASSIFICACAO IN ('F','H') 
    ) k1
    LEFT JOIN (
        SELECT 
            PCAPLICVERBA.NUMVERBA AS NUMVERBAPLIC,
            SUM(PCAPLICVERBA.VLAPLIC) AS VALORAPLICADO,
            CASE 
                WHEN PCAPLICVERBA.DTESTORNO IS NULL THEN 'N'
                ELSE 'S'
            END AS ESTORNOAPLIC
        FROM PCAPLICVERBA
        GROUP BY 
            PCAPLICVERBA.NUMVERBA,
            PCAPLICVERBA.DTESTORNO
    ) k2
        ON k1.NUMEROVERBA = k2.NUMVERBAPLIC
    INNER JOIN (
        SELECT
            PCMOVCRFOR.NUMVERBA AS NUMVERBALANC,
            SUM(
                CAST(DECODE(PCMOVCRFOR.TIPO, 'D', PCMOVCRFOR.VALOR, 0) AS NUMERIC(18,6))
            ) AS VALORDEBITO,
            SUM(
                CAST(DECODE(PCMOVCRFOR.TIPO, 'C', PCMOVCRFOR.VALOR * (-1), 0) AS NUMERIC(18,6))
            ) AS VALORCREDITO,
            CASE 
                WHEN PCMOVCRFOR.DTESTORNO IS NULL THEN 'N'
                ELSE 'S'
            END AS ESTORNOVERBA
        FROM PCMOVCRFOR
        GROUP BY 
            PCMOVCRFOR.NUMVERBA,
            PCMOVCRFOR.DTESTORNO
    ) k4
        ON k1.NUMEROVERBA = k4.NUMVERBALANC
"""
sql_acompanhamento_verba_devolucao = """
    SELECT
        CASE
        WHEN F.classificacao = 'F' THEN 'FARMA'
        WHEN F.classificacao = 'H' THEN 'HB'
        ELSE 'OUTROS'
        END AS classificacao,
        P.codfilial AS filial,
        F.codfornec,
        F.fornecedor,
        E.nome AS comprador,
        SUM(P.VALOR) AS valor_verba_devolucao,
        TO_CHAR(P.dtemissao, 'DD/MM/YYYY') AS dtemissao,
        TO_CHAR(P.DTVENC, 'DD/MM/YYYY') AS data_vencimento
    FROM
        PCPREST P
    INNER JOIN
        PCCOB B ON P.CODCOB = B.CODCOB
    INNER JOIN
        PCCLIENT C ON P.CODCLI = C.CODCLI
    INNER JOIN
        PCFORNEC F ON C.CODCLI = F.CODCLI
    LEFT JOIN
        PCPRACA A ON C.CODPRACA = A.CODPRACA
    INNER JOIN
        PCUSUARI U ON P.CODUSUR = U.CODUSUR
    INNER JOIN
        PCSUPERV S ON NVL(P.CODSUPERVISOR, U.CODSUPERVISOR) = S.CODSUPERVISOR
    INNER JOIN
        PCEMPR E ON E.MATRICULA = F.CODCOMPRADOR
    WHERE
        P.DTPAG IS NULL
        AND EXTRACT(YEAR FROM P.dtemissao) >= 2025
        AND P.CODCOB <> 'DESD'
        AND P.VALOR <> 0
        AND P.CODCOB NOT IN ('DEVP', 'DEVT', 'BNF', 'BNFT', 'BNFR', 'BNTR', 'BNRP', 'CRED')
        AND EXISTS (
            SELECT 1
            FROM PCLIB
            WHERE
                CODTABELA = '8'
                AND CODFUNC = 608
                AND PCLIB.CODIGOA IS NOT NULL
                AND (CODIGOA = NVL(P.CODCOB, CODIGOA) OR CODIGOA = '9999')
        )
        AND EXISTS (
            SELECT 1
            FROM PCLIB
            WHERE
                CODTABELA = '7'
                AND CODFUNC = 608
                AND PCLIB.CODIGOA IS NOT NULL
                AND (CODIGON = NVL(NVL(P.CODSUPERVISOR, U.CODSUPERVISOR), CODIGON) OR CODIGON = 9999)
        )
    GROUP BY
        P.DTVENC,
        F.CODFORNEC,
        F.FORNECEDOR,
        P.CODFILIAL,
        F.CLASSIFICACAO,
        P.DTPAG,
        P.dtemissao,
        E.nome
    ORDER BY
        P.DTVENC
"""

sql_cliente = """
    WITH RawData AS (
        SELECT
            codcli,
            cliente,
            fantasia,
            codcliprinc,
            -- 1. Limpeza agressiva e remoção de espaços
            REGEXP_REPLACE(TRIM(cgcent), '[^0-9]', '') AS documento_bruto
        FROM pcclient
    )
    SELECT
        codcli AS cod_cliente,
        cliente AS nm_cliente,
        fantasia,
        codcliprinc,
        -- 2. Garantia de tipo String e tratamento de zeros à esquerda
        CASE 
            WHEN LENGTH(documento_bruto) <= 11 THEN LPAD(documento_bruto, 11, '0')
            ELSE LPAD(documento_bruto, 14, '0')
        END AS documento_limpo,
        -- 3. Flag de metadados para governança
        CASE 
            WHEN LENGTH(documento_bruto) <= 11 THEN 'CPF'
            ELSE 'CNPJ'
        END AS tp_documento
    FROM RawData
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

        df_vlultent = pd.read_sql(sql_vlultent, con=connection)

        Nome_arquivo_vlultent = 'valorultent.csv'
        verificar_e_apagar_csv(Nome_arquivo_vlultent)
        df_vlultent.to_csv(Nome_arquivo_vlultent, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print(f' Relatório de vlultent salvo como {Nome_arquivo_vlultent}.')

        # df_acompanhamento_verba = pd.read_sql(sql_acompanhamento_verba, con=connection)
        
        # NOME_ARQUIVO_VERBA = 'dados_acompanhamento_verba.csv'
        # verificar_e_apagar_csv(NOME_ARQUIVO_VERBA)
        # df_acompanhamento_verba.to_csv(NOME_ARQUIVO_VERBA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        # print(f' Relatório de Acompanhamento de Verba salvo como {NOME_ARQUIVO_VERBA}.')

        # df_acompanhamento_verba_devolucao = pd.read_sql(sql_acompanhamento_verba_devolucao, con=connection)
        
        # NOME_ARQUIVO_DEVOLUCAO = 'dados_acompanhamento_verba_devolucao.csv'
        # verificar_e_apagar_csv(NOME_ARQUIVO_DEVOLUCAO) 
        # df_acompanhamento_verba_devolucao.to_csv(NOME_ARQUIVO_DEVOLUCAO, index=False, sep=';', encoding='utf-8-sig', decimal=',') 
        # print(f' Relatório de Devolução de Verba salvo como {NOME_ARQUIVO_DEVOLUCAO}.')

        # NOME_ARQUIVO_CLIENTE= 'dados_cliente.csv'
        # df_cliente = pd.read_sql(sql_cliente, con=connection)
        # verificar_e_apagar_csv(NOME_ARQUIVO_CLIENTE)
        # df_cliente.to_csv(NOME_ARQUIVO_CLIENTE, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        # print(f' Relatório de Cliente salvo como {NOME_ARQUIVO_CLIENTE}.')


except oracledb.Error as e:
    error_obj = e.args[0]
    print(f'Erro ao se conectar ou executar a query no banco Oracle: {error_obj.code}:{error_obj.message}')
    print('Verificar as credenciais, DNS e o status do servidor.')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {e}')