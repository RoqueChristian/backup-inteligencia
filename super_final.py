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


sql_rac = """
    SELECT
        m.codfilial AS FILIAL,
        CASE 
            WHEN p.TIPOFV= 'PE' AND p.ORIGEMPED = 'F' THEN 'PEDIDO ELETRÔNICO'
            WHEN p.TIPOFV != 'PE' AND p.TIPOFV  = 'OL' AND p.ORIGEMPED = 'F' THEN 'OPERADOR LOGÍSTICO'
            WHEN p.TIPOFV is null  AND p.ORIGEMPED = 'F' THEN 'FORÇA DE VENDAS'
            WHEN p.ORIGEMPED = 'T' THEN 'TELEMARKETING'
            WHEN p.ORIGEMPED = 'W' THEN 'E-COMMERCE' 
        ELSE 'OUTROS' END AS ORIGEMPEDIDO,
        m.codusur AS CODIGO_RCA,
        u.nome as RCA,
        s.nome as SUPERVISOR,
        m.codprod,
        SUM(ROUND(((NVL(m.PUNIT, 0) + NVL(m.VLOUTRASDESP, 0) + NVL(m.VLFRETE_RATEIO, 0) +
        NVL(m.VLOUTROS, 0)) - NVL(m.VLIPI,0) - NVL(m.ST,0) - NVL(m.VLREPASSE,0) +
        NVL(m.VLIPI,0) + NVL(m.ST,0)) * NVL(m.QT,0),2)) AS FATURAMENTO
    FROM pcmov m
    INNER JOIN pcusuari u on u.codusur = m.codusur
    LEFT JOIN pcsuperv s on s.codsupervisor = u.codsupervisor
    LEFT JOIN pcpedc p on p.numped = m.numped
    WHERE 
        m.dtmov BETWEEN TO_DATE('01/11/2025', 'DD/MM/YYYY') AND TO_DATE('30/11/2025', 'DD/MM/YYYY')
        AND m.dtcancel IS NULL AND m.codfilial <> 10 AND m.CODOPER = 'S' AND p.codemitente = 8888
    GROUP BY 
        m.codfilial,
        u.nome,
        s.nome,
        m.codprod,
        m.codusur,
        p.TIPOFV,
        p.ORIGEMPED
"""

sql_telev = """
    SELECT
        m.codfilial AS filial,
        M.CODPROD,
        EXTRACT(DAY FROM m.dtmov) AS dia_faturamento,
        p.codemitente codigo_rca,
        SUM(ROUND(((NVL(m.PUNIT, 0) + NVL(m.VLOUTRASDESP, 0) + NVL(m.VLFRETE_RATEIO, 0)+
        NVL(m.VLOUTROS, 0))- NVL(m.VLIPI,0) - NVL(m.ST,0)- NVL(m.VLREPASSE,0)+
        NVL(m.VLIPI,0) + NVL(m.ST,0)) * NVL(m.QT,0),2)) AS faturamento
    FROM pcmov m
    INNER JOIN pcpedc p ON p.numped = m.numped
    WHERE 
        m.dtmov BETWEEN TO_DATE('01/11/2025', 'DD/MM/YYYY') AND TO_DATE('30/11/2025', 'DD/MM/YYYY') AND CODOPER = 'S'
        AND m.dtcancel IS NULL AND m.codfilial <> 10 AND p.origemped = 'T' AND p.dtcancel IS NULL AND p.codemitente <> 8888
    GROUP BY 
        EXTRACT(DAY FROM m.dtmov),
        m.codfilial,
        M.CODPROD,
        p.codemitente
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

        df_rca = pd.read_sql(sql_rac, con=connection)
        
        NOME_ARQUIVO_VERBA = 'dados_rca_prod.csv'
        verificar_e_apagar_csv(NOME_ARQUIVO_VERBA)
        df_rca.to_csv(NOME_ARQUIVO_VERBA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print(f' Relatório de Acompanhamento de Verba salvo como {NOME_ARQUIVO_VERBA}.')

        df_telev = pd.read_sql(sql_telev, con=connection)
        
        NOME_ARQUIVO_DEVOLUCAO = 'dados_telev_prod.csv'
        verificar_e_apagar_csv(NOME_ARQUIVO_DEVOLUCAO) 
        df_telev.to_csv(NOME_ARQUIVO_DEVOLUCAO, index=False, sep=';', encoding='utf-8-sig', decimal=',') 
        print(f' Relatório de Devolução de Verba salvo como {NOME_ARQUIVO_DEVOLUCAO}.')


except oracledb.Error as e:
    error_obj = e.args[0]
    print(f'Erro ao se conectar ou executar a query no banco Oracle: {error_obj.code}:{error_obj.message}')
    print('Verificar as credenciais, DNS e o status do servidor.')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {e}')