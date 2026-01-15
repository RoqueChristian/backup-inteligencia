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


sql_total_day_rca = """
    SELECT
        c.codfilial AS filial,
        c.codusur AS cod_rca,
        u.nome AS nm_rca,
        s.codsupervisor,
        s.nome AS supervisor,
        c.codcli AS cod_cliente,
        cli.cliente AS nm_cliente,
        i.codprod AS cod_produto,
        p.descricao AS nm_produto,
        i.qt AS qt_vendida,
        i.pvenda AS vl_venda,
        i.posicao AS posicao_pedido,
        pm.DESCRICAORESUMIDA
    FROM
        pcpedc c
    INNER JOIN
        pcpedi i ON i.numped = c.numped
    LEFT JOIN
        pcpromocaomed pm ON pm.codpromocaomed = i.codpromocaomed
    LEFT JOIN 
        pcusuari u ON u.codusur = c.codusur
    LEFT JOIN 
        pcsuperv s ON s.codsupervisor = u.codsupervisor
    INNER JOIN
        pcclient cli ON cli.codcli = c.codcli 
    INNER JOIN
        pcprodut p ON p.codprod = i.codprod 
    WHERE
        c.data = TO_DATE('2025-12-11', 'YYYY-MM-DD') --- AND TO_DATE('2025-12-12', 'YYYY-MM-DD')
        AND c.condvenda IN (1, 5)
"""
sql_total_day_televenda = """
    SELECT
        c.codfilial as filial,
        c.codemitente as cod_televenda,
        e.nome as nm_televenda,
        s.codsupervisor,
        s.nome as supervisor,
        c.codcli as cod_cliente,
        cli.cliente as nm_cliente,
        i.codprod as cod_produto,
        p.descricao as nm_produto,
        i.qt as qt_vendida,
        i.pvenda as vl_venda,
        i.posicao as posicao_pedido
    from pcpedc c
    left join pcpedi i on i.numped = c.numped
    left join pcempr e on e.matricula = c.codemitente
    left join pcclient cli on cli.codcli = c.codcli
    left join pcprodut p on p.codprod = i.codprod
    left join pcusuari u on u.codusur = e.codusur
    left join pcsuperv s on s.codsupervisor = u.codsupervisor
    where c.data = TO_DATE('2025-12-11', 'YYYY-MM-DD')
        and c.condvenda in (1,5) 
        AND c.codemitente <> 8888
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

        df_total_day_rca = pd.read_sql(sql_total_day_rca, con=connection)
        
        NOME_ARQUIVO_VERBA = 'total_day_rcca.csv'
        verificar_e_apagar_csv(NOME_ARQUIVO_VERBA)
        df_total_day_rca.to_csv(NOME_ARQUIVO_VERBA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print(f' Relatório de Acompanhamento RCA salvo como {NOME_ARQUIVO_VERBA}.')

        df_total_day_televenda = pd.read_sql(sql_total_day_televenda, con=connection)
        
        NOME_ARQUIVO_DEVOLUCAO = 'total_day_televenda.csv'
        verificar_e_apagar_csv(NOME_ARQUIVO_DEVOLUCAO) 
        df_total_day_televenda.to_csv(NOME_ARQUIVO_DEVOLUCAO, index=False, sep=';', encoding='utf-8-sig', decimal=',') 
        print(f' Relatório de Acompanhamento televendas salvo como {NOME_ARQUIVO_DEVOLUCAO}.')


except oracledb.Error as e:
    error_obj = e.args[0]
    print(f'Erro ao se conectar ou executar a query no banco Oracle: {error_obj.code}:{error_obj.message}')
    print('Verificar as credenciais, DNS e o status do servidor.')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {e}')