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


sql_super_estoque = """
    WITH dias_estoque_zerado AS (
SELECT
   codfilial,
   codprod,
   SUM(CASE WHEN QTESTGER = 0 THEN 1 ELSE 0 END) AS qtd_dias_zerados
FROM
   PCHISTEST
WHERE
   DATA >= TRUNC(SYSDATE) - 90
GROUP BY
   codfilial,
   codprod
),
vendas_ultimos_90_dias AS (
SELECT
   codfilial,
   codprod,
   SUM(qt) AS qtd_vendas_90d
FROM
   pcmov
WHERE
   codoper = 'S'
   AND dtmov >= TRUNC(SYSDATE) - 90
GROUP BY
   codfilial,
   codprod
),
estoque AS (
SELECT
    e.codfilial,
    CASE 
        WHEN f.classificacao = 'F' THEN 'Farma'
        WHEN f.classificacao = 'H' THEN 'HB'
        ELSE 'Outros'
    END AS classificacao,
    e.codprod,
    p.descricao AS produto,
    cat.categoria,
    sec.descricao AS secao,
    dep.descricao AS departamento,
    f.codcomprador as cod_comprador,
    ep.nome as comprador,
    f.codfornec,
    f.fornecedor,
    e.custoultent AS valor_ultima_entrada,
    SUM(e.qtestger) AS qtd_total_estoque,
    SUM(e.qtgirodia) AS qtd_giro_dia,
    ROUND(
        CASE 
            WHEN SUM(e.qtgirodia) > 0 THEN SUM(e.qtestger) / SUM(e.qtgirodia)
            ELSE 0 
        END, 2
    ) AS cobertura_estoque
FROM pcest e
INNER JOIN pcprodut p    ON p.codprod = e.codprod
INNER JOIN pcfornec f    ON f.codfornec = p.codfornec
LEFT JOIN pcdepto dep    ON dep.codepto = p.codepto
LEFT JOIN pcsecao sec    ON sec.codsec = p.codsec 
                        AND sec.codepto = dep.codepto
LEFT JOIN pccategoria cat ON cat.codcategoria = p.codcategoria 
                         AND cat.codsec = sec.codsec
LEFT JOIN pcempr ep on ep.matricula = f.codcomprador
WHERE e.codfilial <> 10 
  AND e.qtestger <> 0 
  AND f.classificacao IN ('F', 'H')
GROUP BY
    e.codfilial,
    f.classificacao,
    e.codprod,
    p.descricao,
    cat.categoria,
    sec.descricao,
    dep.descricao,
    f.codfornec,
    f.fornecedor,
    f.codcomprador,
    e.custoultent,
    ep.nome
),
produtos_primeira_entrada_menor_120_dias as (
SELECT
   codfilial,
   codprod,
   dtprimcompra
FROM pcest
WHERE dtprimcompra >= TRUNC(SYSDATE) - 120
)
SELECT
e.codfilial,
e.classificacao,
e.codprod,
e.produto,
e.categoria,
e.secao,
e.departamento,
e.cod_comprador,
e.comprador,
e.codfornec,
e.fornecedor,
e.valor_ultima_entrada,
e.qtd_total_estoque,
e.qtd_giro_dia,
e.cobertura_estoque AS cobertura_estoque_original,

-- COBERTURA AJUSTADA
CASE
   WHEN NVL(v.qtd_vendas_90d, 0) = 0 THEN 9999
   ELSE e.cobertura_estoque
END AS cobertura_estoque_ajustada, 

NVL(v.qtd_vendas_90d, 0) AS qtd_vendas_90d,
NVL(d.qtd_dias_zerados, 0) AS qtd_dias_zerados,
ROUND(e.qtd_total_estoque * e.valor_ultima_entrada, 2) AS valor_estoque,
ROUND(
   CASE
   WHEN (90 - NVL(d.qtd_dias_zerados, 0)) <= 0 THEN NULL
   ELSE NVL(v.qtd_vendas_90d, 0) / (90 - NVL(d.qtd_dias_zerados, 0))
   END,
2) AS giro_medio_corrigido,

ROUND(
GREATEST(
   0,
   CASE
       WHEN n.codfilial IS NOT NULL AND n.codprod IS NOT NULL THEN 0
       WHEN NVL(v.qtd_vendas_90d, 0) = 0 THEN 0 
       WHEN e.cobertura_estoque IS NULL OR e.cobertura_estoque = 0 THEN 0
       WHEN e.classificacao = 'Farma'
       THEN (
                   (ROUND(e.qtd_total_estoque * e.valor_ultima_entrada, 2) / e.cobertura_estoque)
               ) * (e.cobertura_estoque - 90)
       WHEN e.classificacao = 'HB'
       THEN (
                   (ROUND(e.qtd_total_estoque * e.valor_ultima_entrada, 2) / e.cobertura_estoque)
               ) * (e.cobertura_estoque - 60)
       ELSE 0
   END
),
2) AS VALOR_EXCEDENTE,
CASE WHEN n.codfilial IS NOT NULL THEN 'Sim' ELSE 'Não' END AS is_produto_novo,
n.dtprimcompra AS data_primeira_compra

FROM
estoque e
LEFT JOIN
   vendas_ultimos_90_dias v
   ON e.codfilial = v.codfilial AND e.codprod = v.codprod
LEFT JOIN
   dias_estoque_zerado d
   ON e.codfilial = d.codfilial AND e.codprod = d.codprod
LEFT JOIN
   produtos_primeira_entrada_menor_120_dias n 
   ON e.codfilial = n.codfilial AND e.codprod = n.codprod 
ORDER BY
   e.codfilial,
   e.codprod
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

        df_super_estoque = pd.read_sql(sql_super_estoque, con=connection)
        
        NOME_ARQUIVO_VERBA = 'super_estoque.csv'
        verificar_e_apagar_csv(NOME_ARQUIVO_VERBA)
        df_super_estoque.to_csv(NOME_ARQUIVO_VERBA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print(f' Relatório de Acompanhamento Super Estoque salvo como{NOME_ARQUIVO_VERBA}.')
 

except oracledb.Error as e:
    error_obj = e.args[0]
    print(f'Erro ao se conectar ou executar a query no banco Oracle: {error_obj.code}:{error_obj.message}')
    print('Verificar as credenciais, DNS e o status do servidor.')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {e}')