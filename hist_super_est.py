import pandas as pd
import oracledb
import os
from dotenv import load_dotenv

# --- CONFIGURAÇÕES ---
load_dotenv()

ORACLE_LIB_DIR = r"C:\instantclient_23_9" 
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_DSN = os.getenv('ORACLE_DSN', '192.168.0.1/wint')

try:
    oracledb.init_oracle_client(lib_dir=ORACLE_LIB_DIR)
    print("✅ Oracle Thick Mode inicializado.")
except Exception as e:
    print(f"⚠️ Aviso: {e}")

# --- SQL ---
SQL_EXCESSO_MENSAL = """
WITH base_date AS (
    SELECT TO_DATE('01/' || LPAD(:mes_ref, 2, '0') || '/2025', 'DD/MM/YYYY') AS dt_ref FROM DUAL
),
dias_estoque_zerado AS (
    SELECT
        codfilial,
        codprod,
        SUM(CASE WHEN qtestger <= 0 THEN 1 ELSE 0 END) AS qtd_dias_zerados
    FROM pchistest, base_date bd
    WHERE data >= bd.dt_ref - 90 AND data < bd.dt_ref
    GROUP BY codfilial, codprod
),
vendas_ultimos_90_dias AS (
    SELECT
        codfilial,
        codprod,
        SUM(qt) AS qtd_vendas_90d
    FROM pcmov, base_date bd
    WHERE codoper = 'S'
      AND dtmov >= bd.dt_ref - 90 AND dtmov < bd.dt_ref
      AND dtcancel IS NULL
    GROUP BY codfilial, codprod
),
estoque_posicional AS (
    SELECT
        he.codfilial,
        he.codprod,
        he.qtestger AS qtd_estoque_dia_primeiro,
        he.custorep AS valor_ultima_entrada,
        p.descricao AS produto,
        CASE 
            WHEN f.classificacao = 'F' THEN 'Farma'
            WHEN f.classificacao = 'H' THEN 'HB'
            ELSE 'Outros'
        END AS classificacao,
        cat.categoria,
        sec.descricao AS secao,
        dep.descricao AS departamento,
        f.codcomprador,
        ep.nome AS comprador,
        f.codfornec,
        f.fornecedor
    FROM pchistest he
    JOIN base_date bd ON he.data = bd.dt_ref
    INNER JOIN pcprodut p    ON p.codprod = he.codprod
    INNER JOIN pcfornec f    ON f.codfornec = p.codfornec
    LEFT JOIN pcdepto dep    ON dep.codepto = p.codepto
    LEFT JOIN pcsecao sec    ON sec.codsec = p.codsec AND sec.codepto = dep.codepto
    LEFT JOIN pccategoria cat ON cat.codcategoria = p.codcategoria AND cat.codsec = sec.codsec
    LEFT JOIN pcempr ep      ON ep.matricula = f.codcomprador
    WHERE TRIM(UPPER(dep.descricao) NOT IN ('IMOBILIZADO', 'ESTOQUE DE INSUMOS', 'MATERIAIS DE CONSUMO')
),
produtos_novos AS (
    SELECT codfilial, codprod, dtprimcompra
    FROM pcest, base_date bd 
    WHERE dtprimcompra >= bd.dt_ref - 120
      AND dtprimcompra < bd.dt_ref
)
SELECT
    e.codfilial,
    e.classificacao,
    e.codprod,
    e.produto,
    e.categoria,
    e.secao,
    e.departamento,
    e.codcomprador,
    e.comprador,
    e.codfornec,
    e.fornecedor,
    e.valor_ultima_entrada,
    e.qtd_estoque_dia_primeiro AS qtd_total_estoque,
    
    NVL(v.qtd_vendas_90d, 0) AS qtd_vendas_90d,
    NVL(d.qtd_dias_zerados, 0) AS qtd_dias_zerados,
    
    ROUND(e.qtd_estoque_dia_primeiro * e.valor_ultima_entrada, 2) AS valor_estoque,
    
    -- VMD Corrigida: Venda / (90 dias - dias zerados)
    ROUND(
        CASE
            WHEN (90 - NVL(d.qtd_dias_zerados, 0)) <= 0 THEN 0
            ELSE NVL(v.qtd_vendas_90d, 0) / (90 - NVL(d.qtd_dias_zerados, 0))
        END, 4) AS vmd_corrigida,

    -- VALOR EXCEDENTE BASEADO NA VMD CORRIGIDA
    ROUND(
        GREATEST(0,
            CASE
                WHEN n.codprod IS NOT NULL THEN 0 -- Isenta novos
                WHEN NVL(v.qtd_vendas_90d, 0) = 0 THEN (e.qtd_estoque_dia_primeiro * e.valor_ultima_entrada) -- Sem venda = 100% Excesso
                
                WHEN e.classificacao = 'Farma' THEN
                    (e.qtd_estoque_dia_primeiro - ( (NVL(v.qtd_vendas_90d, 0) / NULLIF(90 - NVL(d.qtd_dias_zerados, 0), 0)) * 90) ) * e.valor_ultima_entrada
                
                WHEN e.classificacao = 'HB' THEN
                    (e.qtd_estoque_dia_primeiro - ( (NVL(v.qtd_vendas_90d, 0) / NULLIF(90 - NVL(d.qtd_dias_zerados, 0), 0)) * 60) ) * e.valor_ultima_entrada
                ELSE 0
            END
        ), 2) AS valor_excedente,

    CASE WHEN n.codprod IS NOT NULL THEN 'Sim' ELSE 'Não' END AS is_produto_novo,
    :mes_ref AS mes
FROM estoque_posicional e
LEFT JOIN vendas_ultimos_90_dias v ON e.codfilial = v.codfilial AND e.codprod = v.codprod
LEFT JOIN dias_estoque_zerado d   ON e.codfilial = d.codfilial AND e.codprod = d.codprod
LEFT JOIN produtos_novos n        ON e.codfilial = n.codfilial AND e.codprod = n.codprod
--WHERE NVL(v.qtd_vendas_90d, 0) > 0 
ORDER BY e.codfilial, valor_excedente DESC
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

def processar_anual():
    lista_final = []
    
    try:
        print(f"🚀 Iniciando conexão com {ORACLE_DSN}...")
        with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN) as conn:
            
            for mes in range(1, 13):
                print(f"📊 Processando Mês: {mes:02d}/2025...", end="\r")
                df_mes = pd.read_sql(SQL_EXCESSO_MENSAL, con=conn, params={'mes_ref': mes})
                
                if not df_mes.empty:
                    lista_final.append(df_mes)
            
            if not lista_final:
                print("\n❌ Nenhum dado encontrado.")
                return

            print("\n🔄 Consolidando resultados...")
            df_final = pd.concat(lista_final, ignore_index=True)
            

            df_final = df_final.sort_values(by=['MES', 'CODFILIAL'])
            
            nome_arquivo = 'excesso_estoque_completo_2025.csv'
            verificar_e_apagar_csv(nome_arquivo)
            df_final.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8-sig', decimal=',')
            
            print(f"✨ Sucesso! Arquivo gerado: {nome_arquivo}")
            print(f"📈 Colunas geradas: {list(df_final.columns)}")

    except Exception as e:
        print(f"\n❌ Erro: {e}")

if __name__ == "__main__":
    processar_anual()