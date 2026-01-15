import pandas as pd
import oracledb
from dotenv import load_dotenv
import os

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
dsn = '192.168.0.1/wint'

try:
    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_9")
except Exception as e:
    print(f'Aviso: Cliente Oracle: {e}')

# Queries 
queries = {
    'dim_produto.parquet': """
        SELECT PCPRODUT.CODPROD as cod_produto, PCPRODUT.DESCRICAO as nm_produto, 
        PCCATEGORIA.CATEGORIA as categoria, PCSECAO.DESCRICAO as secao, PCDEPTO.DESCRICAO as departamento
        FROM PCPRODUT
        LEFT JOIN pcdepto ON pcdepto.CODEPTO = PCPRODUT.CODEPTO
        LEFT JOIN PCSECAO ON PCSECAO.CODSEC = PCPRODUT.CODSEC AND pcsecao.codepto = pcdepto.codepto
        LEFT JOIN pccategoria ON pccategoria.CODCATEGORIA = PCPRODUT.CODCATEGORIA and pccategoria.codsec = pcsecao.codsec
        WHERE pcprodut.dtexclusao IS NULL ORDER BY pcprodut.CODPROD
    """,
    'dim_filial.parquet': "SELECT DISTINCT codigo AS cod_filial, razaosocial AS nm_filial FROM pcfilial WHERE codigo <> 99 ORDER BY codigo",
    'dim_fornecedor.parquet': """
        SELECT DISTINCT codfornec AS cod_fornecedor, fornecedor AS nm_fornecedor,
        CASE WHEN classificacao = 'F' THEN 'FARMA' WHEN classificacao = 'H' THEN 'HB' ELSE 'OUTROS' END AS classificacao
        FROM pcfornec WHERE revenda = 'S' ORDER BY codfornec
    """,
    'dim_cliente.parquet': "SELECT DISTINCT codcli AS cod_cliente, cliente nm_cliente, tipofj AS tipo_pj FROM pcclient ORDER BY codcli",
    'dim_vendedor.parquet': "SELECT DISTINCT codusur AS cod_vendedor, nome AS nm_vendedor FROM pcusuari ORDER BY codusur",
    'dim_supervisor.parquet': "SELECT codsupervisor AS cod_supervisor, nome AS nm_supervisor FROM pcsuperv ORDER BY codsupervisor",
    'dim_televendas.parquet': "SELECT DISTINCT p.codemitente cod_televenda, e.nome as nm_televenda FROM pcpedc p INNER JOIN pcempr e on e.matricula = p.codemitente",
    'fato_venda.parquet': """
        SELECT m.codfilial as cod_filial, m.dtmov as data_movimentacao, m.numped as num_pedido, m.codusur as cod_vendedor, p.codemitente as cod_televenda,
        s.codsupervisor as cod_supervisor, m.codcli as cod_cliente, m.codfornec as cod_fornecedor, m.codprod as cod_produto, m.numlote as num_lote, m.datavalidade as data_validade,
        COALESCE(CASE 
            WHEN p.TIPOFV = 'PE' AND p.ORIGEMPED = 'F' THEN 'PEDIDO ELETRÔNICO'
            WHEN p.TIPOFV = 'OL' AND p.ORIGEMPED = 'F' THEN 'OPERADOR LOGÍSTICO'
            WHEN p.ORIGEMPED = 'T' THEN 'TELEMARKETING'
            WHEN p.ORIGEMPED = 'W' THEN 'E-COMMERCE'
            ELSE 'FORÇA DE VENDAS'
        END, 'OUTROS') as origem_pedido,
        SUM(m.qt) as qt_vendida,
        SUM(ROUND((NVL(m.punit, 0) + NVL(m.vloutros, 0) + NVL(m.vlfrete_rateio, 0)) * m.qt, 2)) as valor_bruto,
        SUM(ROUND((NVL(m.punit, 0) - NVL(m.st, 0) - NVL(m.vlipi, 0)) * m.qt, 2)) as valor_liquido
        FROM pcmov m
        LEFT JOIN pcusuari u ON u.codusur = m.codusur
        LEFT JOIN pcsuperv s ON s.codsupervisor = u.codsupervisor
        LEFT JOIN pcpedc p ON p.numped = m.numped
        WHERE m.codoper = 'S' AND m.dtmov >= TO_DATE('01/01/2024', 'DD/MM/YYYY') AND m.dtcancel is null 
        GROUP BY m.codfilial, m.dtmov, m.numped, m.codusur, s.codsupervisor, m.codcli, m.codfornec, m.codprod, m.numlote, m.datavalidade, p.codemitente, p.TIPOFV, p.ORIGEMPED
    """,
    'fato_pedido_venda.parquet': """
        select
            pc.numped as numero_pedido,
            pc.numtransvenda,
            pc.data as data_pedido,
            pc.codcli as cod_cliente,
            pc.codusur as cod_vendedor,
            pc.codemitente as cod_emitente,
            pc.codfilial as cod_filial,
            pi.codprod as cod_produto,
            PC.vltotal as valor_total,
            pc.vlatend as valor_atendido,
            pi.pvenda as vl_venda_unitario,
            pi.qt as quantidade,
            pi.qtfalta as qtd_falta,
            pi.numlote as num_lote,
            COALESCE(
                CASE 
                    WHEN pc.TIPOFV = 'PE' AND pc.ORIGEMPED = 'F' THEN 'PEDIDO ELETRÔNICO'
                    WHEN pc.TIPOFV = 'OL' AND pc.ORIGEMPED = 'F' THEN 'OPERADOR LOGÍSTICO'
                    WHEN pc.ORIGEMPED = 'T' THEN 'TELEMARKETING'
                    WHEN pc.ORIGEMPED = 'W' THEN 'E-COMMERCE'
                    WHEN pc.ORIGEMPED = 'F' THEN 'FORÇA DE VENDAS'
                END, 'OUTROS') as origem_pedido,
            case
                when pc.posicao = 'P' then 'aguardado liberação'
                when pc.posicao = 'B' then 'pedido bloqueado'
                when pc.posicao = 'L' then 'pedido liberado'
                when pc.posicao = 'F' then 'pedido faturado'
                when pc.posicao = 'C' then 'pedido cancelado'
                when pc.posicao = 'R' then 'pedido em separacao'
                when pc.posicao = 'D' then 'pedido duplicado'
                when pc.posicao = 'M' then 'pedido em montagem'
                else 'outros' 
            end as posicao_pedido
        from pcpedc pc
        inner join pcpedi pi on pi.numped = pc.numped
        where pc.data >= to_date('01/12/2025', 'DD/MM/YYYY') and pc.dtcancel is null
        order by posicao_pedido
    """,
    'fato_pretacao_receber.parquet': """
        select
            codcli as cod_cliente, prest as prestacao, duplic as duplicata,
            valor, dtvenc as dt_vencimento,
            vpago as valor_pago, txperm, dtpag as dt_pagamento,
            dtemissao as dt_emissao, codfilial as cod_filial,
            codusur as cod_vendedor, valordesc as vl_desconto,
            vldevol as vl_devolucao, dtdevol as dt_devolucao,
            codsupervisor as cod_supervisor, numtransvenda,
            numped, codemitentepedido as cod_emitente_pedido
        from pcprest
        where dtemissao >= to_date('01/01/2025', 'SS/MM/YYYY') and dtcancel is null and dtpag is null
    """
}

def exportar_para_parquet(query, nome_arquivo, conexao):
    try:
        if os.path.exists(nome_arquivo):
            os.remove(nome_arquivo)
        
        print(f'[...] Extraindo: {nome_arquivo}')
        df = pd.read_sql(query, con=conexao)
        
        # O "pulo do gato": Salvar como parquet com compressão snappy
        df.to_parquet(nome_arquivo, compression='snappy', index=False)
        print(f'[+] Sucesso: {nome_arquivo} ({len(df)} registros)')
        
    except Exception as e:
        print(f'[!] Erro em {nome_arquivo}: {e}')

if __name__ == "__main__":
    with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn) as connection:
        for arquivo, sql in queries.items():
            exportar_para_parquet(sql, arquivo, connection)