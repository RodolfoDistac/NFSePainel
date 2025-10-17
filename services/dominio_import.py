# services/dominio_import.py
from __future__ import annotations

from typing import List, Dict, Tuple, Optional, Mapping
from infra.sybase import connect

# ============================
# AJUSTE AQUI conforme o Domínio
# ============================
# Nomes de TABELAS e COLUNAS esperadas no seu Banco Domínio (SQL Anywhere).
# Troque conforme a sua base. Mantive nomes comuns para ponto de partida.
TBL_CLIENTES = "CLIENTES"
TBL_FORNECEDORES = "FORNECEDORES"
COL_DOC = "CNPJ_CPF"         # coluna do documento (CNPJ/CPF) sem máscara, apenas dígitos
COL_RAZAO = "RAZAO_SOCIAL"   # razão social/descrição
COL_FANTASIA = "NOME_FANTASIA"  # opcional; se não existir, retornaremos vazio
COL_IE = "INSCRICAO_ESTADUAL"   # opcional
COL_MUNICIPIO = "MUNICIPIO"     # opcional
COL_UF = "UF"                   # opcional

# Campos que tentaremos selecionar. Se não existirem, a query cai para uma seleção mínima.
BASE_COLS = [COL_DOC, COL_RAZAO, COL_FANTASIA, COL_IE, COL_MUNICIPIO, COL_UF]

def _build_select_sql(table: str, cols: List[str], in_count: int) -> str:
    """
    Monta o SELECT com IN (?, ?, ...).
    Se alguma coluna não existir, a execução vai falhar -> tratamos e caímos para o mínimo.
    """
    placeholders = ",".join("?" for _ in range(in_count))
    selected = ", ".join(cols)
    return f"SELECT '{table}' AS TIPO, {selected} FROM {table} WHERE {COL_DOC} IN ({placeholders})"

def _fallback_select_sql(table: str, in_count: int) -> str:
    placeholders = ",".join("?" for _ in range(in_count))
    # mínimo viável: DOC e RAZAO; se RAZAO também não existir, vira apenas DOC
    return f"SELECT '{table}' AS TIPO, {COL_DOC}, {COL_RAZAO} FROM {table} WHERE {COL_DOC} IN ({placeholders})"

def buscar_clientes_fornecedores(
    cnpjs: List[str],
    sybase_cfg: Optional[Mapping[str, str]] = None
) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Consulta CLIENTES e FORNECEDORES no Domínio pelos CNPJs/CPFs informados.
    Retorna: (encontrados, nao_encontrados)
        - encontrados: lista de dicts, com chaves padronizadas:
            ["TIPO","DOC","RAZAO","FANTASIA","IE","MUNICIPIO","UF"]
        - nao_encontrados: lista de documentos não localizados em nenhuma tabela
    """
    docs = sorted({d for d in (cnpjs or []) if d})
    if not docs:
        return [], []

    found: Dict[str, Dict[str, str]] = {}

    with connect(sybase_cfg) as con:
        cur = con.cursor()

        # Tenta CLIENTES com cols base; se falhar, usa fallback
        sql_cli = _build_select_sql(TBL_CLIENTES, BASE_COLS, len(docs))
        try:
            cur.execute(sql_cli, docs)
        except Exception:
            sql_cli = _fallback_select_sql(TBL_CLIENTES, len(docs))
            cur.execute(sql_cli, docs)

        for row in cur.fetchall():
            # row: TIPO, [COL_DOC, COL_RAZAO, ...]
            # Convertemos para o dicionário padronizado
            values = [str(x) if x is not None else "" for x in row]
            tipo = "CLIENTE"
            # Determina mapa de acordo com query usada
            if len(values) >= 8:  # TIPO + 6 cols
                _, doc, razao, fantasia, ie, municipio, uf = values[:7]
            elif len(values) >= 4:  # fallback: TIPO + DOC + RAZAO
                _, doc, razao = values[:3]
                fantasia = ie = municipio = uf = ""
            else:
                continue
            found[doc] = {
                "TIPO": tipo, "DOC": doc, "RAZAO": razao, "FANTASIA": fantasia,
                "IE": ie, "MUNICIPIO": municipio, "UF": uf
            }

        # Tenta FORNECEDORES
        sql_forn = _build_select_sql(TBL_FORNECEDORES, BASE_COLS, len(docs))
        try:
            cur.execute(sql_forn, docs)
        except Exception:
            sql_forn = _fallback_select_sql(TBL_FORNECEDORES, len(docs))
            cur.execute(sql_forn, docs)

        for row in cur.fetchall():
            values = [str(x) if x is not None else "" for x in row]
            tipo = "FORNECEDOR"
            if len(values) >= 8:
                _, doc, razao, fantasia, ie, municipio, uf = values[:7]
            elif len(values) >= 4:
                _, doc, razao = values[:3]
                fantasia = ie = municipio = uf = ""
            else:
                continue
            # Se já tinha em CLIENTES, mantém o primeiro e marca TIPO se for mais relevante pra você
            if doc not in found:
                found[doc] = {
                    "TIPO": tipo, "DOC": doc, "RAZAO": razao, "FANTASIA": fantasia,
                    "IE": ie, "MUNICIPIO": municipio, "UF": uf
                }

    encontrados = list(found.values())
    nao_encontrados = [d for d in docs if d not in found]
    return encontrados, nao_encontrados
