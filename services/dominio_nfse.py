# services/dominio_nfse.py
from __future__ import annotations

from typing import List, Dict, Tuple, Optional, Mapping, Iterable
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from infra.sybase import connect

"""
Ajuste os nomes de tabela/colunas abaixo conforme seu Domínio (SQL Anywhere / Sybase).
A consulta espera que a tabela de NFS-e contenha, no mínimo, as colunas mapeadas.
Se sua estrutura for diferente, basta atualizar as constantes.
"""

# ======= AJUSTE AQUI =======
TBL_NFSE = "XML_SERVICO"  # ex.: "XML_SERVICO" ou "NFSE" ou "NOTAS_SERVICO"

COL_DOC_TOMADOR   = "CNPJ_TOMADOR"       # documento do tomador (apenas dígitos)
COL_NUM_NFE       = "NUMERO_NFE"         # número da NFSe
COL_DATA_EMISSAO  = "DATA_EMISSAO"       # data de emissão (date/datetime)
COL_VALOR_SERVICO = "VR_BRUTO"           # valor do serviço (sem retenções)
COL_ALIQUOTA      = "ALIQUOTA"           # alíquota (%) ou fração (0.x) — trataremos
COL_VALOR_INSS    = "VR_INSS"
COL_VALOR_IR      = "VR_IR"
COL_VALOR_PIS     = "VR_PIS"
COL_VALOR_COFINS  = "VR_COFINS"
COL_VALOR_CSLL    = "VR_CSLL"
COL_VALOR_ISS     = "VR_ISS"
COL_ISS_RETIDO_FL = "ISS_RETIDO"         # flag retido: 'S'/'N' ou 1/0
COL_DISCRIMINACAO = "DISCRIMINACAO"
# ===========================

def _to_decimal(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        s = str(x).strip()
        # aceita 1.234,56 ou 1234.56
        s = s.replace(".", "").replace(",", ".") if ("," in s and "." in s) else s.replace(",", ".")
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return Decimal("0")

def _fmt_brl(d: Decimal, places: int = 2) -> str:
    q = Decimal("1").scaleb(-places)
    v = d.quantize(q, rounding=ROUND_HALF_UP)
    s = f"{v:.{places}f}"
    intp, frac = s.split(".")
    intp = int(intp)
    intp = f"{intp:,}".replace(",", ".")
    return f"{intp},{frac}"

def _fmt_date_ddmmaa(v) -> str:
    # converte tipos datetime/date/str para dd-mm-aaaa
    if v is None:
        return ""
    try:
        import datetime as dt
        if isinstance(v, (dt.date, dt.datetime)):
            d = v.date() if isinstance(v, dt.datetime) else v
            return f"{d.day:02d}-{d.month:02d}-{d.year:04d}"
    except Exception:
        pass
    s = str(v).strip()
    # tenta ISO: yyyy-mm-dd...
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        y, m, d = s[:10].split("-")
        return f"{d}-{m}-{y}"
    # tenta dd/mm/yyyy
    if len(s) >= 10 and s[2] in "/-" and s[5] in "/-":
        p1, p2, p3 = s[:10].replace("/", "-").split("-")
        if len(p1) == 2 and len(p3) == 4:
            return f"{p1}-{p2}-{p3}"
    return s

def _iss_retido_to_bool(v) -> Optional[bool]:
    if v is None:
        return None
    t = str(v).strip().lower()
    if t in {"s", "sim", "1", "true"}: return True
    if t in {"n", "nao", "não", "0", "false"}: return False
    return None

def _chunked(it: Iterable[str], size: int) -> Iterable[List[str]]:
    buf: List[str] = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def _build_select(in_count: int) -> str:
    ph = ",".join("?" for _ in range(in_count))
    # calculamos ISS_RET/ISS_NORMAL a partir do flag de retenção
    return f"""
        SELECT
            CAST({COL_DOC_TOMADOR} AS VARCHAR(20)) AS DOC,
            CAST({COL_NUM_NFE} AS VARCHAR(30))     AS NFE,
            {COL_DATA_EMISSAO}                      AS EMISSAO,
            {COL_VALOR_SERVICO}                     AS VR_SERV,
            {COL_ALIQUOTA}                          AS ALIQUOTA,
            {COL_VALOR_INSS}                        AS VR_INSS,
            {COL_VALOR_IR}                          AS VR_IR,
            {COL_VALOR_PIS}                         AS VR_PIS,
            {COL_VALOR_COFINS}                      AS VR_COFINS,
            {COL_VALOR_CSLL}                        AS VR_CSLL,
            {COL_VALOR_ISS}                         AS VR_ISS,
            {COL_ISS_RETIDO_FL}                     AS ISS_RETIDO,
            {COL_DISCRIMINACAO}                     AS DISCRIMINACAO
        FROM {TBL_NFSE}
        WHERE {COL_NUM_NFE} IN ({ph})
    """

def buscar_nfse_por_numeros(
    numeros_nfe: List[str],
    sybase_cfg: Optional[Mapping[str, str]] = None,
    batch_size: int = 300
) -> List[Dict[str, str]]:
    """
    Busca NFS-e no Domínio pelos números informados e devolve no mesmo formato de colunas do painel:
    TOMADOR, NFE, EMISSAO, VALOR, ALIQ, INSS, IR, PIS, COFINS, CSLL, ISS_RET, ISS_NORMAL, DISCRIMINACAO
    """
    nums = [n for n in dict.fromkeys((numeros_nfe or [])) if str(n).strip() != ""]
    if not nums:
        return []

    out: List[Dict[str, str]] = []

    with connect(sybase_cfg) as con:
        cur = con.cursor()
        for group in _chunked(nums, batch_size):
            sql = _build_select(len(group))
            cur.execute(sql, group)
            for row in cur.fetchall():
                # row conforme SELECT acima
                # Tipagem flexível (pyodbc retorna tuple)
                DOC, NFE, EMISSAO, VR_SERV, ALIQUOTA, VR_INSS, VR_IR, VR_PIS, VR_COFINS, VR_CSLL, VR_ISS, ISS_RETIDO, DISCR = row

                vserv = _to_decimal(VR_SERV)
                aliq  = _to_decimal(ALIQUOTA)
                # normaliza aliquota: se veio fração <=1, vira %; se já %, mantém
                if aliq <= Decimal("1") and aliq != Decimal("0"):
                    aliq = aliq * Decimal("100")

                vinss = _to_decimal(VR_INSS)
                vir   = _to_decimal(VR_IR)
                vpis  = _to_decimal(VR_PIS)
                vcof  = _to_decimal(VR_COFINS)
                vcsll = _to_decimal(VR_CSLL)
                viss  = _to_decimal(VR_ISS)

                retido = _iss_retido_to_bool(ISS_RETIDO)
                if retido is True:
                    iss_ret = viss
                    iss_nor = Decimal("0")
                elif retido is False:
                    iss_ret = Decimal("0")
                    iss_nor = viss
                else:
                    # se não der pra saber, assume normal
                    iss_ret = Decimal("0")
                    iss_nor = viss

                out.append({
                    "TOMADOR": str(DOC or "").strip(),
                    "NFE": str(NFE or "").strip(),
                    "EMISSAO": _fmt_date_ddmmaa(EMISSAO),
                    "VALOR": _fmt_brl(vserv),
                    "ALIQ": _fmt_brl(aliq),
                    "INSS": _fmt_brl(vinss),
                    "IR": _fmt_brl(vir),
                    "PIS": _fmt_brl(vpis),
                    "COFINS": _fmt_brl(vcof),
                    "CSLL": _fmt_brl(vcsll),
                    "ISS_RET": _fmt_brl(iss_ret),
                    "ISS_NORMAL": _fmt_brl(iss_nor),
                    "DISCRIMINACAO": str(DISCR or "").strip(),
                })
    return out
