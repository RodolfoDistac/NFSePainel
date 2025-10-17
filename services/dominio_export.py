# services/dominio_export.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Mapping, Optional, Iterable
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# ========== AJUSTES GERAIS DO LAYOUT ==========
SEP = "|"          # separador de campos
END = ""           # sufixo (fica vazio; cada write já inclui \n)
DEFAULT_DIR = Path("C:/A")  # diretório padrão para export em arquivo

# Campos usados do dataset do painel:
COLS = [
    "TOMADOR","NFE","EMISSAO","VALOR","ALIQ","INSS","IR","PIS","COFINS",
    "CSLL","ISS_RET","ISS_NORMAL","DISCRIMINACAO","STATUS","ACUMULADOR"
]

# ========== HELPERS ==========
def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def _to_decimal(s: str | None) -> Decimal:
    if not s:
        return Decimal("0")
    x = s.strip()
    # aceita 1.234,56 ou 1234.56
    x = x.replace(".", "").replace(",", ".") if ("," in x and "." in x) else x.replace(",", ".")
    try:
        return Decimal(x)
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

def _norm_str(v: object) -> str:
    return ("" if v is None else str(v)).strip()

def _ddmmaaaa(s: str) -> str:
    """Normaliza data exibida pelo painel (já deve vir dd-mm-aaaa; mantém fallback)."""
    t = (s or "").strip()
    if len(t) == 10 and t[2] == "-" and t[5] == "-":
        return t
    # tenta ISO yyyy-mm-dd
    if len(t) >= 10 and t[4] == "-" and t[7] == "-":
        y, m, d = t[:10].split("-")
        return f"{d}-{m}-{y}"
    return t

def _write_line(f, *parts: str) -> None:
    f.write(SEP + (SEP.join(parts)) + SEP + "\n")

def _iter_rows(rows: List[Dict[str, str]]) -> Iterable[Dict[str, str]]:
    for r in rows:
        yield r

# ========== ENVIO DIRETO AO DOMÍNIO (Cabeçalho + Tomador) ==========
# Mantido do passo anterior. Permite envio direto (BD) com staging.
from infra.sybase import connect

def _tomadores_unicos(rows: List[Dict[str, str]]) -> List[str]:
    return sorted({(r.get("TOMADOR") or "").strip() for r in rows if (r.get("TOMADOR") or "").strip()})

def enviar_cabecalho_tomador_dominio(
    rows: List[Dict[str, str]],
    sybase_cfg: Optional[Mapping[str, str]] = None
) -> Tuple[int, int]:
    """
    Envia TOMADORES únicos para staging no Domínio.
    Tabela esperada: TMP_IMPORT_CAB_TOMADOR(CNPJ_TOMADOR PK, DT_CRIACAO TIMESTAMP)
    -> Ajuste conforme a estrutura do seu Domínio.
    """
    tomadores = [t for t in _tomadores_unicos(rows) if t]
    if not tomadores:
        return (0, 0)

    inserted = 0
    with connect(sybase_cfg) as con:
        cur = con.cursor()
        # Cria a tabela se possível
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS TMP_IMPORT_CAB_TOMADOR (
                    CNPJ_TOMADOR VARCHAR(20) NOT NULL PRIMARY KEY,
                    DT_CRIACAO   TIMESTAMP   NOT NULL DEFAULT CURRENT TIMESTAMP
                )
            """)
        except Exception:
            try:
                cur.execute("SELECT COUNT(*) FROM TMP_IMPORT_CAB_TOMADOR")
                cur.fetchall()
            except Exception:
                raise RuntimeError(
                    "A tabela 'TMP_IMPORT_CAB_TOMADOR' não existe e não pôde ser criada.\n"
                    "Ajuste para a estrutura oficial do Domínio e tente novamente."
                )

        for cnpj in tomadores:
            try:
                cur.execute("INSERT INTO TMP_IMPORT_CAB_TOMADOR (CNPJ_TOMADOR) VALUES (?)", (cnpj,))
                inserted += 1
            except Exception:
                # duplicado → ignora
                continue

    return (len(tomadores), inserted)

# ========== EXPORTAÇÃO FINAL EM ARQUIVO (layout 0000/3000/3020/3300/3500/9999) ==========

def _build_0000(rows_count: int) -> List[str]:
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return ["0000", "GERADO_PY", ts, f"QTD={rows_count}"]

def _build_3000(r: Dict[str, str]) -> List[str]:
    return [
        "3000",
        _norm_str(r.get("NFE")),
        _norm_str(r.get("TOMADOR")),
        _ddmmaaaa(_norm_str(r.get("EMISSAO"))),
        _norm_str(r.get("VALOR")),
        _norm_str(r.get("ALIQ")),
        _norm_str(r.get("DISCRIMINACAO")),
        f"STATUS={_norm_str(r.get('STATUS') or 'Normal')}",
    ]

def _build_3020(r: Dict[str, str]) -> List[str]:
    return [
        "3020",
        _norm_str(r.get("NFE")),
        _norm_str(r.get("INSS")),
        _norm_str(r.get("IR")),
        _norm_str(r.get("PIS")),
        _norm_str(r.get("COFINS")),
        _norm_str(r.get("CSLL")),
        _norm_str(r.get("ISS_RET")),
        _norm_str(r.get("ISS_NORMAL")),
    ]

def _build_3300(r: Dict[str, str]) -> List[str]:
    acc = _norm_str(r.get("ACUMULADOR"))
    return ["3300", _norm_str(r.get("NFE")), f"ACC={acc}"]

def _extract_parcelas(r: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Lê r.get('PARCELAS') se existir (lista de dicts {n, venc (dd-mm-aaaa), valor (string BRL)})
    Caso não exista, gera 1 parcela à vista (venc=EMISSAO, valor=VALOR).
    """
    px = r.get("PARCELAS")
    if isinstance(px, list) and px:
        out: List[Dict[str, str]] = []
        for p in px:
            try:
                n = str(p.get("n", "")).strip()
                venc = _ddmmaaaa(_norm_str(p.get("venc")))
                val = _norm_str(p.get("valor"))
                if not n:  # fallback de numeração
                    n = str(len(out) + 1)
                if not venc:
                    venc = _ddmmaaaa(_norm_str(r.get("EMISSAO")))
                if not val:
                    val = _norm_str(r.get("VALOR"))
                out.append({"n": n, "venc": venc, "valor": val})
            except Exception:
                continue
        if out:
            return out

    # default: 1 parcela à vista
    return [{"n": "1", "venc": _ddmmaaaa(_norm_str(r.get("EMISSAO"))), "valor": _norm_str(r.get("VALOR"))}]

def _build_3500(r: Dict[str, str]) -> List[List[str]]:
    nfe = _norm_str(r.get("NFE"))
    parcelas = _extract_parcelas(r)
    rows: List[List[str]] = []
    for p in parcelas:
        rows.append(["3500", nfe, str(p["n"]), _norm_str(p["venc"]), _norm_str(p["valor"])])
    return rows

def _build_9999(total_linhas: int) -> List[str]:
    return ["9999", f"LINHAS={total_linhas}"]

def export_final(rows: List[Dict[str, str]], out_dir: Path | str = DEFAULT_DIR) -> Path:
    """
    Gera arquivo TXT com layout:
      0000 (header)
      3000 (nota)
      3020 (impostos)
      3300 (acumulador)
      3500 (parcelas)
      9999 (trailer)
    Regras:
      - Canceladas: valores zerados e STATUS=Cancelada.
      - Parcelas: usa r['PARCELAS'] se houver; senão 1 parcela à vista (EMISSAO).
    """
    out_dir = _ensure_dir(Path(out_dir))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = out_dir / f"dominio_export_final_{ts}.txt"

    # Pré-processa: garante colunas e zera valores se cancelada
    norm_rows: List[Dict[str, str]] = []
    for r in _iter_rows(rows):
        rr = {k: r.get(k) for k in (set(COLS) | {"PARCELAS"})}
        status = _norm_str(rr.get("STATUS") or "Normal")
        if status.lower() == "cancelada":
            # zera todos os numéricos
            for k in ["VALOR","ALIQ","INSS","IR","PIS","COFINS","CSLL","ISS_RET","ISS_NORMAL"]:
                rr[k] = "0,00"
        # acum default se vazio (deriva de ISS)
        acc = _norm_str(rr.get("ACUMULADOR"))
        if not acc:
            is_ret = (_norm_str(rr.get("ISS_RET")) != "0,00")
            rr["ACUMULADOR"] = "425" if is_ret else "411"
        norm_rows.append(rr)

    with out.open("w", encoding="utf-8") as f:
        # 0000
        head = _build_0000(len(norm_rows))
        _write_line(f, *head)

        total_linhas = 1  # conta 0000
        # linhas por NF
        for rr in norm_rows:
            l3000 = _build_3000(rr); _write_line(f, *l3000); total_linhas += 1
            l3020 = _build_3020(rr); _write_line(f, *l3020); total_linhas += 1
            l3300 = _build_3300(rr); _write_line(f, *l3300); total_linhas += 1
            for l3500 in _build_3500(rr):
                _write_line(f, *l3500); total_linhas += 1

        # 9999
        trail = _build_9999(total_linhas + 1)  # +1 (esta linha)
        _write_line(f, *trail)

    return out
