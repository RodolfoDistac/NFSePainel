# services/parcelas.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional
from decimal import Decimal, InvalidOperation

# ===================== Datas =====================

def _last_day_of_month(d: date) -> date:
    """Retorna o último dia do mês da data informada."""
    first_next = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first_next - timedelta(days=1)

def calcular_vencimento_padrao(today: Optional[date] = None) -> date:
    """
    Vencimento padrão = último dia do mês ANTERIOR ao mês do sistema.
    Ex.: hoje=2025-10-16 -> padrão = 2025-09-30
    """
    today = today or date.today()
    primeiro_mes_atual = today.replace(day=1)
    ultimo_mes_anterior = primeiro_mes_atual - timedelta(days=1)
    return ultimo_mes_anterior  # já é o último dia do mês anterior

def format_dd_mm_aaaa(d: date) -> str:
    return f"{d.day:02d}-{d.month:02d}-{d.year:04d}"

def parse_dd_mm_aaaa(s: str) -> Optional[date]:
    s = (s or "").strip()
    if len(s) != 10 or s[2] != "-" or s[5] != "-":
        return None
    try:
        dd = int(s[:2]); mm = int(s[3:5]); yy = int(s[6:])
        return date(yy, mm, dd)
    except Exception:
        return None

# ===================== Valores =====================

def _to_decimal_brl(s: str) -> Decimal:
    """
    Converte string BRL para Decimal.
    Aceita "1.234,56" ou "1234,56" ou "1234.56".
    """
    if s is None:
        return Decimal("0")
    t = str(s).strip()
    if not t:
        return Decimal("0")
    # Heurística: se tiver ponto e vírgula, troca ponto de milhar e vírgula decimal
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", ".")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return Decimal("0")

def _fmt_brl(d: Decimal) -> str:
    v = d.quantize(Decimal("0.01"))
    s = f"{v:.2f}"
    inteiro, frac = s.split(".")
    inteiro = int(inteiro)
    inteiro = f"{inteiro:,}".replace(",", ".")
    return f"{inteiro},{frac}"

# ===================== Regras de aplicação =====================

def _is_cancelada(row: Dict[str, str]) -> bool:
    return (row.get("STATUS") or "").strip().lower() == "cancelada"

def ajustar_acumuladores(linhas: List[Dict[str, str]]) -> int:
    """
    Ajusta acumuladores conforme regra:
      - 410 -> 411 (ISS normal)
      - 424 -> 425 (ISS retido)
      - vazio -> deriva de ISS_RET (se != 0,00 => 425, senão 411)
    Não altera linhas canceladas.
    Retorna a quantidade de linhas alteradas.
    """
    changed = 0
    for r in linhas:
        if _is_cancelada(r):
            continue
        acc = (r.get("ACUMULADOR") or "").strip()
        if acc == "410":
            r["ACUMULADOR"] = "411"; changed += 1
        elif acc == "424":
            r["ACUMULADOR"] = "425"; changed += 1
        elif not acc:
            is_ret = (r.get("ISS_RET", "0,00") != "0,00")
            r["ACUMULADOR"] = "425" if is_ret else "411"; changed += 1
    return changed

def aplicar_parcelas_uma(linhas: List[Dict[str, str]], venc_ddmmaa: str) -> int:
    """
    Define 1 parcela para cada linha (não cancelada), com:
      - n = "1"
      - venc = venc_ddmmaa (dd-mm-aaaa)
      - valor = VALOR da nota (string BRL original)
    Retorna quantidade de linhas às quais foram atribuídas parcelas.
    """
    d = parse_dd_mm_aaaa(venc_ddmmaa)
    if not d:
        raise ValueError("Data de vencimento inválida. Use o formato dd-mm-aaaa.")
    applied = 0
    for r in linhas:
        if _is_cancelada(r):
            # Mantém coerência: cancelada continua sem parcelas (ou com valor 0,00 se já existir).
            # Aqui optamos por NÃO definir parcela nova para canceladas.
            continue
        val = (r.get("VALOR") or "0,00")
        r["PARCELAS"] = [{"n": "1", "venc": venc_ddmmaa, "valor": val}]
        applied += 1
    return applied

def aplicar_parcelas_e_acumuladores(linhas: List[Dict[str, str]], venc_ddmmaa: str) -> Tuple[int, int]:
    """
    Conjunto: ajusta acumuladores e aplica 1 parcela.
    Retorna (qtd_acumuladores_ajustados, qtd_parcelas_aplicadas).
    """
    a = ajustar_acumuladores(linhas)
    p = aplicar_parcelas_uma(linhas, venc_ddmmaa)
    return a, p
