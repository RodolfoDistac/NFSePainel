# services/empresa_service.py
# -*- coding: utf-8 -*-
"""
Serviço de Empresas para o Login do NFSePainel.

- Fonte primária: Oracle (perfil CAD - CadastroDistac servidor), via infra.oracle.
- Normaliza colunas (id/nome/cnpj) vindas da query ORACLE_CAD_EMPRESAS_QUERY.
- Agora também expõe `cod_dominio` (quando a query trouxer essa coluna),
  mantendo compatibilidade com o fluxo anterior (Java usava COD_DOMINIO).
- Cache leve em memória para evitar consultas repetidas.
- Healthcheck de conectividade Oracle (perfis CAD e BAIXA).
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from infra.oracle import fetch_empresas as _oracle_fetch_empresas
from infra.oracle import healthcheck as _oracle_healthcheck

logger = logging.getLogger("nfse.services.empresa")
if not logger.handlers:
    _h = logging.StreamHandler()
    _fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s empresa %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    _h.setFormatter(_fmt)
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


# -----------------------------------------------------------------------------
# Modelo
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Empresa:
    id: str                 # pode ser o próprio COD_DOMINIO se a query fizer alias
    nome: str
    cnpj: str               # formatado (se 14 dígitos), senão raw
    cod_dominio: Optional[str] = None  # novo campo (compatível com Java)


__all__ = [
    "Empresa",
    "listar_empresas",
    "buscar_empresas",
    "healthcheck_oracle",
    "formatar_cnpj",
]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
_DIGITS = re.compile(r"\D+")


def _only_digits(s: str) -> str:
    return _DIGITS.sub("", s or "")


def formatar_cnpj(v: str) -> str:
    """
    Formata CNPJ (14 dígitos) em 00.000.000/0000-00.
    Se não tiver 14 dígitos, retorna original.
    """
    d = _only_digits(v)
    if len(d) == 14:
        return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"
    return v or ""


def _pick_first(row: Dict[str, Any], keys: Iterable[str], default: str = "") -> str:
    for k in keys:
        if k in row and row[k] is not None:
            return str(row[k]).strip()
    return default


def _row_to_empresa(row: Dict[str, Any]) -> Empresa:
    """
    Mapeia um dict de resultado (colunas minúsculas) para Empresa.
    Aceita variações comuns de nomes de colunas.
    - Para compatibilidade com o Java: tenta pegar COD_DOMINIO.
    """
    # Colunas são baixadas em lower-case em infra.oracle
    id_ = _pick_first(
        row,
        ("id", "empresa_id", "codigo", "cod_empresa", "cod_dominio"),  # se a query fez AS ID, cai aqui
    )
    nome = _pick_first(
        row,
        ("nome", "razao_social", "nome_fantasia", "nomefantasia", "empresa"),
    )
    cnpj_raw = _pick_first(row, ("cnpj", "cpf_cnpj", "doc", "documento"))
    cnpj_fmt = formatar_cnpj(cnpj_raw)

    cod_dom = _pick_first(
        row,
        ("cod_dominio", "codigo_dominio", "coddominio", "cod_dom"),
        default="",
    ) or None

    # Se não houver cod_dominio explícito, mas o 'id' já é o COD_DOMINIO (por alias), reusa
    if cod_dom is None and _only_digits(id_):
        cod_dom = id_

    return Empresa(id=id_ or "", nome=nome or "", cnpj=cnpj_fmt, cod_dominio=cod_dom)


# -----------------------------------------------------------------------------
# Cache simples em memória
# -----------------------------------------------------------------------------
_CACHE: Dict[str, Any] = {"ts": 0.0, "data": []}
_TTL: int = int(os.getenv("EMPRESAS_TTL", "60"))  # segundos


def _cache_get() -> Optional[List[Empresa]]:
    now = time.time()
    data = _CACHE.get("data") or []
    ts = float(_CACHE.get("ts") or 0.0)
    if data and (now - ts) < _TTL:
        return data  # type: ignore[return-value]
    return None


def _cache_set(empresas: List[Empresa]) -> None:
    _CACHE["data"] = empresas
    _CACHE["ts"] = time.time()


# -----------------------------------------------------------------------------
# API pública
# -----------------------------------------------------------------------------
def listar_empresas(force: bool = False, query: Optional[str] = None) -> List[Empresa]:
    """
    Lista empresas via Oracle (perfil CAD), aplicando normalização e cache.
    - Se 'query' for informado, sobrepõe a ORACLE_CAD_EMPRESAS_QUERY do ambiente.
    - Retorna Empresa(id, nome, cnpj, cod_dominio).
    """
    if not force:
        cached = _cache_get()
        if cached is not None:
            return cached

    try:
        rows = _oracle_fetch_empresas("CAD", query=query)
    except Exception as exc:  # pragma: no cover
        logger.error({"event": "empresas_fetch_error", "err": str(exc)})
        return []

    empresas = [_row_to_empresa(r) for r in rows]
    empresas.sort(key=lambda e: (e.nome or "").upper())
    _cache_set(empresas)
    logger.info({"event": "empresas_listadas", "count": len(empresas)})
    return empresas


def buscar_empresas(term: str, base: Optional[List[Empresa]] = None) -> List[Empresa]:
    """
    Busca por termo (case-insensitive) em nome ou CNPJ (sem pontuação).
    """
    term = (term or "").strip()
    if not term:
        return base or listar_empresas()

    base = base or listar_empresas()
    t_digits = _only_digits(term)
    t_upper = term.upper()

    out: List[Empresa] = []
    for e in base:
        if t_digits and _only_digits(e.cnpj).startswith(t_digits):
            out.append(e)
            continue
        if t_upper in (e.nome or "").upper():
            out.append(e)
    return out


def healthcheck_oracle() -> Dict[str, bool]:
    """
    Executa healthcheck nos perfis Oracle relevantes para o app.
    Retorna dict: {"CAD": True/False, "BAIXA": True/False}
    """
    status = {
        "CAD": bool(_oracle_healthcheck("CAD")),
        "BAIXA": bool(_oracle_healthcheck("BAIXA")),
    }
    logger.info({"event": "oracle_healthcheck_all", **status})
    return status
