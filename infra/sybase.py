# infra/sybase.py
# -*- coding: utf-8 -*-
"""
Conector SQL Anywhere / Sybase para o NFSePainel.

Compatibilidade:
- Mantém os mesmos nomes de variáveis utilizados no app (.env):
  SYBASE_DSN, SYBASE_DRIVER, SYBASE_HOST, SYBASE_PORT, SYBASE_DB,
  SYBASE_UID, SYBASE_PWD.
- A função pública `connect(cfg: Optional[Mapping[str,str]] = None, *, timeout=10, autocommit=False)`
  suporta override por sessão (ex.: vindo do "Login empresa") sem quebrar quem lê de env.

Exemplos de uso:
    from infra.sybase import connect

    # 1) Usando apenas o .env/ambiente
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT TOP 1 1")
        print(cur.fetchone())

    # 2) Override por sessão (login manual)
    cfg = {
        "SYBASE_DSN": "",
        "SYBASE_DRIVER": "SQL Anywhere 17",
        "SYBASE_HOST": "IGOR",
        "SYBASE_PORT": "2638",
        "SYBASE_DB": "Contabil",
        "SYBASE_UID": "ESTACAO08",
        "SYBASE_PWD": "2104",
    }
    with connect(cfg) as con:
        ...

Notas:
- Se `SYBASE_DSN` estiver definido, será priorizado (DSN ODBC do Windows).
- Caso contrário, monta a connection string direta com DRIVER/HOST/PORT/DB/UID/PWD.
- Não persiste logs em arquivo; somente mensagens para o logger padrão do app.
"""

from __future__ import annotations

import logging
import os
from typing import Mapping, Optional

logger = logging.getLogger("nfse.infra.sybase")
if not logger.handlers:
    _h = logging.StreamHandler()
    _fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s sybase %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    _h.setFormatter(_fmt)
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


def _get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _from_cfg_or_env(cfg: Optional[Mapping[str, str]], key: str, default: str = "") -> str:
    if cfg and key in cfg and cfg[key] is not None:
        return str(cfg[key]).strip()
    return _get_env(key, default)


def _build_conn_str(cfg: Optional[Mapping[str, str]]) -> str:
    """
    Monta a connection string a partir de `cfg` (override) ou env.
    - Prioriza DSN; se vazio, usa DRIVER/HOST/PORT/DB/UID/PWD.
    """
    dsn = _from_cfg_or_env(cfg, "SYBASE_DSN")
    uid = _from_cfg_or_env(cfg, "SYBASE_UID")
    pwd = _from_cfg_or_env(cfg, "SYBASE_PWD")

    if dsn:
        parts = [f"DSN={dsn}"]
        if uid:
            parts.append(f"UID={uid}")
        if pwd:
            parts.append(f"PWD={pwd}")
        return ";".join(parts)

    driver = _from_cfg_or_env(cfg, "SYBASE_DRIVER", "SQL Anywhere 17")
    host = _from_cfg_or_env(cfg, "SYBASE_HOST", "IGOR")
    port = _from_cfg_or_env(cfg, "SYBASE_PORT", "2638")
    dbn = _from_cfg_or_env(cfg, "SYBASE_DB", "Contabil")

    # Observações:
    # - Curly braces no Driver (ODBC) -> {SQL Anywhere 17}
    # - CharSet opcional; em muitos ambientes ajuda com acentuação. Se causar erro, remova.
    # - Encrypt opcional desabilitado (ajuste se necessário).
    conn = (
        f"Driver={{{driver}}};"
        f"Host={host}:{port};"
        f"DBN={dbn};"
        f"UID={uid};"
        f"PWD={pwd};"
        # f"CharSet=utf8;"            # descomente se sua base estiver em UTF-8
        # f"Compress=NO;"             # ajuste conforme política
        # f"Encrypt=NO;"              # ajuste se necessário
    )
    return conn


def connect(
    cfg: Optional[Mapping[str, str]] = None,
    *,
    timeout: int = 10,
    autocommit: bool = False,
):
    """
    Abre conexão pyodbc com o SQL Anywhere (Domínio).

    Parâmetros
    ----------
    cfg : Mapping[str, str] | None
        Override das variáveis do .env com as mesmas chaves (SYBASE_*).
    timeout : int
        Timeout da conexão (segundos). Default: 10
    autocommit : bool
        Se True, ativa autocommit. Default: False (recomendado para operações transacionais).

    Retorna
    -------
    pyodbc.Connection (context manager)
    """
    try:
        import pyodbc  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Não foi possível importar 'pyodbc'. Instale os requisitos e o driver ODBC do SQL Anywhere 17.\n"
            "Ex.: pip install pyodbc"
        ) from exc

    conn_str = _build_conn_str(cfg)

    # Esconde senha no preview
    pwd = _from_cfg_or_env(cfg, "SYBASE_PWD")
    preview = conn_str.replace(pwd, "***") if pwd else conn_str
    logger.info({"event": "sybase_connect_attempt", "conn": preview})

    try:
        con = pyodbc.connect(conn_str, autocommit=autocommit, timeout=timeout)
    except Exception as exc:
        # Erros comuns: driver não instalado, DSN inexistente, host/porta inválidos.
        # Propagamos a exceção com uma mensagem amigável.
        raise RuntimeError(f"Falha ao conectar no Domínio (Sybase). Detalhe: {exc}") from exc

    # Smoke simples (não consome transação)
    try:
        cur = con.cursor()
        cur.execute("SELECT TOP 1 1")
        cur.fetchone()
        cur.close()
    except Exception:
        # Não derruba a conexão; apenas loga. (Alguns perfis limitam SELECT TOP)
        logger.warning({"event": "sybase_smoke_warn"})

    logger.info({"event": "sybase_connect_ok"})
    return con


def ping(con) -> bool:
    """
    Verifica se a conexão está funcional (SELECT 1).
    Retorna True/False; não levanta exceção.
    """
    try:
        cur = con.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        return True
    except Exception as exc:
        logger.error({"event": "sybase_ping_error", "err": str(exc)})
        return False
