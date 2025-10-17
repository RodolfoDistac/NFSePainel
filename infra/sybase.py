from __future__ import annotations

import os
import pyodbc
from contextlib import contextmanager
from typing import Iterator, Optional, Mapping

def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(name)
    return val if (val is not None and str(val).strip() != "") else default

def build_conn_str_from(cfg: Optional[Mapping[str, str]] = None) -> str:
    """
    Monta a connection string ODBC para SQL Anywhere / Sybase.
    Aceita overrides via `cfg` (dict) ou lê de variáveis de ambiente.
    Chaves válidas em cfg/env:
      SYBASE_DSN, SYBASE_DRIVER, SYBASE_HOST, SYBASE_PORT, SYBASE_DB, SYBASE_UID, SYBASE_PWD
    """
    def pick(key: str, default: Optional[str] = None) -> Optional[str]:
        if cfg and key in cfg and str(cfg[key]).strip() != "":
            return str(cfg[key]).strip()
        return _get_env(key, default)

    dsn = pick("SYBASE_DSN")
    if dsn:
        return f"DSN={dsn};"

    driver = pick("SYBASE_DRIVER", "SQL Anywhere 17")
    host = pick("SYBASE_HOST", "IGOR")
    port = pick("SYBASE_PORT", "2638")
    db   = pick("SYBASE_DB", "Contabil")
    uid  = pick("SYBASE_UID", "ESTACAO08")
    pwd  = pick("SYBASE_PWD", "")

    return f"DRIVER={{{driver}}};HOST={host}:{port};DBN={db};UID={uid};PWD={pwd};".replace("{driver}", driver)

@contextmanager
def connect(cfg: Optional[Mapping[str, str]] = None) -> Iterator[pyodbc.Connection]:
    """
    Abre conexão com o Domínio (SQL Anywhere / Sybase) usando pyodbc.
    Se `cfg` for informado, usa overrides; caso contrário lê do ambiente.
    """
    conn_str = build_conn_str_from(cfg)
    conn = pyodbc.connect(conn_str, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
