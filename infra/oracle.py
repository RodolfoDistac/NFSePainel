# infra/oracle.py
# -*- coding: utf-8 -*-
"""
Camada de acesso ao Oracle para o NFSePainel.

- Pools independentes por perfil (ex.: CAD, BAIXA)
- Suporte a modo thin/thick (Instant Client)
- utilitários: healthcheck, run_query, fetch_empresas, buscar_estacao
- Query PADRÃO de empresas (CAD) quando ORACLE_CAD_EMPRESAS_QUERY não está definida:
    SELECT COD_DOMINIO AS ID, NOME, CNPJ
    FROM TABCADASTROEMPRESAS
    WHERE NVL(EMP_ATIVAS, 1) = 1
    ORDER BY NOME

Observações:
- Não carregamos .env aqui (isso é feito pelo app quando necessário).
- Logs: imprimimos no console e também usamos utils.logs.log_emit quando disponível.
"""

from __future__ import annotations

import os
import json
import datetime as _dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional

import oracledb


# --------------------------------------------------------------------------------------
# Logging helper (console + integração com utils.logs, se disponível)
# --------------------------------------------------------------------------------------

def _ts() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def _console_log(level: str, channel: str, payload: Mapping[str, Any]) -> None:
    try:
        print(f"{_ts()} {level.upper()} {channel} {json.dumps(dict(payload), ensure_ascii=False)}")
    except Exception:
        # fallback bem simples
        print(f"{_ts()} {level.upper()} {channel} {payload}")

def _emit(level: str, event: str, **fields: Any) -> None:
    # console
    _console_log(level, "oracle", {"event": event, **fields})
    # integração opcional com utils.logs (sem travar se não existir)
    try:
        from utils.logs import log_emit as _log_emit  # type: ignore
        _log_emit(None, level, event, **fields)
    except Exception:
        pass


# --------------------------------------------------------------------------------------
# Inicialização do modo Oracle (thin/thick)
# --------------------------------------------------------------------------------------

_MODE = os.getenv("ORACLE_MODE", "thin").lower()
if _MODE == "thick":
    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR") or None
    try:
        # Se já estiver em thick, init_oracle_client ignora; se não, muda para thick
        oracledb.init_oracle_client(lib_dir=lib_dir)
        _emit("info", "oracle_init_thick_ok", lib_dir=lib_dir)
    except Exception as e:
        _emit("error", "oracle_init_thick_error", err=str(e))

_emit("info", "oracle_mode", value=("thick" if not oracledb.is_thin_mode() else "thin"))


# --------------------------------------------------------------------------------------
# Config / Pool
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class OracleConfig:
    host: str
    port: int
    service: str
    user: str
    password: str
    min: int = 1
    max: int = 4
    increment: int = 1

    @staticmethod
    def from_env(prefix: str) -> "OracleConfig":
        """
        Lê variáveis de ambiente no formato:
            ORACLE_<PREFIX>_HOST, _PORT, _SERVICE, _USER, _PASSWORD
        """
        def need(name: str) -> str:
            env = f"ORACLE_{prefix}_{name}"
            val = os.getenv(env)
            if val is None or str(val).strip() == "":
                raise RuntimeError(f"Variável de ambiente ausente: {env}")
            return str(val).strip()

        host = need("HOST")
        port = int(need("PORT"))
        service = need("SERVICE")
        user = need("USER")
        password = need("PASSWORD")

        return OracleConfig(
            host=host,
            port=port,
            service=service,
            user=user,
            password=password,
            min=int(os.getenv(f"ORACLE_{prefix}_POOL_MIN", "1")),
            max=int(os.getenv(f"ORACLE_{prefix}_POOL_MAX", "4")),
            increment=int(os.getenv(f"ORACLE_{prefix}_POOL_INC", "1")),
        )

    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service}"


class _PoolRegistry:
    def __init__(self) -> None:
        self._pools: Dict[str, oracledb.ConnectionPool] = {}

    def get(self, profile: str) -> oracledb.ConnectionPool:
        prof = (profile or "").upper()
        if prof in self._pools:
            return self._pools[prof]

        cfg = OracleConfig.from_env(prof)
        pool = oracledb.create_pool(
            user=cfg.user,
            password=cfg.password,
            dsn=cfg.dsn(),
            min=cfg.min,
            max=cfg.max,
            increment=cfg.increment,
        )
        self._pools[prof] = pool
        _emit(
            "info",
            "oracle_pool_created",
            profile=prof,
            host=cfg.host,
            port=cfg.port,
            service=cfg.service,
            min=cfg.min,
            max=cfg.max,
            increment=cfg.increment,
            mode=("thick" if not oracledb.is_thin_mode() else "thin"),
        )
        return pool

    def close_all(self) -> None:
        for pool in self._pools.values():
            try:
                pool.close()
            except Exception:
                pass
        self._pools.clear()


_pools = _PoolRegistry()


def get_pool(profile: str) -> oracledb.ConnectionPool:
    return _pools.get(profile)


# --------------------------------------------------------------------------------------
# Utilitários de consulta
# --------------------------------------------------------------------------------------

def _rows_to_dicts(cur: oracledb.Cursor) -> List[Dict[str, Any]]:
    cols = [d[0].lower() for d in cur.description]  # chaves minúsculas (compat c/ uso atual)
    out: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def healthcheck(profile: str) -> bool:
    """
    Executa SELECT 1 e retorna True/False.
    """
    try:
        pool = get_pool(profile)
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM dual")
            cur.fetchall()
        _emit("info", "oracle_healthcheck", profile=profile, ok=True)
        return True
    except Exception as e:
        _emit("error", "oracle_healthcheck_error", profile=profile, err=str(e))
        return False


def run_query(profile: str, sql: str, params: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Executa uma query arbitrária e retorna lista de dicts (colunas minúsculas).
    """
    pool = get_pool(profile)
    with pool.acquire() as conn:
        cur = conn.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        if cur.description is None:
            return []
        return _rows_to_dicts(cur)


# --------------------------------------------------------------------------------------
# Query padrão de EMPRESAS (CAD)
# --------------------------------------------------------------------------------------

_DEFAULT_EMPRESAS_QUERY = """
SELECT
  COD_DOMINIO AS ID,
  NOME,
  CNPJ
FROM TABCADASTROEMPRESAS
WHERE NVL(EMP_ATIVAS, 1) = 1
ORDER BY NOME
""".strip()


def fetch_empresas(profile: str = "CAD") -> List[Dict[str, str]]:
    """
    Retorna empresas no formato:
      [{"id": <str|int>, "nome": <str>, "cnpj": <str>}...]

    Ordem de resolução da query:
      1) ORACLE_CAD_EMPRESAS_QUERY (se definida)
      2) Query padrão (_DEFAULT_EMPRESAS_QUERY)
    """
    query = (os.getenv("ORACLE_CAD_EMPRESAS_QUERY") or "").strip() or _DEFAULT_EMPRESAS_QUERY
    _emit("info", "oracle_fetch_empresas_start", profile=profile)

    pool = get_pool(profile)
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(query)
        if cur.description is None:
            _emit("warn", "oracle_fetch_empresas_sem_result", profile=profile)
            return []

        # Normaliza para as chaves esperadas (id, nome, cnpj) sem depender do case exato
        cols = [d[0].lower() for d in cur.description]
        rows: List[Dict[str, str]] = []
        for r in cur.fetchall():
            row = {cols[i]: r[i] for i in range(len(cols))}
            # aceita variações de nomes
            rid = row.get("id") or row.get("cod_dominio") or row.get("coddominio") or row.get("codigo") or row.get("codigoempresa")
            rnome = row.get("nome") or row.get("razao") or row.get("razao_social") or row.get("razaosocial")
            rcnpj = row.get("cnpj") or row.get("cpf_cnpj") or row.get("cpfcnpj")
            rows.append({
                "id": str(rid) if rid is not None else "",
                "nome": str(rnome or ""),
                "cnpj": str(rcnpj or ""),
            })

        _emit("info", "oracle_fetch_empresas_ok", qtd=len(rows))
        return rows


# --------------------------------------------------------------------------------------
# Utilitário: buscar estação por usuário (BAIXA)
# --------------------------------------------------------------------------------------

_DEFAULT_ESTACAO_QUERY = """
SELECT CAST(ESTACAO AS VARCHAR2(10)) AS ESTACAO
FROM TAB_USUARIO
WHERE UPPER(USUARIO) = :USUARIO
""".strip()


def buscar_estacao(usuario: str, profile: str = "BAIXA") -> Optional[str]:
    """
    Retorna a estação (string) configurada para o usuário (case-insensitive) no BAIXA.
    A query pode ser sobrescrita por ORACLE_BAIXA_ESTACAO_QUERY.
    """
    if not usuario:
        return None

    sql = (os.getenv("ORACLE_BAIXA_ESTACAO_QUERY") or "").strip() or _DEFAULT_ESTACAO_QUERY
    params = {"USUARIO": str(usuario).upper()}

    try:
        res = run_query(profile, sql, params)
        if not res:
            _emit("warn", "oracle_buscar_estacao_vazio", usuario=usuario)
            return None
        linha = res[0]
        est = linha.get("estacao") or linha.get("ESTACAO") or list(linha.values())[0]
        est_str = "" if est is None else str(est)
        _emit("info", "oracle_buscar_estacao_ok", usuario=usuario, estacao=est_str)
        return est_str
    except Exception as e:
        _emit("error", "oracle_buscar_estacao_error", usuario=usuario, err=str(e))
        return None
# infra/oracle.py
# -*- coding: utf-8 -*-
"""
Camada de acesso ao Oracle para o NFSePainel.

- Pools independentes por perfil (ex.: CAD, BAIXA)
- Suporte a modo thin/thick (Instant Client)
- utilitários: healthcheck, run_query, fetch_empresas, buscar_estacao
- Query PADRÃO de empresas (CAD) quando ORACLE_CAD_EMPRESAS_QUERY não está definida:
    SELECT COD_DOMINIO AS ID, NOME, CNPJ
    FROM TABCADASTROEMPRESAS
    WHERE NVL(EMP_ATIVAS, 1) = 1
    ORDER BY NOME

Observações:
- Não carregamos .env aqui (isso é feito pelo app quando necessário).
- Logs: imprimimos no console e também usamos utils.logs.log_emit quando disponível.
"""

from __future__ import annotations

import os
import json
import datetime as _dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional

import oracledb


# --------------------------------------------------------------------------------------
# Logging helper (console + integração com utils.logs, se disponível)
# --------------------------------------------------------------------------------------

def _ts() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def _console_log(level: str, channel: str, payload: Mapping[str, Any]) -> None:
    try:
        print(f"{_ts()} {level.upper()} {channel} {json.dumps(dict(payload), ensure_ascii=False)}")
    except Exception:
        # fallback bem simples
        print(f"{_ts()} {level.upper()} {channel} {payload}")

def _emit(level: str, event: str, **fields: Any) -> None:
    # console
    _console_log(level, "oracle", {"event": event, **fields})
    # integração opcional com utils.logs (sem travar se não existir)
    try:
        from utils.logs import log_emit as _log_emit  # type: ignore
        _log_emit(None, level, event, **fields)
    except Exception:
        pass


# --------------------------------------------------------------------------------------
# Inicialização do modo Oracle (thin/thick)
# --------------------------------------------------------------------------------------

_MODE = os.getenv("ORACLE_MODE", "thin").lower()
if _MODE == "thick":
    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR") or None
    try:
        # Se já estiver em thick, init_oracle_client ignora; se não, muda para thick
        oracledb.init_oracle_client(lib_dir=lib_dir)
        _emit("info", "oracle_init_thick_ok", lib_dir=lib_dir)
    except Exception as e:
        _emit("error", "oracle_init_thick_error", err=str(e))

_emit("info", "oracle_mode", value=("thick" if not oracledb.is_thin_mode() else "thin"))


# --------------------------------------------------------------------------------------
# Config / Pool
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class OracleConfig:
    host: str
    port: int
    service: str
    user: str
    password: str
    min: int = 1
    max: int = 4
    increment: int = 1

    @staticmethod
    def from_env(prefix: str) -> "OracleConfig":
        """
        Lê variáveis de ambiente no formato:
            ORACLE_<PREFIX>_HOST, _PORT, _SERVICE, _USER, _PASSWORD
        """
        def need(name: str) -> str:
            env = f"ORACLE_{prefix}_{name}"
            val = os.getenv(env)
            if val is None or str(val).strip() == "":
                raise RuntimeError(f"Variável de ambiente ausente: {env}")
            return str(val).strip()

        host = need("HOST")
        port = int(need("PORT"))
        service = need("SERVICE")
        user = need("USER")
        password = need("PASSWORD")

        return OracleConfig(
            host=host,
            port=port,
            service=service,
            user=user,
            password=password,
            min=int(os.getenv(f"ORACLE_{prefix}_POOL_MIN", "1")),
            max=int(os.getenv(f"ORACLE_{prefix}_POOL_MAX", "4")),
            increment=int(os.getenv(f"ORACLE_{prefix}_POOL_INC", "1")),
        )

    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service}"


class _PoolRegistry:
    def __init__(self) -> None:
        self._pools: Dict[str, oracledb.ConnectionPool] = {}

    def get(self, profile: str) -> oracledb.ConnectionPool:
        prof = (profile or "").upper()
        if prof in self._pools:
            return self._pools[prof]

        cfg = OracleConfig.from_env(prof)
        pool = oracledb.create_pool(
            user=cfg.user,
            password=cfg.password,
            dsn=cfg.dsn(),
            min=cfg.min,
            max=cfg.max,
            increment=cfg.increment,
        )
        self._pools[prof] = pool
        _emit(
            "info",
            "oracle_pool_created",
            profile=prof,
            host=cfg.host,
            port=cfg.port,
            service=cfg.service,
            min=cfg.min,
            max=cfg.max,
            increment=cfg.increment,
            mode=("thick" if not oracledb.is_thin_mode() else "thin"),
        )
        return pool

    def close_all(self) -> None:
        for pool in self._pools.values():
            try:
                pool.close()
            except Exception:
                pass
        self._pools.clear()


_pools = _PoolRegistry()


def get_pool(profile: str) -> oracledb.ConnectionPool:
    return _pools.get(profile)


# --------------------------------------------------------------------------------------
# Utilitários de consulta
# --------------------------------------------------------------------------------------

def _rows_to_dicts(cur: oracledb.Cursor) -> List[Dict[str, Any]]:
    cols = [d[0].lower() for d in cur.description]  # chaves minúsculas (compat c/ uso atual)
    out: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def healthcheck(profile: str) -> bool:
    """
    Executa SELECT 1 e retorna True/False.
    """
    try:
        pool = get_pool(profile)
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM dual")
            cur.fetchall()
        _emit("info", "oracle_healthcheck", profile=profile, ok=True)
        return True
    except Exception as e:
        _emit("error", "oracle_healthcheck_error", profile=profile, err=str(e))
        return False


def run_query(profile: str, sql: str, params: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Executa uma query arbitrária e retorna lista de dicts (colunas minúsculas).
    """
    pool = get_pool(profile)
    with pool.acquire() as conn:
        cur = conn.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        if cur.description is None:
            return []
        return _rows_to_dicts(cur)


# --------------------------------------------------------------------------------------
# Query padrão de EMPRESAS (CAD)
# --------------------------------------------------------------------------------------

_DEFAULT_EMPRESAS_QUERY = """
SELECT
  COD_DOMINIO AS ID,
  NOME,
  CNPJ
FROM TABCADASTROEMPRESAS
WHERE NVL(EMP_ATIVAS, 1) = 1
ORDER BY NOME
""".strip()


def fetch_empresas(profile: str = "CAD") -> List[Dict[str, str]]:
    """
    Retorna empresas no formato:
      [{"id": <str|int>, "nome": <str>, "cnpj": <str>}...]

    Ordem de resolução da query:
      1) ORACLE_CAD_EMPRESAS_QUERY (se definida)
      2) Query padrão (_DEFAULT_EMPRESAS_QUERY)
    """
    query = (os.getenv("ORACLE_CAD_EMPRESAS_QUERY") or "").strip() or _DEFAULT_EMPRESAS_QUERY
    _emit("info", "oracle_fetch_empresas_start", profile=profile)

    pool = get_pool(profile)
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(query)
        if cur.description is None:
            _emit("warn", "oracle_fetch_empresas_sem_result", profile=profile)
            return []

        # Normaliza para as chaves esperadas (id, nome, cnpj) sem depender do case exato
        cols = [d[0].lower() for d in cur.description]
        rows: List[Dict[str, str]] = []
        for r in cur.fetchall():
            row = {cols[i]: r[i] for i in range(len(cols))}
            # aceita variações de nomes
            rid = row.get("id") or row.get("cod_dominio") or row.get("coddominio") or row.get("codigo") or row.get("codigoempresa")
            rnome = row.get("nome") or row.get("razao") or row.get("razao_social") or row.get("razaosocial")
            rcnpj = row.get("cnpj") or row.get("cpf_cnpj") or row.get("cpfcnpj")
            rows.append({
                "id": str(rid) if rid is not None else "",
                "nome": str(rnome or ""),
                "cnpj": str(rcnpj or ""),
            })

        _emit("info", "oracle_fetch_empresas_ok", qtd=len(rows))
        return rows


# --------------------------------------------------------------------------------------
# Utilitário: buscar estação por usuário (BAIXA)
# --------------------------------------------------------------------------------------

_DEFAULT_ESTACAO_QUERY = """
SELECT CAST(ESTACAO AS VARCHAR2(10)) AS ESTACAO
FROM TAB_USUARIO
WHERE UPPER(USUARIO) = :USUARIO
""".strip()


def buscar_estacao(usuario: str, profile: str = "BAIXA") -> Optional[str]:
    """
    Retorna a estação (string) configurada para o usuário (case-insensitive) no BAIXA.
    A query pode ser sobrescrita por ORACLE_BAIXA_ESTACAO_QUERY.
    """
    if not usuario:
        return None

    sql = (os.getenv("ORACLE_BAIXA_ESTACAO_QUERY") or "").strip() or _DEFAULT_ESTACAO_QUERY
    params = {"USUARIO": str(usuario).upper()}

    try:
        res = run_query(profile, sql, params)
        if not res:
            _emit("warn", "oracle_buscar_estacao_vazio", usuario=usuario)
            return None
        linha = res[0]
        est = linha.get("estacao") or linha.get("ESTACAO") or list(linha.values())[0]
        est_str = "" if est is None else str(est)
        _emit("info", "oracle_buscar_estacao_ok", usuario=usuario, estacao=est_str)
        return est_str
    except Exception as e:
        _emit("error", "oracle_buscar_estacao_error", usuario=usuario, err=str(e))
        return None
