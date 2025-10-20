# infra/oracle.py
# -*- coding: utf-8 -*-
"""
Conectividade Oracle para o NFSePainel.

Perfis suportados:
- CAD   (CadastroDistac - servidor)   -> usa prefixo de env: ORACLE_CAD_*
- BAIXA (Importação Baixa - servidor) -> usa prefixo de env: ORACLE_BAIXA_*

Variáveis de ambiente esperadas por perfil (prefixadas):
  {PREFIX}_HOST        (ex.: 192.168.236.44)
  {PREFIX}_PORT        (ex.: 1521)
  {PREFIX}_SERVICE     (ex.: XE ou UDISTAC)
  {PREFIX}_USER
  {PREFIX}_PASSWORD

Query opcional para listar empresas (login):
  ORACLE_CAD_EMPRESAS_QUERY
    - Default: "SELECT ID, NOME, CNPJ FROM EMPRESAS WHERE ATIVA = 1 ORDER BY NOME"

Seleção do modo de driver:
  ORACLE_MODE=thin|thick   (default: thin)
  ORACLE_CLIENT_LIB_DIR=C:/oracle/instantclient_19_24  (obrigatório se ORACLE_MODE=thick)

Dependência: oracledb
  pip install oracledb
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

try:
    import oracledb  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Biblioteca 'oracledb' não encontrada. "
        "Instale com 'pip install oracledb' antes de usar infra.oracle."
    ) from exc


# -----------------------------------------------------------------------------
# Logging básico (estruturado) - sem vazar segredos
# -----------------------------------------------------------------------------
logger = logging.getLogger("nfse.infra.oracle")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s oracle %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
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
    timeout: int = 60
    wait_timeout: int = 120
    max_lifetime: int = 3600

    @classmethod
    def from_env(cls, prefix: str) -> "OracleConfig":
        """Carrega configuração do perfil a partir do prefixo de env (ex.: ORACLE_CAD)."""

        def need(name: str) -> str:
            val = os.getenv(f"{prefix}_{name}")
            if not val:
                raise RuntimeError(f"Variável de ambiente ausente: {prefix}_{name}")
            return val

        host = need("HOST")
        port = int(os.getenv(f"{prefix}_PORT", "1521"))
        service = need("SERVICE")
        user = need("USER")
        # Não logamos password por segurança
        password = need("PASSWORD")

        # Tuning opcional do pool por env
        min_ = int(os.getenv(f"{prefix}_POOL_MIN", "1"))
        max_ = int(os.getenv(f"{prefix}_POOL_MAX", "4"))
        inc_ = int(os.getenv(f"{prefix}_POOL_INC", "1"))
        timeout_ = int(os.getenv(f"{prefix}_POOL_TIMEOUT", "60"))
        wait_timeout_ = int(os.getenv(f"{prefix}_POOL_WAIT_TIMEOUT", "120"))
        max_life_ = int(os.getenv(f"{prefix}_POOL_MAX_LIFETIME", "3600"))

        return cls(
            host=host,
            port=port,
            service=service,
            user=user,
            password=password,
            min=min_,
            max=max_,
            increment=inc_,
            timeout=timeout_,
            wait_timeout=wait_timeout_,
            max_lifetime=max_life_,
        )


# -----------------------------------------------------------------------------
# Inicialização do modo do driver (thin/thick)
# -----------------------------------------------------------------------------
_client_init_lock = threading.Lock()
_client_initialized = False


def _ensure_driver_mode() -> None:
    """
    Garante o modo correto do driver Oracle antes de criar pools.

    - Se ORACLE_MODE=thick, chama oracledb.init_oracle_client(lib_dir=...).
    - Se ORACLE_MODE=thin (default), não faz nada.
    - É seguro chamar múltiplas vezes; só inicializa uma vez.
    """
    global _client_initialized

    if _client_initialized:
        return

    with _client_init_lock:
        if _client_initialized:
            return

        mode = (os.getenv("ORACLE_MODE", "thin") or "thin").strip().lower()
        if mode not in ("thin", "thick"):
            logger.warning({"event": "oracle_mode_invalid", "value": mode})
            mode = "thin"

        if mode == "thick":
            lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR", "").strip()
            if not lib_dir:
                raise RuntimeError(
                    "ORACLE_MODE=thick requer ORACLE_CLIENT_LIB_DIR apontando para a pasta do Oracle Instant Client.\n"
                    "Exemplo: C:/oracle/instantclient_19_24"
                )

            try:
                oracledb.init_oracle_client(lib_dir=lib_dir)
                logger.info({"event": "oracle_init_thick_ok", "lib_dir": lib_dir})
            except Exception as exc:  # pragma: no cover
                logger.error(
                    {"event": "oracle_init_thick_error", "lib_dir": lib_dir, "err": str(exc)}
                )
                raise

        else:
            logger.info({"event": "oracle_mode", "value": "thin"})

        _client_initialized = True


# -----------------------------------------------------------------------------
# Gerenciador de Pools (lazy, thread-safe)
# -----------------------------------------------------------------------------
class _PoolHolder:
    """Mantém pools por perfil. Usa criação sob demanda (lazy)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pools: Dict[str, oracledb.ConnectionPool] = {}

    def get(self, profile: str) -> oracledb.ConnectionPool:
        key = profile.upper()
        if key not in ("CAD", "BAIXA"):
            raise ValueError("Perfil Oracle inválido. Use 'CAD' ou 'BAIXA'.")

        with self._lock:
            pool = self._pools.get(key)
            if pool:
                return pool

            # Garante modo thin/thick antes de abrir o pool
            _ensure_driver_mode()

            prefix = "ORACLE_CAD" if key == "CAD" else "ORACLE_BAIXA"
            cfg = OracleConfig.from_env(prefix)

            dsn = oracledb.makedsn(cfg.host, cfg.port, service_name=cfg.service)
            pool = oracledb.create_pool(
                user=cfg.user,
                password=cfg.password,
                dsn=dsn,
                min=cfg.min,
                max=cfg.max,
                increment=cfg.increment,
                homogeneous=True,
                timeout=cfg.timeout,
                wait_timeout=cfg.wait_timeout,
                max_lifetime_session=cfg.max_lifetime,
            )

            self._pools[key] = pool
            logger.info(
                {
                    "event": "oracle_pool_created",
                    "profile": key,
                    "host": cfg.host,
                    "port": cfg.port,
                    "service": cfg.service,
                    "min": cfg.min,
                    "max": cfg.max,
                    "increment": cfg.increment,
                    "mode": "thick" if not getattr(oracledb, "is_thin_mode", lambda: True)() else "thin",
                }
            )
            return pool

    def close_all(self) -> None:
        with self._lock:
            for key, pool in list(self._pools.items()):
                try:
                    pool.close()
                    logger.info({"event": "oracle_pool_closed", "profile": key})
                except Exception as exc:  # pragma: no cover
                    logger.warning(
                        {"event": "oracle_pool_close_error", "profile": key, "err": str(exc)}
                    )
            self._pools.clear()


_pools = _PoolHolder()


def get_pool(profile: str) -> oracledb.ConnectionPool:
    """Obtém (ou cria) um pool para o perfil informado ('CAD' ou 'BAIXA')."""
    return _pools.get(profile)


def close_pools() -> None:
    """Fecha todos os pools (para encerrar o app com limpeza)."""
    _pools.close_all()


# -----------------------------------------------------------------------------
# Healthcheck
# -----------------------------------------------------------------------------
def healthcheck(profile: str = "CAD") -> bool:
    """Executa um SELECT 1 para confirmar conectividade."""
    pool = get_pool(profile)
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM DUAL")
                row = cur.fetchone()
                ok = bool(row and row[0] == 1)
                logger.info({"event": "oracle_healthcheck", "profile": profile, "ok": ok})
                return ok
    except Exception as exc:
        logger.error({"event": "oracle_healthcheck_error", "profile": profile, "err": str(exc)})
        return False


# -----------------------------------------------------------------------------
# Consultas utilitárias
# -----------------------------------------------------------------------------
def fetch_empresas(profile: str = "CAD", query: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Busca as empresas para popular o 'Login Empresa'.

    - Por padrão, usa ORACLE_CAD_EMPRESAS_QUERY quando profile='CAD'.
    - Se não houver env, cai no default:
      SELECT ID, NOME, CNPJ FROM EMPRESAS WHERE ATIVA = 1 ORDER BY NOME

    Retorna: lista de dicts com as colunas retornadas (id, nome, cnpj, ...).
    """
    pool = get_pool(profile)

    if query is None and profile.upper() == "CAD":
        query = os.getenv(
            "ORACLE_CAD_EMPRESAS_QUERY",
            "SELECT ID, NOME, CNPJ FROM EMPRESAS WHERE ATIVA = 1 ORDER BY NOME",
        )

    if not query:
        raise ValueError("Query de empresas não informada e sem default disponível.")

    logger.info({"event": "oracle_fetch_empresas_start", "profile": profile})

    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0].lower() for d in cur.description]  # nomes de coluna
            rows = cur.fetchall() or []
            result = [dict(zip(cols, r)) for r in rows]

    logger.info(
        {"event": "oracle_fetch_empresas_done", "profile": profile, "count": len(result)}
    )
    return result


def run_query(profile: str, sql: str, params: Optional[Iterable[Any]] = None) -> List[Dict[str, Any]]:
    """
    Executa uma consulta ad-hoc (somente SELECT).
    Uso interno para diagnósticos/ajustes durante a evolução do projeto.
    """
    if not sql.strip().lower().startswith("select"):
        raise ValueError("run_query aceita apenas SELECT.")

    pool = get_pool(profile)
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            cols = [d[0].lower() for d in cur.description]
            rows = cur.fetchall() or []
            return [dict(zip(cols, r)) for r in rows]


def buscar_estacao(usuario: str, *, profile: str = "BAIXA") -> Optional[str]:
    """
    Retorna a 'Estação' vinculada a um USUÁRIO (tabela USUARIO_ESTACAO no perfil BAIXA).
    - Faz comparação case-insensitive (UPPER).
    - Ex.: usuario='RODOLFO' -> 'Estação 19'
    """
    usuario = (usuario or "").strip()
    if not usuario:
        return None

    pool = get_pool(profile)
    sql = "SELECT ESTACAO FROM USUARIO_ESTACAO WHERE UPPER(USUARIO) = :usuario"

    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"usuario": usuario.upper()})
                row = cur.fetchone()
                if not row:
                    logger.info({"event": "oracle_buscar_estacao_none", "usuario": usuario})
                    return None
                est = str(row[0]).strip() if row[0] is not None else None
                logger.info({"event": "oracle_buscar_estacao_ok", "usuario": usuario, "estacao": est})
                return est
    except Exception as exc:
        logger.error({"event": "oracle_buscar_estacao_error", "usuario": usuario, "err": str(exc)})
        return None
