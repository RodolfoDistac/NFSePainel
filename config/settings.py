from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

@dataclass
class SybaseSettings:
    dsn: Optional[str] = None
    driver: str = "SQL Anywhere 17"
    host: str = "IGOR"
    port: str = "2638"
    db: str = "Contabil"
    uid: str = "ESTACAO08"
    pwd: str = ""

@dataclass
class OracleSettings:
    host: str = "localhost"
    port: str = "1521"
    service: str = "XE"
    user: str = ""
    password: str = ""

@dataclass
class Settings:
    sybase: SybaseSettings
    oracle: OracleSettings
    export_dir: Path = Path("C:/A")
    theme: str = "SystemDefault"

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    return v

def load_settings(dotenv_path: Optional[str] = None) -> Settings:
    # Carrega .env (se existir)
    load_dotenv(dotenv_path, override=False)

    syb = SybaseSettings(
        dsn=_env("SYBASE_DSN"),
        driver=_env("SYBASE_DRIVER", "SQL Anywhere 17"),
        host=_env("SYBASE_HOST", "IGOR"),
        port=_env("SYBASE_PORT", "2638"),
        db=_env("SYBASE_DB", "Contabil"),
        uid=_env("SYBASE_UID", "ESTACAO08"),
        pwd=_env("SYBASE_PWD", "") or "",
    )
    ora = OracleSettings(
        host=_env("ORACLE_HOST", "localhost"),
        port=_env("ORACLE_PORT", "1521"),
        service=_env("ORACLE_SERVICE", "XE"),
        user=_env("ORACLE_USER", "") or "",
        password=_env("ORACLE_PASSWORD", "") or "",
    )
    exp = Path(_env("EXPORT_DIR", "C:/A") or "C:/A")
    theme = _env("APP_THEME", "SystemDefault") or "SystemDefault"
    return Settings(sybase=syb, oracle=ora, export_dir=exp, theme=theme)

def to_env_dict(s: SybaseSettings) -> dict:
    return {
        "SYBASE_DSN": s.dsn or "",
        "SYBASE_DRIVER": s.driver,
        "SYBASE_HOST": s.host,
        "SYBASE_PORT": s.port,
        "SYBASE_DB": s.db,
        "SYBASE_UID": s.uid,
        "SYBASE_PWD": s.pwd,
    }
