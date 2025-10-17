# utils/logs.py
"""
Logs estruturados (JSON) com mascaramento de PII (CPF/CNPJ) e suporte opcional a UI.
- Saída sempre enviada para stderr.
- Se receber um componente com método .print (ex.: sg.Multiline), também imprime na UI.
- Contexto padrão (campos fixos) pode ser configurado via set_context()/add_context().

Exemplo:
    from utils.logs import log_emit, set_context
    set_context(app="nfse-painel", version="0.1.0")
    log_emit(multiline, "info", "processamento_concluido", total=10, ok=9, fail=1)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping
import json
import sys

__all__ = ["log_emit", "set_context", "add_context"]

# ----------------------------
# Contexto global de logging
# ----------------------------
_DEFAULT_CONTEXT: Dict[str, Any] = {}


def set_context(**ctx: Any) -> None:
    """Substitui completamente o contexto padrão."""
    global _DEFAULT_CONTEXT
    _DEFAULT_CONTEXT = dict(ctx or {})


def add_context(**ctx: Any) -> None:
    """Mescla (atualiza) o contexto padrão com novos campos."""
    _DEFAULT_CONTEXT.update(ctx or {})


# ----------------------------
# Helpers de sanitização
# ----------------------------
def _mask_pii(val: Any) -> Any:
    """
    Mascara potenciais CPF/CNPJ (somente dígitos, tamanhos comuns 11–14).
    Mantém outros valores inalterados.
    """
    if isinstance(val, str) and val.isdigit() and 11 <= len(val) <= 14:
        return f"{val[:3]}***{val[-2:]}"
    return val


def _sanitize(obj: Any) -> Any:
    """
    Aplica _mask_pii recursivamente em dicts/listas; converte objetos não-JSON em str.
    """
    if isinstance(obj, Mapping):
        return {str(k): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, (str, type(None), int, float, bool)):
        return _mask_pii(obj)
    # fallback para tipos não serializáveis
    return _mask_pii(str(obj))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_level(level: str) -> str:
    lv = (level or "").strip().lower()
    if lv in {"warning"}:
        return "warn"
    if lv not in {"debug", "info", "warn", "error"}:
        return "info"
    return lv


# ----------------------------
# API principal
# ----------------------------
def log_emit(multiline: Any, level: str, msg: str, **fields: Any) -> Dict[str, Any]:
    """
    Emite um log JSON no stderr e (se disponível) no componente de UI.

    Args:
        multiline: componente com método .print (ex.: PySimpleGUI Multiline) ou None.
        level: 'debug' | 'info' | 'warn' | 'error'
        msg: mensagem curta (slug) indicando o evento.
        **fields: campos adicionais (serão saneados/mask).

    Returns:
        O payload (dict) emitido.
    """
    payload: Dict[str, Any] = {
        "ts": _now_iso(),
        "level": _norm_level(level),
        "msg": msg,
        **_DEFAULT_CONTEXT,              # contexto fixo
        **_sanitize(fields),             # campos variáveis saneados
    }

    line = json.dumps(payload, ensure_ascii=False)

    # UI (se houver e suportar .print)
    try:
        if multiline is not None and hasattr(multiline, "print"):
            multiline.print(line)
    except Exception:
        # Nunca quebrar a aplicação por causa do log na UI
        pass

    # stderr (sempre)
    print(line, file=sys.stderr)

    return payload
