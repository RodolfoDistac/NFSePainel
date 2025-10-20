# utils/logs.py
# -*- coding: utf-8 -*-
"""
Utilitários de logging para o NFSePainel.

Objetivos:
- Fornecer logs estruturados (uma linha JSON por evento).
- Suportar sink assíncrono para GUI (PySimpleGUI) sem travar a UI.
- Oferecer "contexto" global de log (empresa, usuário, estação etc.) para ser
  mesclado automaticamente em todos os registros.

API pública:
    - create_gui_sink(window, multiline_key, event_key) -> AsyncLogSink
    - log_emit(sink, level, event, **fields) -> dict
    - format_record(record) -> str
    - set_context(dict | None) -> None   # substitui o contexto global
    - add_context(**kvs) -> None          # atualiza/incremental

Uso típico (assíncrono recomendado):
    from utils.logs import create_gui_sink, log_emit, format_record

    window = sg.Window(..., finalize=True)
    SINK = create_gui_sink(window, multiline_key="-LOG-", event_key="-LOGEVT-")

    # no loop de eventos do panel.py:
    if event == "-LOGEVT-":
        rec = values[event]  # dict
        window["-LOG-"].print(format_record(rec))

    # Em qualquer lugar:
    log_emit(SINK, "info", "processamento_iniciado", total=123)

Contexto global:
    from utils.logs import set_context, add_context
    set_context({"empresa":"ACME","usuario":"RODOLFO","estacao":"19"})
    add_context(sessao="2025-10")  # incrementa sem substituir

Observações:
- Não persistimos em arquivo (requisito). Tudo fica na UI/console.
- Evitar passar dados sensíveis; chaves com nomes de segredo serão mascaradas.
"""

from __future__ import annotations

import datetime as _dt
import json
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    import PySimpleGUI as sg  # type: ignore
except Exception:  # pragma: no cover
    sg = None  # permite import em contextos sem GUI


__all__ = [
    "AsyncLogSink",
    "create_gui_sink",
    "log_emit",
    "format_record",
    "set_context",
    "add_context",
]


# --------------------------------------------------------------------------------------
# Contexto global de logs (thread-safe)
# --------------------------------------------------------------------------------------

_CTX_LOCK = threading.RLock()
_CONTEXT: Dict[str, Any] = {}


def set_context(ctx: Optional[Dict[str, Any]]) -> None:
    """
    Substitui o contexto global.
    Use None ou {} para limpar.
    """
    with _CTX_LOCK:
        _CONTEXT.clear()
        if ctx:
            _CONTEXT.update(_sanitize_dict(ctx))


def add_context(**kvs: Any) -> None:
    """
    Atualiza/incrementa o contexto global com os pares informados.
    """
    if not kvs:
        return
    with _CTX_LOCK:
        _CONTEXT.update(_sanitize_dict(kvs))


def _snapshot_context() -> Dict[str, Any]:
    with _CTX_LOCK:
        return dict(_CONTEXT)


# --------------------------------------------------------------------------------------
# Formatação
# --------------------------------------------------------------------------------------

def _now_iso() -> str:
    # ISO-like com segundos (sem timezone para manter compat com prints anteriores)
    return _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def format_record(record: Dict[str, Any]) -> str:
    """
    Converte um dict de log em linha única JSON (utf-8 friendly).
    """
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


# --------------------------------------------------------------------------------------
# Sanitização (evitar vazamento de segredos / valores não serializáveis)
# --------------------------------------------------------------------------------------

_SECRET_KEYS = {"password", "pwd", "senha", "secret", "token", "apikey", "api_key", "key"}


def _sanitize_value(k: str, v: Any) -> Any:
    # mascara chaves com possíveis segredos
    if k.lower() in _SECRET_KEYS:
        return "****"
    # verifica serialização
    try:
        json.dumps(v)
        return v
    except Exception:
        return str(v)


def _sanitize_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _sanitize_value(k, v) for k, v in d.items()}


# --------------------------------------------------------------------------------------
# Sink assíncrono para GUI
# --------------------------------------------------------------------------------------

@dataclass
class AsyncLogSink:
    """
    Representa um "destino" de logs assíncrono para a GUI.

    - window: sg.Window (usamos write_event_value para enfileirar update sem travar a UI)
    - multiline_key: chave do elemento Multiline (não usada diretamente aqui; fica com o panel)
    - event_key: chave do evento customizado que o panel tratará no loop
    """
    window: "sg.Window"
    multiline_key: str
    event_key: str

    def post(self, record: Dict[str, Any]) -> None:
        """
        Envia o registro para a fila de eventos da janela.
        """
        try:
            self.window.write_event_value(self.event_key, record)
        except Exception:
            # Se a janela estiver fechada ou indisponível, ignoramos.
            pass


def create_gui_sink(window: "sg.Window", *, multiline_key: str, event_key: str) -> AsyncLogSink:
    """
    Cria um sink assíncrono para a janela informada.
    """
    return AsyncLogSink(window=window, multiline_key=multiline_key, event_key=event_key)


# --------------------------------------------------------------------------------------
# Emissão
# --------------------------------------------------------------------------------------

def log_emit(sink: Any, level: str, event: str, **fields: Any) -> Dict[str, Any]:
    """
    Emite um registro de log estruturado.

    Parâmetros
    ----------
    sink:
        - AsyncLogSink -> usa write_event_value (assíncrono).
        - sg.Multiline -> usa .print (síncrono).
        - None ou desconhecido -> apenas retorna o dict (sem I/O).
    level: "debug" | "info" | "warn" | "error"
    event: nome curto do evento (snake_case)
    **fields: campos adicionais (não envie dados sensíveis/PII)

    Retorna
    -------
    dict do registro emitido.
    """
    lvl = (level or "info").lower()
    if lvl not in ("debug", "info", "warn", "error"):
        lvl = "info"

    # snapshot do contexto no momento da emissão
    ctx = _snapshot_context()

    # contexto -> depois campos do evento (podem sobrescrever o contexto)
    record: Dict[str, Any] = {"ts": _now_iso(), "level": lvl, "event": event}
    record.update(_sanitize_dict(ctx))
    record.update(_sanitize_dict(fields))

    # Destino assíncrono (preferido)
    if isinstance(sink, AsyncLogSink):
        sink.post(record)
        return record

    # Destino síncrono (Multiline)
    if sg is not None and hasattr(sink, "print"):
        try:
            sink.print(format_record(record))
        except Exception:
            pass
        return record

    # Sem destino conhecido: apenas retorna (útil para testes)
    return record
