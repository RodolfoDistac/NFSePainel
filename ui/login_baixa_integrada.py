# ui/login_baixa_integrada.py
# -*- coding: utf-8 -*-
"""
Login - Baixa Integrada (estilo do sistema Java)

- Empresa: combo vindo do Oracle (perfil CAD), usando services.empresa_service
- Usuário: texto (default provisório = "RODOLFO")
- Estação: preenchida automaticamente ao digitar/alterar o usuário
           via infra.oracle.buscar_estacao (perfil BAIXA)

Retorno:
    show_login_baixa_integrada(...) -> dict | None

    {
        "empresa_id": <str>,          # id exibido (pode ser COD_DOMINIO se vier como ID)
        "empresa_nome": <str>,
        "empresa_cnpj": <str>,
        "cod_dominio": <str|None>,    # se existir; caso contrário, igual a empresa_id
        "usuario": <str>,             # upper()
        "estacao": <str|None>
    }
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional

import PySimpleGUI as sg

from services.empresa_service import Empresa, listar_empresas, buscar_empresas
from infra.oracle import buscar_estacao

logger = logging.getLogger("nfse.ui.login_baixa")
if not logger.handlers:
    _h = logging.StreamHandler()
    _fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s login_baixa %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    _h.setFormatter(_fmt)
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _fmt_empresa(e: Empresa) -> str:
    # Ex.: "SINDICATO — 00.000.000/0000-00 [ID: 424]"
    cnpj = f" — {e.cnpj}" if e.cnpj else ""
    return f"{(e.nome or '').strip()}{cnpj} [ID: {e.id}]"


def _parse_empresa_id(text: str) -> Optional[str]:
    if not text:
        return None
    start = text.rfind("[ID:")
    end = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return None
    return text[start + 4 : end].strip()


def _find_by_id(empresas: List[Empresa], emp_id: str) -> Optional[Empresa]:
    for e in empresas:
        if e.id == emp_id:
            return e
    return None


# --------------------------------------------------------------------------------------
# UI
# --------------------------------------------------------------------------------------
def show_login_baixa_integrada(
    parent: Optional[sg.Window] = None,
    *,
    default_usuario: str = "RODOLFO",
) -> Optional[Dict[str, str]]:
    """
    Abre o modal de Login (Empresa/Usuário/Estação) e retorna o payload ao confirmar,
    ou None se o usuário cancelar.
    """
    # Carrega empresas (Oracle CAD)
    try:
        empresas = listar_empresas(force=True)
    except Exception as exc:
        sg.popup_error(
            "Não foi possível carregar a lista de empresas do Oracle (CAD).\n"
            f"Detalhe: {exc}"
        )
        return None

    if not empresas:
        sg.popup_error(
            "Nenhuma empresa retornada do Oracle (CAD).\n"
            "Ajuste a variável ORACLE_CAD_EMPRESAS_QUERY para a tabela correta."
        )
        return None

    nomes_fmt = [_fmt_empresa(e) for e in empresas]

    # Chaves
    KEY_BUSCA = "-BUSCA-"
    KEY_EMP = "-EMPRESA-"
    KEY_USR = "-USUARIO-"
    KEY_EST = "-ESTACAO-"
    KEY_OK = "-OK-"
    KEY_CANCEL = "-CANCEL-"
    KEY_TESTE = "-TEST-"
    KEY_TIMER = "-TIMER-"

    # Layout (sem dependências de imagem para manter simples e portátil)
    layout = [
        [sg.Text("Empresa:"), sg.Combo(
            values=nomes_fmt,
            key=KEY_EMP,
            default_value=nomes_fmt[0],
            readonly=True,
            size=(60, 1)
        )],
        [sg.Text("Buscar:"), sg.Input(key=KEY_BUSCA, enable_events=True, expand_x=True)],
        [sg.Text("Usuário:"), sg.Input(default_usuario, key=KEY_USR, enable_events=True, size=(20, 1))],
        [sg.Text("Estação:"), sg.Input("", key=KEY_EST, disabled=True, size=(20, 1)),
         sg.Push(), sg.Button("Entrar", key=KEY_OK), sg.Button("Cancelar", key=KEY_CANCEL)],
    ]

    w = sg.Window(
        "Login - Baixa Integrada",
        layout,
        modal=True,
        finalize=True,
        keep_on_top=False,
        icon=None,
    )

    # ---------- Atualização automática da estação com debounce ----------
    # Estratégia simples: a cada alteração do usuário, armamos um "timer" (thread)
    # que espera 300ms; se o texto não mudou, consulta o Oracle BAIXA e atualiza.
    est_lock = threading.Lock()
    last_user_text = default_usuario.strip().upper()
    pending_job: Optional[threading.Thread] = None

    def schedule_station_lookup(text_now: str) -> None:
        nonlocal pending_job, last_user_text

        with est_lock:
            last_user_text = (text_now or "").strip().upper()

            def _job(expected_text: str):
                time.sleep(0.30)
                with est_lock:
                    # Se o texto mudou desde que agendamos, aborta
                    if expected_text != last_user_text:
                        return
                # Busca estação
                try:
                    est = buscar_estacao(expected_text)
                except Exception as exc_inner:
                    logger.error({"event": "buscar_estacao_error", "err": str(exc_inner)})
                    est = None
                try:
                    # Atualiza campo na thread da UI via write_event
                    w.write_event_value(KEY_TIMER, est or "")
                except Exception:
                    pass

            # dispara thread
            t = threading.Thread(target=_job, args=(last_user_text,), daemon=True)
            pending_job = t
            t.start()

    # Primeira resolução de estação (para o default)
    schedule_station_lookup(default_usuario)

    selected_payload: Optional[Dict[str, str]] = None
    current_list = empresas[:]

    while True:
        ev, vals = w.read()
        if ev in (sg.WINDOW_CLOSED, KEY_CANCEL):
            selected_payload = None
            break

        if ev == KEY_BUSCA:
            term = vals.get(KEY_BUSCA, "") or ""
            current_list = buscar_empresas(term, base=empresas)
            nomes_fmt = [_fmt_empresa(e) for e in current_list]
            if not nomes_fmt:
                nomes_fmt = ["(sem resultados)"]
            w[KEY_EMP].update(values=nomes_fmt, value=nomes_fmt[0])

        if ev == KEY_USR:
            schedule_station_lookup(vals.get(KEY_USR, "") or "")

        if ev == KEY_TIMER:
            # Resultado da consulta assíncrona da estação
            est = vals.get(KEY_TIMER, "") or ""
            w[KEY_EST].update(est)

        if ev == KEY_OK:
            # Empresa
            texto = vals.get(KEY_EMP, "") or ""
            emp_id = _parse_empresa_id(texto)
            emp_sel = _find_by_id(current_list, emp_id) if emp_id else None
            if not emp_sel:
                sg.popup_error("Selecione uma empresa válida.")
                continue

            # Usuário + Estação
            usuario = (vals.get(KEY_USR, "") or "").strip().upper()
            if not usuario:
                sg.popup_error("Informe o usuário.")
                continue
            est = (vals.get(KEY_EST, "") or "").strip()
            if not est:
                # Tenta resolver se ainda não veio
                est = buscar_estacao(usuario) or ""

            selected_payload = {
                "empresa_id": emp_sel.id,
                "empresa_nome": emp_sel.nome,
                "empresa_cnpj": emp_sel.cnpj,
                "cod_dominio": (emp_sel.cod_dominio or emp_sel.id),
                "usuario": usuario,
                "estacao": est or None,
            }
            break

    w.close()
    return selected_payload
