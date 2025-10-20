# ui/login_empresa.py
# -*- coding: utf-8 -*-
"""
UI de Login de Empresa e Logs para o NFSePainel (PySimpleGUI).

Fornece:
- Tab "Login Empresa": busca/seleção de empresa (Oracle CAD) com atualizar e testar conexão (CAD/BAIXA).
- Tab "Logs": viewer assíncrono (fila) com limpeza e auto-scroll.
- Ponte de logs que não trava a UI (QueueHandler + thread que envia eventos para a window).

Integra com:
- services.empresa_service (listar_empresas, buscar_empresas, healthcheck_oracle)

Uso (exemplo de integração no seu loop principal):
    import PySimpleGUI as sg
    from ui.login_empresa import (
        build_login_tab, build_logs_tab,
        LoginEmpresaController, UILogBridge, KEYS
    )

    login_tab = build_login_tab()
    logs_tab  = build_logs_tab()

    layout = [[sg.TabGroup([[login_tab, logs_tab]], key='-TABS-')]]
    window = sg.Window('NFSePainel', layout, finalize=True)

    # Iniciar ponte de logs e controller
    log_bridge = UILogBridge(window)
    log_bridge.start()  # começa a receber logs na aba

    controller = LoginEmpresaController(window)
    controller.load_empresas()  # carrega lista de empresas ao abrir

    while True:
        event, values = window.read()
        if event in (sg.WINDOW_CLOSED, 'Exit'):
            break
        # Delegar eventos para o controller
        controller.handle_event(event, values)
        # Eventos do viewer de logs
        log_bridge.handle_event(event, values)

    # Encerrar limpo (limpa fila/listener)
    log_bridge.stop()
    window.close()

Observações:
- A limpeza de logs acontece quando o app fecha (log_bridge.stop()) — nenhuma rotação/arquivo persistente aqui.
- Este módulo não altera o tema, container principal ou tabs existentes. Ele apenas fornece as duas tabs e o controlador.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import PySimpleGUI as sg

from services.empresa_service import (
    Empresa,
    buscar_empresas,
    healthcheck_oracle,
    listar_empresas,
)

# =============================================================================
# Constantes / KEYS
# =============================================================================

class KEYS:
    # Tabs
    TAB_LOGIN = "-TAB-LOGIN-"
    TAB_LOGS = "-TAB-LOGS-"

    # Login
    EMPRESA_BUSCA = "-EMPRESA-BUSCA-"
    EMPRESA_LIST = "-EMPRESA-LIST-"
    EMPRESA_REFRESH = "-EMPRESA-REFRESH-"
    EMPRESA_SELECT = "-EMPRESA-SELECT-"
    STATUS_CAD = "-STATUS-CAD-"
    STATUS_BAIXA = "-STATUS-BAIXA-"
    TEST_CAD = "-TEST-CAD-"
    TEST_BAIXA = "-TEST-BAIXA-"

    # Logs
    LOG_MULTILINE = "-LOG-TEXT-"
    LOG_CLEAR = "-LOG-CLEAR-"
    LOG_AUTOSCROLL = "-LOG-AUTOSCROLL-"
    LOG_EVENT = "-LOG-LINE-EVENT-"

    # Eventos internos
    EMPRESAS_LOADED = "-EV-EMPRESAS-LOADED-"
    EMPRESA_SELECTED = "-EV-EMPRESA-SELECTED-"
    BUSCA_APLICADA = "-EV-BUSCA-APLICADA-"
    STATUS_ATUALIZADO = "-EV-STATUS-OK-"


# =============================================================================
# Ponte de Logs (não bloqueante)
# =============================================================================

class UILogBridge:
    """
    Conecta o logging do Python a um Multiline do PySimpleGUI de forma assíncrona,
    sem travar a UI. Usa QueueHandler -> Queue -> thread consumidora -> write_event_value.
    """

    def __init__(self, window: sg.Window, level: int = logging.INFO) -> None:
        self.window = window
        self.queue: "queue.Queue[logging.LogRecord]" = queue.Queue(maxsize=1000)
        self.handler = logging.handlers.QueueHandler(self.queue)  # type: ignore[attr-defined]
        self.handler.setLevel(level)
        self.formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        self.listener_thread = threading.Thread(target=self._consumer, name="UILogBridge", daemon=True)
        self._stop = threading.Event()
        self._attached = False

    def start(self) -> None:
        """Anexa ao root logger e inicia a thread consumidora."""
        if self._attached:
            return
        root = logging.getLogger()
        root.addHandler(self.handler)
        # Mantém nível do root no mínimo necessário
        if root.level > logging.DEBUG:
            root.setLevel(logging.INFO)
        self.listener_thread.start()
        self._attached = True

    def stop(self) -> None:
        """Desanexa handler e encerra a thread. Limpa fila."""
        if not self._attached:
            return
        root = logging.getLogger()
        try:
            root.removeHandler(self.handler)
        except Exception:
            pass
        self._stop.set()
        # drena fila rapidamente
        try:
            while not self.queue.empty():
                self.queue.get_nowait()
        except Exception:
            pass

    def _consumer(self) -> None:
        """Consome a fila e envia linhas formatadas para a UI via evento."""
        while not self._stop.is_set():
            try:
                record = self.queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                line = self.formatter.format(record)
            except Exception:
                # fallback simples
                line = f"{getattr(record, 'levelname', 'INFO')} {getattr(record, 'name', '')}: {getattr(record, 'msg', '')}"
            try:
                self.window.write_event_value(KEYS.LOG_EVENT, line)
            except Exception:
                # se a window já fechou, encerramos a thread
                break

    # Integração com loop do PySimpleGUI
    def handle_event(self, event, values) -> None:
        if event == KEYS.LOG_EVENT:
            # Append no Multiline
            ml: sg.Multiline = self.window[KEYS.LOG_MULTILINE]  # type: ignore[assignment]
            autoscroll: bool = bool(values.get(KEYS.LOG_AUTOSCROLL, True))
            text = values.get(KEYS.LOG_EVENT, "")
            try:
                ml.update(value=(ml.get() + (text + "\n")))
                if autoscroll:
                    ml.set_vscroll_position(1.0)
            except Exception:
                pass
        elif event == KEYS.LOG_CLEAR:
            try:
                self.window[KEYS.LOG_MULTILINE].update(value="")
            except Exception:
                pass


# =============================================================================
# Login Empresa - Controller
# =============================================================================

@dataclass
class _State:
    empresas: List[Empresa]
    filtradas: List[Empresa]
    selected: Optional[Empresa]


class LoginEmpresaController:
    """
    Controlador da Tab Login Empresa.
    - Carrega empresas do Oracle CAD
    - Aplica busca
    - Dispara evento EMPRESA_SELECTED quando usuário confirma a seleção
    - Testa conectividade CAD/BAIXA sob demanda
    """

    def __init__(self, window: sg.Window) -> None:
        self.window = window
        self.state = _State(empresas=[], filtradas=[], selected=None)
        self.log = logging.getLogger("nfse.ui.login")

    # --------------- Data loading / busca ---------------

    def load_empresas(self, force: bool = False) -> None:
        """Carrega a lista de empresas do serviço e preenche a Combo/Lista."""
        self.log.info({"event": "login_load_empresas_start"})
        empresas = listar_empresas(force=force)
        self.state.empresas = empresas
        self.state.filtradas = empresas
        nomes = [self._format_option(e) for e in empresas]
        self.window[KEYS.EMPRESA_LIST].update(values=nomes)
        self.window.write_event_value(KEYS.EMPRESAS_LOADED, len(empresas))
        self.log.info({"event": "login_load_empresas_done", "count": len(empresas)})

    def apply_busca(self, term: str) -> None:
        """Aplica busca por termo e atualiza lista."""
        base = self.state.empresas or []
        filtradas = buscar_empresas(term, base=base)
        self.state.filtradas = filtradas
        nomes = [self._format_option(e) for e in filtradas]
        self.window[KEYS.EMPRESA_LIST].update(values=nomes)
        self.window.write_event_value(KEYS.BUSCA_APLICADA, len(filtradas))

    # --------------- Testes de conexão ---------------

    def run_healthchecks(self) -> Dict[str, bool]:
        st = healthcheck_oracle()
        self._update_status_icons(st.get("CAD", False), st.get("BAIXA", False))
        return st

    def _update_status_icons(self, cad_ok: bool, baixa_ok: bool) -> None:
        ok = "✅ OK"    # simples, pode trocar por ícones ou cores
        nok = "❌ Falhou"
        self.window[KEYS.STATUS_CAD].update(value=(ok if cad_ok else nok))
        self.window[KEYS.STATUS_BAIXA].update(value=(ok if baixa_ok else nok))

    # --------------- Seleção ---------------

    @staticmethod
    def _format_option(e: Empresa) -> str:
        # Exibição: "NOME — 00.000.000/0000-00 [ID: 123]"
        cnpj = f" — {e.cnpj}" if e.cnpj else ""
        return f"{(e.nome or '').strip()}{cnpj} [ID: {e.id}]"

    def _parse_selected(self, text: str) -> Optional[Empresa]:
        """Extrai ID do texto e encontra a Empresa correspondente."""
        if not text:
            return None
        # Procura [ID: xxx]
        start = text.rfind("[ID:")
        end = text.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return None
        id_part = text[start + 4 : end].strip()
        for e in self.state.filtradas:
            if e.id == id_part:
                return e
        for e in self.state.empresas:
            if e.id == id_part:
                return e
        return None

    # --------------- Events ---------------

    def handle_event(self, event, values) -> None:
        if event == KEYS.EMPRESA_REFRESH:
            self.load_empresas(force=True)

        elif event == KEYS.EMPRESA_BUSCA:
            term = values.get(KEYS.EMPRESA_BUSCA, "") or ""
            self.apply_busca(term)

        elif event == KEYS.TEST_CAD or event == KEYS.TEST_BAIXA:
            st = self.run_healthchecks()
            self.log.info({"event": "oracle_healthcheck_clicked", **st})

        elif event == KEYS.EMPRESA_SELECT:
            selected_text = values.get(KEYS.EMPRESA_LIST, "")
            # Combo retorna string selecionada
            emp = self._parse_selected(selected_text if isinstance(selected_text, str) else "")
            self.state.selected = emp
            if emp:
                self.log.info({"event": "empresa_selected", "id": emp.id, "nome": emp.nome})
                # Dispara evento p/ o loop principal agir (ex.: abrir conexão Domínio, etc.)
                self.window.write_event_value(KEYS.EMPRESA_SELECTED, {"id": emp.id, "nome": emp.nome, "cnpj": emp.cnpj})
            else:
                self.log.warning({"event": "empresa_selected_none"})

        elif event == KEYS.EMPRESAS_LOADED:
            # Apenas para feedback rápido na UI (opcional)
            pass

        elif event == KEYS.BUSCA_APLICADA:
            # Apenas para feedback rápido na UI (opcional)
            pass


# =============================================================================
# Builders de Tabs
# =============================================================================

def build_login_tab() -> sg.Tab:
    """
    Constrói a Tab "Login Empresa" com:
    - Busca por nome/CNPJ
    - Combo com lista de empresas
    - Botões: Atualizar, Testar CAD, Testar BAIXA, Selecionar
    - Status de conectividade CAD/BAIXA
    """
    # Linha 1: busca e atualizar
    row_busca = [
        sg.Text("Buscar:", size=(8, 1)),
        sg.Input(key=KEYS.EMPRESA_BUSCA, enable_events=True, tooltip="Nome ou CNPJ"),
        sg.Button("Atualizar", key=KEYS.EMPRESA_REFRESH),
    ]

    # Linha 2: lista/seleção
    row_lista = [
        sg.Text("Empresa:", size=(8, 1)),
        sg.Combo(
            values=["(carregando...)"],
            key=KEYS.EMPRESA_LIST,
            size=(68, 1),
            readonly=True,
            enable_events=False,
        ),
        sg.Button("Selecionar", key=KEYS.EMPRESA_SELECT),
    ]

    # Linha 3: status CAD/BAIXA + testar
    row_status = [
        sg.Text("CAD:", size=(5, 1)),
        sg.Text("—", key=KEYS.STATUS_CAD, size=(10, 1)),
        sg.Button("Testar CAD", key=KEYS.TEST_CAD),
        sg.Text("   "),
        sg.Text("BAIXA:", size=(7, 1)),
        sg.Text("—", key=KEYS.STATUS_BAIXA, size=(10, 1)),
        sg.Button("Testar BAIXA", key=KEYS.TEST_BAIXA),
    ]

    layout = [
        row_busca,
        [sg.HorizontalSeparator()],
        row_lista,
        [sg.HorizontalSeparator()],
        row_status,
    ]

    return sg.Tab("Login Empresa", layout, key=KEYS.TAB_LOGIN, expand_x=True, expand_y=False)


def build_logs_tab() -> sg.Tab:
    """
    Constrói a Tab "Logs" com:
    - Multiline somente leitura
    - Toggle de auto-scroll
    - Botão de limpar
    """
    header = [
        sg.Checkbox("Auto-scroll", key=KEYS.LOG_AUTOSCROLL, default=True),
        sg.Button("Limpar", key=KEYS.LOG_CLEAR),
    ]
    viewer = [
        sg.Multiline(
            default_text="",
            key=KEYS.LOG_MULTILINE,
            size=(100, 20),
            autoscroll=False,  # controlamos manualmente
            write_only=True,
            disabled=True,
            expand_x=True,
            expand_y=True,
            font=("Consolas", 9),
            border_width=1,
        )
    ]
    layout = [header, viewer]
    return sg.Tab("Logs", layout, key=KEYS.TAB_LOGS, expand_x=True, expand_y=True)
