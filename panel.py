# panel.py
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Mapping
from decimal import Decimal, InvalidOperation

import PySimpleGUI as sg

from config.settings import load_settings, to_env_dict
from services.dominio_export import export_final, enviar_cabecalho_tomador_dominio
from services.dominio_import import buscar_clientes_fornecedores
from services.dominio_nfse import buscar_nfse_por_numeros
from services.parcelas import (
    calcular_vencimento_padrao,
    format_dd_mm_aaaa,
    parse_dd_mm_aaaa,
    aplicar_parcelas_e_acumuladores,
)
from ui.login_baixa_integrada import show_login_baixa_integrada

# ---------------- Config e constantes ----------------

# Colunas (ordem exata solicitada)
COLUMNS: List[str] = [
    "TOMADOR", "NFE", "EMISSAO", "VALOR", "ALIQ",
    "INSS", "IR", "PIS", "COFINS", "CSLL",
    "ISS_RET", "ISS_NORMAL", "DISCRIMINACAO"
]
_NUMERIC_COLS = {"VALOR", "ALIQ", "INSS", "IR", "PIS", "COFINS", "CSLL", "ISS_RET", "ISS_NORMAL"}

# Carrega .env (se houver) e configurações padrão
SETTINGS = load_settings()
DEFAULT_EXPORT_DIR = SETTINGS.export_dir

# Overrides de credenciais Sybase (definido pelo "Login empresa", por sessão)
G_SYBASE_CFG: Optional[Mapping[str, str]] = None

# Empresa selecionada (Oracle CAD / Domínio)
G_EMPRESA_SEL: Optional[Dict[str, str]] = None


# ---------------- Utilitários ----------------

def _compute_scaling(screen_w: int, screen_h: int) -> float:
    if screen_h >= 2160:
        return 1.6
    if screen_h >= 1440:
        return 1.3
    if screen_h >= 1080:
        return 1.1
    return 1.0

def _normalize_path(p: str) -> str:
    return (p or "").strip().strip('"').strip("'")

def _validate_input_path(raw_path: str) -> Optional[str]:
    p = Path(_normalize_path(raw_path))
    if p.exists() and (p.is_dir() or p.suffix.lower() in {".zip", ".xml"}):
        return str(p)
    return None

def _digits_only(doc: str | None) -> str:
    return "" if doc is None else "".join(ch for ch in str(doc) if ch.isdigit())

def _brl_to_decimal(s: str | None) -> Decimal:
    if s is None:
        return Decimal("0")
    t = str(s).strip()
    if not t:
        return Decimal("0")
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", ".")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return Decimal("0")

def _decimal_to_brl(d: Decimal) -> str:
    v = d.quantize(Decimal("0.01"))
    s = f"{v:.2f}"
    inteiro, frac = s.split(".")
    inteiro = int(inteiro)
    inteiro = f"{inteiro:,}".replace(",", ".")
    return f"{inteiro},{frac}"

def _safe_long_job(input_str: str):
    try:
        rows, counts, errors = _parse_all(input_str)
        return ("ok", (rows, counts, errors))
    except Exception as e:
        return ("error", f"{type(e).__name__}: {e}")

def _parse_all(input_str: str) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    """Lê todos os XMLs e retorna linhas já prontas para a tabela."""
    from parsers.nfse_abrasf import NFSeParser
    from dataio.loaders import iter_xml_bytes

    parser = NFSeParser()
    path = Path(input_str)

    rows: List[Dict[str, Any]] = []
    counts = {"total": 0, "ok": 0, "fail": 0}
    errors: List[str] = []

    for name, xml_bytes in iter_xml_bytes(path):
        counts["total"] += 1
        try:
            r = parser.parse(xml_bytes, name).to_row()
            r["TOMADOR"] = _digits_only(r.get("TOMADOR"))
            rows.append(r)
            counts["ok"] += 1
        except Exception as e:
            counts["fail"] += 1
            errors.append(f"{name}: {e}")

    return rows, counts, errors

def _apply_filter(rows: List[Dict[str, Any]], text: str) -> List[Dict[str, Any]]:
    q = (text or "").lower().strip()
    if not q:
        return list(rows)

    def match(row: Dict[str, Any]) -> bool:
        for col in COLUMNS:
            v = row.get(col)
            if v is None:
                continue
            if q in str(v).lower():
                return True
        return False

    return [r for r in rows if match(r)]

def _parse_brl_number(s: str) -> float:
    s = str(s or "").strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0

def _sort_rows(rows: List[Dict[str, Any]], col_idx: int, ascending: bool) -> List[Dict[str, Any]]:
    col = COLUMNS[col_idx]
    if col in _NUMERIC_COLS:
        keyfunc = lambda r: _parse_brl_number(r.get(col, "0"))
    else:
        keyfunc = lambda r: str(r.get(col, "")).lower()
    return sorted(rows, key=keyfunc, reverse=not ascending)

def _make_row_colors(rows: List[Dict[str, Any]]):
    colors = []
    for idx, r in enumerate(rows):
        if str(r.get("STATUS", "")).lower() == "cancelada":
            colors.append((idx, "black", "#ffcccc"))
    return colors

def _autosize_table(table_elem: sg.Table, values: List[List[str]], headings: List[str]) -> None:
    tv = table_elem.Widget
    try:
        import tkinter.font as tkfont
        f = tkfont.nametofont("TkDefaultFont")
    except Exception:
        f = None

    sample = values[:1000] if len(values) > 1000 else values
    for i, head in enumerate(headings):
        texts = [str(head)]
        for row in sample:
            if i < len(row):
                texts.append(str(row[i]))
        if f is not None:
            width = max(f.measure(t) for t in texts) + 24
        else:
            width = max(len(t) for t in texts) * 7 + 24
        width = max(60, min(600, width))
        try:
            tv.column(str(i), width=width, stretch=(head == "DISCRIMINACAO"))
        except Exception:
            pass

def _compare_rows(a: Dict[str, str], b: Dict[str, str]) -> bool:
    keys = ["VALOR","ALIQ","INSS","IR","PIS","COFINS","CSLL","ISS_RET","ISS_NORMAL"]
    for k in keys:
        if (a.get(k) or "").strip() != (b.get(k) or "").strip():
            return False
    return True

def _compute_totals(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    totals: Dict[str, Decimal] = {k: Decimal("0") for k in _NUMERIC_COLS}
    for r in rows:
        for k in _NUMERIC_COLS:
            totals[k] += _brl_to_decimal(r.get(k))
    return {k: _decimal_to_brl(v) for k, v in totals.items()}


# ---------------- Login (Domínio) ----------------

def _login_dialog() -> Optional[Mapping[str, str]]:
    from utils.logs import log_emit, create_gui_sink, format_record  # safe import se usado fora do main

    base = to_env_dict(SETTINGS.sybase)
    layout = [
        [sg.Text("Conexão Domínio (SQL Anywhere / Sybase)", font=("Segoe UI", 11, "bold"))],
        [sg.Text("Driver"), sg.Input(base["SYBASE_DRIVER"], key="-DRV-", size=(30, 1))],
        [sg.Text("Host"), sg.Input(base["SYBASE_HOST"], key="-HOST-", size=(20, 1)),
         sg.Text("Porta"), sg.Input(base["SYBASE_PORT"], key="-PORT-", size=(7, 1))],
        [sg.Text("Banco"), sg.Input(base["SYBASE_DB"], key="-DB-", size=(20, 1))],
        [sg.Text("Usuário"), sg.Input(base["SYBASE_UID"], key="-UID-", size=(20, 1)),
         sg.Text("Senha"), sg.Input(base["SYBASE_PWD"], key="-PWD-", size=(20, 1), password_char="•")],
        [sg.Push(),
         sg.Button("Testar conexão", key="-TEST-"),
         sg.Button("Aplicar (sessão)", key="-APPLY-"),
         sg.Button("Cancelar", key="-CLOSE-")]
    ]
    w = sg.Window("Login empresa (manual)", layout, modal=True, finalize=True)
    cfg: Optional[Mapping[str, str]] = None
    while True:
        ev, vals = w.read()
        if ev in (sg.WINDOW_CLOSED, "-CLOSE-"):
            cfg = None
            break
        if ev == "-TEST-":
            try:
                import pyodbc
            except Exception as e:
                sg.popup_error("pyodbc não está disponível.\nInstale os requisitos e o driver ODBC do SQL Anywhere.\n\n" + str(e))
                continue
            test_cfg = {
                "SYBASE_DSN": "",
                "SYBASE_DRIVER": vals["-DRV-"].strip(),
                "SYBASE_HOST": vals["-HOST-"].strip(),
                "SYBASE_PORT": vals["-PORT-"].strip(),
                "SYBASE_DB": vals["-DB-"].strip(),
                "SYBASE_UID": vals["-UID-"].strip(),
                "SYBASE_PWD": vals["-PWD-"],
            }
            try:
                from infra.sybase import connect
                with connect(test_cfg) as con:
                    cur = con.cursor()
                    cur.execute("SELECT 1")
                    cur.fetchall()
                sg.popup_ok("Conexão OK!")
            except Exception as e:
                sg.popup_error("Falha ao conectar:\n" + str(e))
        if ev == "-APPLY-":
            cfg = {
                "SYBASE_DSN": "",
                "SYBASE_DRIVER": vals["-DRV-"].strip(),
                "SYBASE_HOST": vals["-HOST-"].strip(),
                "SYBASE_PORT": vals["-PORT-"].strip(),
                "SYBASE_DB": vals["-DB-"].strip(),
                "SYBASE_UID": vals["-UID-"].strip(),
                "SYBASE_PWD": vals["-PWD-"],
            }
            break
    w.close()
    return cfg


# ---------------- Janelas auxiliares ----------------

def _show_clientes_window(encontrados: List[Dict[str, str]], nao_encontrados: List[str]) -> None:
    headings = ["TIPO", "DOC", "RAZAO", "FANTASIA", "IE", "MUNICIPIO", "UF"]
    values = [[r.get(h, "") for h in headings] for r in encontrados]

    tab1 = [
        [sg.Text(f"Encontrados: {len(encontrados)}")],
        [sg.Table(values=values,
                  headings=headings,
                  key="-TAB-CLI-",
                  expand_x=True,
                  expand_y=True,
                  auto_size_columns=True,
                  display_row_numbers=False,
                  justification="left")],
        [sg.Push(), sg.Button("Exportar CSV (Encontrados)", key="-EXP-CLI-CSV-")]
    ]

    notf = "\n".join(nao_encontrados) if nao_encontrados else ""
    tab2 = [
        [sg.Text(f"Não encontrados: {len(nao_encontrados)}")],
        [sg.Multiline(notf,
                      size=(60, 8),
                      key="-NFOUND-",
                      expand_x=True,
                      expand_y=True,
                      disabled=True)],
        [sg.Push(), sg.Button("Copiar lista", key="-COPY-NF-")]
    ]

    layout = [
        [sg.TabGroup([[sg.Tab("Encontrados", tab1), sg.Tab("Não encontrados", tab2)]],
                     expand_x=True, expand_y=True)],
        [sg.Push(), sg.Button("Fechar")]
    ]

    w = sg.Window("Clientes / Fornecedores (Domínio)",
                  layout,
                  modal=True,
                  resizable=True,
                  finalize=True,
                  size=(900, 500))

    while True:
        ev, vals = w.read()
        if ev in (sg.WINDOW_CLOSED, "Fechar"):
            break

        if ev == "-EXP-CLI-CSV-":
            out = sg.popup_get_file("Salvar CSV (Encontrados)",
                                    save_as=True,
                                    default_extension=".csv",
                                    file_types=(("CSV", "*.csv"),))
            if out:
                try:
                    import csv
                    with open(out, "w", newline="", encoding="utf-8") as f:
                        wr = csv.writer(f, delimiter=";")
                        wr.writerow(headings)
                        for r in encontrados:
                            wr.writerow([r.get(h, "") for h in headings])
                    sg.popup_ok(f"Exportado para: {out}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar: {e}")

        if ev == "-COPY-NF-":
            try:
                import pyperclip
                pyperclip.copy(notf)
                sg.popup_ok("Copiado para a área de transferência.")
            except Exception:
                try:
                    sg.clipboard_set(notf)
                    sg.popup_ok("Copiado para a área de transferência.")
                except Exception as e:
                    sg.popup_error(f"Não foi possível copiar: {e}")

    w.close()


def _show_nfse_dominio_window(dominio_rows: List[Dict[str, str]], painel_rows: List[Dict[str, str]]) -> None:
    idx_painel = {(r.get("NFE"), r.get("TOMADOR")): r for r in painel_rows}

    enriched: List[Dict[str, str]] = []
    ok = diver = novos = 0
    for d in dominio_rows:
        key = (d.get("NFE"), d.get("TOMADOR"))
        p = idx_painel.get(key)
        status = "NOVA"
        if p:
            status = "OK" if _compare_rows(p, d) else "DIVERGENTE"
        if status == "OK": ok += 1
        elif status == "DIVERGENTE": diver += 1
        else: novos += 1
        e = {"STATUS": status}
        e.update(d)
        enriched.append(e)

    headings = ["STATUS"] + COLUMNS
    values = [[r.get(h, "") for h in headings] for r in enriched]

    layout = [
        [sg.Text(f"Domínio: {len(dominio_rows)} registro(s) | OK: {ok} | Divergente: {diver} | Novo: {novos}")],
        [sg.Table(values=values,
                  headings=headings,
                  key="-TAB-NFSE-",
                  expand_x=True,
                  expand_y=True,
                  auto_size_columns=True,
                  display_row_numbers=False,
                  justification="left")],
        [sg.Push(),
         sg.Button("Exportar CSV", key="-EXP-NFSE-CSV-"),
         sg.Button("Fechar")]
    ]
    w = sg.Window("NFS-e do Domínio (Conciliação)", layout, modal=True, resizable=True, finalize=True, size=(1100, 520))

    while True:
        ev, vals = w.read()
        if ev in (sg.WINDOW_CLOSED, "Fechar"):
            break
        if ev == "-EXP-NFSE-CSV-":
            out = sg.popup_get_file("Salvar CSV", save_as=True, default_extension=".csv", file_types=(("CSV","*.csv"),))
            if out:
                try:
                    import csv
                    with open(out, "w", newline="", encoding="utf-8") as f:
                        wr = csv.writer(f, delimiter=";")
                        wr.writerow(headings)
                        for r in enriched:
                            wr.writerow([r.get(h,"") for h in headings])
                    sg.popup_ok(f"Exportado para: {out}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar: {e}")
    w.close()


def _parcelas_dialog() -> Optional[str]:
    padrao = format_dd_mm_aaaa(calcular_vencimento_padrao())
    layout = [
        [sg.Text("Gerar 1 parcela para cada NF (linhas visíveis).")],
        [sg.Text("Vencimento (dd-mm-aaaa):"), sg.Input(padrao, key="-VENC-", size=(12,1))],
        [sg.Text("Ex.: se hoje for 16/10/2025 → padrão 30-09-2025")],
        [sg.Push(), sg.Button("Aplicar", key="-OK-"), sg.Button("Cancelar")]
    ]
    w = sg.Window("Assistente de Parcelas", layout, modal=True, finalize=True)
    venc: Optional[str] = None
    while True:
        ev, vals = w.read()
        if ev in (sg.WINDOW_CLOSED, "Cancelar"):
            venc = None
            break
        if ev == "-OK-":
            d = parse_dd_mm_aaaa(vals.get("-VENC-", ""))
            if not d:
                sg.popup_error("Data inválida. Use o formato dd-mm-aaaa, por exemplo 30-09-2025.")
                continue
            venc = format_dd_mm_aaaa(d)
            break
    w.close()
    return venc


# ---------------- Janela principal ----------------

def _make_window(theme: Optional[str] = None) -> sg.Window:
    sg.theme(theme or SETTINGS.theme or "SystemDefault")
    sw, sh = sg.Window.get_screen_size()
    scale = _compute_scaling(sw, sh)
    sg.set_options(dpi_awareness=True, scaling=scale, font=("Segoe UI", 10))
    win_size = (int(sw * 0.9), int(sh * 0.9))

    bar_actions = [[
        sg.Button("Login empresa", key="-LOGIN-"),
        sg.Button("Importar XMLs", key="-LOAD-BAR-"),
        sg.Button("Exportar Cabeçalho + Tomador", key="-EXP-HEAD-"),
        sg.Button("Importar Clientes", key="-IMP-CLI-"),
        sg.Button("Importar NFS Domínio", key="-IMP-NFS-"),
        sg.Button("Gerar Parcelas (manual)", key="-GERA-PARC-"),
        sg.Button("Exportar Final", key="-EXP-FINAL-"),
    ]]

    row_input = [[
        sg.Text("Entrada (pasta, .zip ou .xml):", size=(25, 1)),
        sg.Input(key="-INPUT-", expand_x=True),
        sg.FolderBrowse("Pasta", target="-INPUT-"),
        sg.FileBrowse("Arquivo", file_types=(("ZIP/XML", "*.zip;*.xml"), ("Todos", "*.*")), target="-INPUT-"),
        sg.Button("Carregar", key="-LOAD-", bind_return_key=True),
    ]]

    row_filter = [[
        sg.Text("Filtro (qualquer coluna):", size=(25, 1)),
        sg.Input(key="-FILTER-", expand_x=True, enable_events=True),
        sg.Button("Limpar Filtro", key="-CLEARF-"),
        sg.Push(),
        sg.Button("Exportar CSV", key="-EXPORT-"),
    ]]

    table = sg.Table(
        values=[],
        headings=COLUMNS,
        key="-TABLE-",
        auto_size_columns=False,
        justification="left",
        display_row_numbers=False,
        enable_events=True,
        num_rows=20,
        expand_x=True,
        expand_y=True,
        vertical_scroll_only=False,
        tooltip="Resultados das NFS-e",
    )

    row_status = [
        sg.Text("Status:"), sg.Text("Aguardando…", key="-STATUS-", expand_x=True),
        sg.Text("Total:"), sg.Text("0", key="-TOT-"),
        sg.Text("  Sucesso:"), sg.Text("0", key="-OK-"),
        sg.Text("  Falha:"), sg.Text("0", key="-FAIL-"),
    ]

    totals_labels = [
        ("VALOR", "-TVAL-"), ("ALIQ", "-TALIQ-"), ("INSS", "-TINSS-"), ("IR", "-TIR-"),
        ("PIS", "-TPIS-"), ("COFINS", "-TCOF-"), ("CSLL", "-TCSLL-"),
        ("ISS_RET", "-TISSR-"), ("ISS_NORMAL", "-TISSN-")
    ]
    totals_row = []
    for label, key in totals_labels:
        totals_row += [sg.Text(f"{label}:"), sg.Text("0,00", key=key, size=(12,1))]
    frame_tot = sg.Frame("Totais (linhas visíveis)", [[sg.Column([totals_row], scrollable=False)]], expand_x=True)

    frame_logs = sg.Frame(
        "Logs",
        [[sg.Multiline(
            key="-LOG-",
            size=(10, 8),
            expand_x=True,
            expand_y=True,
            autoscroll=True,
            write_only=True
        )]],
        expand_x=True,
        expand_y=True
    )

    layout = [
        *bar_actions,
        *row_input,
        *row_filter,
        [table],
        [sg.Column([row_status], expand_x=True)],
        [frame_tot],
        [frame_logs],
        [sg.Push(), sg.Button("Sair", key="-EXIT-")],
    ]

    window = sg.Window(
        "Painel NFSe",
        layout,
        size=win_size,
        resizable=True,
        finalize=True,
        enable_close_attempted_event=True,
    )
    try:
        window.maximize()
    except Exception:
        pass
    _attach_header_sort(window)
    return window

def _attach_header_sort(window: sg.Window) -> None:
    tv = window["-TABLE-"].Widget
    for i, col in enumerate(COLUMNS):
        try:
            tv.heading(str(i), text=col, command=lambda c=i: window.write_event_value("-SORT-", c))
        except Exception:
            pass


# ---------------- Main / Loop de eventos ----------------

def main(input_path: Optional[str] = None, theme: Optional[str] = None) -> int:
    global G_SYBASE_CFG, G_EMPRESA_SEL  # globais

    # Importa utilitários de log (agora com sink assíncrono)
    from utils.logs import create_gui_sink, log_emit, format_record
    from dataio.exporters import export_csv

    window = _make_window(theme)

    # Sink de logs assíncronos: evento customizado "-LOGEVT-"
    SINK = create_gui_sink(window, multiline_key="-LOG-", event_key="-LOGEVT-")

    all_rows: List[Dict[str, Any]] = []
    view_rows: List[Dict[str, Any]] = []
    sort_state = {"col": None, "asc": True}

    os.environ.setdefault("EXPORT_DIR", str(DEFAULT_EXPORT_DIR))

    if input_path:
        valid_input = _validate_input_path(input_path)
        if valid_input:
            window["-INPUT-"].update(valid_input)
            window["-STATUS-"].update("Processando…")
            window.perform_long_operation(lambda: _safe_long_job(valid_input), "-DONE-")
        else:
            sg.popup_error(
                "O caminho informado via linha de comando não existe ou não é suportado.\n"
                "Use os botões Pasta/Arquivo e depois clique em Carregar."
            )

    def _update_totals(rows: List[Dict[str, Any]]):
        tots = _compute_totals(rows)
        window["-TVAL-"].update(tots["VALOR"])
        window["-TALIQ-"].update(tots["ALIQ"])
        window["-TINSS-"].update(tots["INSS"])
        window["-TIR-"].update(tots["IR"])
        window["-TPIS-"].update(tots["PIS"])
        window["-TCOF-"].update(tots["COFINS"])
        window["-TCSLL-"].update(tots["CSLL"])
        window["-TISSR-"].update(tots["ISS_RET"])
        window["-TISSN-"].update(tots["ISS_NORMAL"])

    def _render(rows: List[Dict[str, Any]]):
        table_vals = [[str(r.get(col, "")) for col in COLUMNS] for r in rows]
        row_colors = _make_row_colors(rows)
        window["-TABLE-"].update(values=table_vals, row_colors=row_colors)
        _autosize_table(window["-TABLE-"], table_vals, COLUMNS)
        _update_totals(rows)

    while True:
        event, values = window.read()
        if event in (sg.WINDOW_CLOSED, "-EXIT-", sg.WIN_CLOSE_ATTEMPTED_EVENT):
            break

        try:
            # -------- Evento de Log assíncrono ----------
            if event == "-LOGEVT-":
                rec = values.get(event, {})
                try:
                    window["-LOG-"].print(format_record(rec))
                except Exception:
                    pass
                continue

            # -------- Importação ----------
            if event in ("-LOAD-", "-LOAD-BAR-"):
                raw = values.get("-INPUT-", "")
                if not raw.strip():
                    sg.popup_error("Informe um caminho (Pasta/Arquivo) antes de carregar.")
                    continue
                valid = _validate_input_path(raw)
                if not valid:
                    sg.popup_error("Caminho inválido. Selecione uma PASTA ou um ARQUIVO .zip / .xml válido.")
                    continue

                window["-STATUS-"].update("Processando…")
                window["-TABLE-"].update(values=[])
                window["-TOT-"].update("0"); window["-OK-"].update("0"); window["-FAIL-"].update("0")
                window["-LOG-"].update("")
                window.perform_long_operation(lambda: _safe_long_job(valid), "-DONE-")

            if event == "-DONE-":
                kind, payload = values[event]
                if kind == "error":
                    msg = str(payload)
                    log_emit(SINK, "error", "processamento_falhou", detalhe=msg)
                    window["-STATUS-"].update("Falhou.")
                    sg.popup_error(f"Falha no processamento:\n{msg}")
                    continue

                rows, counts, errors = payload
                all_rows = rows
                view_rows = list(all_rows)
                sort_state.update(col=None, asc=True)

                _render(view_rows)
                window["-STATUS-"].update("Concluído.")
                window["-TOT-"].update(str(counts.get("total", 0)))
                window["-OK-"].update(str(counts.get("ok", 0)))
                window["-FAIL-"].update(str(counts.get("fail", 0)))

                log_emit(SINK, "info", "processamento_concluido", **counts)
                for err in errors[:50]:
                    log_emit(SINK, "error", "falha_parse", detalhe=err)
                if len(errors) > 50:
                    log_emit(SINK, "warn", "erros_suprimidos", restantes=len(errors) - 50)

            # -------- Filtro / Ordenação ----------
            if event == "-FILTER-":
                q = values.get("-FILTER-", "")
                view_rows = _apply_filter(all_rows, q)
                if sort_state["col"] is not None:
                    view_rows = _sort_rows(view_rows, sort_state["col"], sort_state["asc"])
                _render(view_rows)

            if event == "-CLEARF-":
                window["-FILTER-"].update("")
                view_rows = list(all_rows)
                if sort_state["col"] is not None:
                    view_rows = _sort_rows(view_rows, sort_state["col"], sort_state["asc"])
                _render(view_rows)

            if event == "-SORT-":
                col_idx = int(values[event])
                if sort_state["col"] == col_idx:
                    sort_state["asc"] = not sort_state["asc"]
                else:
                    sort_state.update(col=col_idx, asc=True)
                view_rows = _sort_rows(view_rows, col_idx, sort_state["asc"])
                _render(view_rows)

            # -------- Barra de ações ----------
            if event == "-LOGIN-":
                # Primeiro tenta o login integrado (Empresa/Usuário/Estação) via Oracle
                payload = show_login_baixa_integrada(window, default_usuario="RODOLFO")
                if payload:
                    G_EMPRESA_SEL = payload
                    try:
                        window.TKroot.title(f"Painel NFSe — Empresa: {payload.get('empresa_nome')}")
                    except Exception:
                        pass
                    try:
                        from infra.sybase import connect
                        test_cfg = G_SYBASE_CFG or to_env_dict(SETTINGS.sybase)
                        with connect(test_cfg) as con:
                            cur = con.cursor()
                            cur.execute("SELECT 1")
                            cur.fetchall()
                        sg.popup_no_wait("Conexão com o Domínio OK.", keep_on_top=True)
                    except Exception as e:
                        sg.popup_no_wait(f"Conexão com o Domínio falhou:\n{e}", keep_on_top=True)
                else:
                    # Fallback para o login manual original (mantido por compatibilidade)
                    cfg = _login_dialog()
                    if cfg:
                        G_SYBASE_CFG = cfg
                        sg.popup_ok("Credenciais aplicadas para esta sessão.")

            if event == "-EXP-HEAD-":
                if not all_rows:
                    sg.popup_error("Nada para enviar. Importe os XMLs primeiro.")
                    continue
                try:
                    qtd, ins = enviar_cabecalho_tomador_dominio(all_rows, sybase_cfg=G_SYBASE_CFG)
                    sg.popup_ok(
                        f"Enviado direto ao Domínio (BD): {qtd} tomador(es).\n"
                        f"Novos inseridos: {ins}.\n\n"
                        "Obs.: garanta a existência da tabela/procedimento no Domínio.\n"
                        "Ajuste em services/dominio_export.py se necessário."
                    )
                except Exception as e:
                    sg.popup_error(f"Falha ao enviar ao Domínio:\n{e}")

            if event == "-IMP-CLI-":
                if not all_rows:
                    sg.popup_error("Primeiro importe os XMLs para obter os TOMADORES.")
                    continue

                fonte = view_rows if view_rows else all_rows
                cnpjs = sorted({(r.get("TOMADOR") or "").strip() for r in fonte if (r.get("TOMADOR") or "").strip()})
                if not cnpjs:
                    sg.popup_error("Nenhum TOMADOR disponível para pesquisa.")
                    continue

                try:
                    encontrados, nao_encontrados = buscar_clientes_fornecedores(cnpjs, sybase_cfg=G_SYBASE_CFG)
                    _show_clientes_window(encontrados, nao_encontrados)
                except Exception as e:
                    sg.popup_error(
                        "Falha ao consultar o Domínio.\n"
                        "Dica: abra 'Login empresa', teste a conexão e aplique para esta sessão.\n\n"
                        f"Erro: {e}\n\n"
                        "Se as tabelas/colunas do seu Domínio forem diferentes, edite\n"
                        "services/dominio_import.py (constantes TBL_* e COL_*)."
                    )

            if event == "-IMP-NFS-":
                if not all_rows:
                    sg.popup_error("Primeiro importe os XMLs para obter os números de NFSe.")
                else:
                    fonte = view_rows if view_rows else all_rows
                    numeros = sorted({(r.get("NFE") or "").strip() for r in fonte if (r.get("NFE") or "").strip()})
                    if not numeros:
                        sg.popup_error("Nenhum número de NFSe disponível para pesquisa (verifique os dados carregados).")
                    else:
                        try:
                            dominio_rows = buscar_nfse_por_numeros(numeros, sybase_cfg=G_SYBASE_CFG)
                            if not dominio_rows:
                                sg.popup_ok("Nenhum registro retornado pelo Domínio para os números informados.")
                            else:
                                _show_nfse_dominio_window(dominio_rows, fonte)
                        except Exception as e:
                            sg.popup_error(
                                "Falha ao consultar NFS-e no Domínio.\n"
                                "Dica: abra 'Login empresa', teste a conexão e aplique para esta sessão.\n\n"
                                f"Erro: {e}\n\n"
                                "Se as tabelas/colunas do seu Domínio forem diferentes, edite\n"
                                "services/dominio_nfse.py (constantes TBL_NFSE/COL_*)."
                            )

            if event == "-GERA-PARC-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha visível. Use o filtro (ex.: por descrição) e tente novamente.")
                    continue

                venc = _parcelas_dialog()
                if not venc:
                    continue

                try:
                    a, p = aplicar_parcelas_e_acumuladores(view_rows, venc)
                except Exception as e:
                    sg.popup_error(f"Falha ao aplicar parcelas: {e}")
                    continue

                index_all = {(rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO")): rr for rr in all_rows}
                for rr in view_rows:
                    key = (rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO"))
                    if key in index_all:
                        index_all[key]["ACUMULADOR"] = rr.get("ACUMULADOR")
                        index_all[key]["PARCELAS"] = rr.get("PARCELAS")

                _render(view_rows)
                sg.popup_ok(
                    f"Parcelas geradas (1 por NF) em {p} linha(s) visível(is).\n"
                    f"Acumuladores ajustados em {a} linha(s).\n"
                    f"Vencimento aplicado: {venc}"
                )

            if event == "-EXP-FINAL-":
                if not all_rows:
                    sg.popup_error("Nada para exportar. Importe os XMLs primeiro.")
                    continue
                try:
                    out = export_final(all_rows, DEFAULT_EXPORT_DIR)
                    sg.popup_ok(f"Arquivo final gerado:\n{out}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar:\n{e}")

            if event == "-EXPORT-":
                if not all_rows:
                    sg.popup_error("Nada para exportar. Carregue os dados primeiro.")
                    continue
                out = sg.popup_get_file(
                    "Salvar CSV",
                    save_as=True,
                    default_extension=".csv",
                    file_types=(("CSV", "*.csv"),)
                )
                if out:
                    try:
                        export_csv(all_rows, Path(out), columns=COLUMNS)
                        sg.popup_ok(f"Exportado para: {out}")
                    except Exception as e:
                        sg.popup_error(f"Falha ao exportar: {e}")

        except Exception as e:
            from traceback import format_exc
            log_emit(SINK, "error", "excecao_na_ui", detalhe=str(e))
            sg.popup_error("Ocorreu um erro inesperado.\n\n" + str(e) + "\n\n" + format_exc())

    window.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
