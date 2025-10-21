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

# ---------------- Config e constantes ----------------

# Colunas (ordem exata solicitada)
COLUMNS: List[str] = [
    "TOMADOR", "NFE", "EMISSAO", "VALOR", "ALIQ",
    "INSS", "IR", "PIS", "COFINS", "CSLL",
    "ISS_RET", "ISS_NORMAL", "DISCRIMINACAO", "PARCELA", "ACUMULADOR"
]
_NUMERIC_COLS = {"VALOR", "ALIQ", "INSS", "IR", "PIS", "COFINS", "CSLL", "ISS_RET", "ISS_NORMAL"}

# Carrega .env (se houver) e configurações padrão
SETTINGS = load_settings()
DEFAULT_EXPORT_DIR = SETTINGS.export_dir

# Overrides de credenciais Sybase (definido pelo "Login empresa", por sessão)
G_SYBASE_CFG: Optional[Mapping[str, str]] = None


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

def _validate_input_path(p: str) -> Optional[str]:
    """Aceita pasta, .zip ou .xml."""
    p = _normalize_path(p)
    if not p:
        return None
    path = Path(p)
    if path.is_dir():
        return str(path)
    if path.is_file() and path.suffix.lower() in (".zip", ".xml"):
        return str(path)
    return None

def _digits_only(s: Optional[str]) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())

def _brl_to_decimal(s: Optional[str]) -> Decimal:
    if not s:
        return Decimal("0")
    t = str(s).strip()
    if t == "":
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
            # Sanitiza documento do tomador (só dígitos) para evitar divergências nas buscas/comparações
            r["TOMADOR"] = _digits_only(r.get("TOMADOR"))
            # Preenche PARCELA a partir do VENCIMENTO do parser (dd/mm/aaaa) ou das PARCELAS (dd-mm-aaaa)
            if r.get("VENCIMENTO"):
                r["PARCELA"] = r.get("VENCIMENTO")
            elif isinstance(r.get("PARCELAS"), list) and r["PARCELAS"]:
                venc0 = r["PARCELAS"][0].get("venc") or ""
                # converte dd-mm-aaaa -> dd/mm/aaaa
                if len(venc0) == 10 and venc0[2] == "-" and venc0[5] == "-":
                    r["PARCELA"] = f"{venc0[:2]}/{venc0[3:5]}/{venc0[6:]}"
                else:
                    r["PARCELA"] = venc0
            else:
                r.setdefault("PARCELA", "")
            # Garante campo ACUMULADOR presente (parser já define 410/424 por padrão)
            r.setdefault("ACUMULADOR", "")

            rows.append(r)
            counts["ok"] += 1
        except Exception as e:
            counts["fail"] += 1
            errors.append(f"{name}: {e}")

    return rows, counts, errors

def _filter_rows(rows: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    q = (query or "").strip().lower()
    if not q:
        return list(rows)
    out: List[Dict[str, Any]] = []
    for r in rows:
        # busca textual simples em alguns campos
        hay = " ".join([
            str(r.get("DISCRIMINACAO", "")),
            str(r.get("NFE", "")),
            str(r.get("TOMADOR", "")),
        ]).lower()
        if q in hay:
            out.append(r)
    return out

def _sort_rows(rows: List[Dict[str, Any]], col: Optional[str], ascending: bool) -> List[Dict[str, Any]]:
    if col is None:
        return rows
    # números: ordenar por Decimal
    if col in _NUMERIC_COLS:
        def keyfunc(r): return _brl_to_decimal(r.get(col))
    else:
        keyfunc = lambda r: str(r.get(col, "")).lower()
    return sorted(rows, key=keyfunc, reverse=not ascending)

def _make_row_colors(rows: List[Dict[str, Any]]):
    # pinta linhas com STATUS="Cancelada" em vermelho claro
    colors = []
    for idx, r in enumerate(rows):
        if str(r.get("STATUS", "")).lower() == "cancelada":
            colors.append((idx, "black", "#ffcccc"))
    return colors

def _autosize_table(table_elem: sg.Table, values: List[List[str]], headings: List[str]) -> None:
    """
    Ajusta a largura (px) de cada coluna com base no maior conteúdo + cabeçalho.
    Limites: 60..600 px; padding: +24 px. DISCRIMINACAO pode 'stretch'.
    """
    tv = table_elem.Widget  # ttk.Treeview
    try:
        import tkinter.font as tkfont
        f = tkfont.nametofont("TkDefaultFont")
        def width_px(text: str) -> int:
            return int(f.measure(text)) + 24
        # mede maior conteúdo de cada coluna
        maxw = []
        for ci, head in enumerate(headings):
            m = width_px(head)
            for row in values:
                if ci < len(row):
                    m = max(m, width_px(str(row[ci])))
            m = max(60, min(600, m))
            maxw.append(m)
        # aplica
        for ci, w in enumerate(maxw):
            tv.column(ci, width=w, stretch=(headings[ci] == "DISCRIMINACAO"))
    except Exception:
        pass

def _compute_totals(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    totals: Dict[str, Decimal] = {k: Decimal("0") for k in _NUMERIC_COLS}
    for r in rows:
        # canceladas já vêm zeradas pelos parsers/export; se não, somamos como estiver na grid
        for k in _NUMERIC_COLS:
            totals[k] += _brl_to_decimal(r.get(k))
    # formata para BRL
    return {k: _decimal_to_brl(v) for k, v in totals.items()}


# ---------------- Login (Domínio) ----------

def _login_dialog() -> Optional[Mapping[str, str]]:
    """Abre diálogo para informar credenciais do Domínio e testar conexão. Retorna dict se 'Aplicar'."""
    # Valores iniciais do .env
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
         sg.Button("Aplicar", key="-APPLY-"),
         sg.Button("Fechar")],
    ]

    w = sg.Window("Login empresa (Domínio)", layout, modal=True, finalize=True)
    while True:
        ev, vals = w.read()
        if ev in (sg.WINDOW_CLOSED, "Fechar"):
            break
        if ev == "-TEST-":
            from infra.sybase import connect, ping
            cfg = {
                "SYBASE_DRIVER": vals["-DRV-"],
                "SYBASE_HOST": vals["-HOST-"],
                "SYBASE_PORT": vals["-PORT-"],
                "SYBASE_DB": vals["-DB-"],
                "SYBASE_UID": vals["-UID-"],
                "SYBASE_PWD": vals["-PWD-"],
                "SYBASE_DSN": "",
            }
            try:
                with connect(cfg) as con:
                    ok = ping(con)
                if ok:
                    sg.popup_ok("Conexão OK!")
                else:
                    sg.popup_error("Falha no ping do Domínio.")
            except Exception as e:
                sg.popup_error(f"Erro ao conectar: {e}")
        if ev == "-APPLY-":
            G_SYBASE_CFG = {
                "SYBASE_DRIVER": vals["-DRV-"],
                "SYBASE_HOST": vals["-HOST-"],
                "SYBASE_PORT": vals["-PORT-"],
                "SYBASE_DB": vals["-DB-"],
                "SYBASE_UID": vals["-UID-"],
                "SYBASE_PWD": vals["-PWD-"],
                "SYBASE_DSN": "",
            }
            # salva na sessão
            globals()["G_SYBASE_CFG"] = G_SYBASE_CFG
            sg.popup_ok("Credenciais do Domínio aplicadas para esta sessão.")
            break
    w.close()
    return G_SYBASE_CFG


# ---------------- Janelas auxiliares (clientes / NFS-e no Domínio) ----------------
def _show_clientes_window(encontrados: List[Dict[str, Any]], nao_encontrados: List[Dict[str, Any]]):
    headings = ["TIPO", "DOC", "RAZAO", "FANTASIA", "IE", "MUNICIPIO", "UF"]
    values = [[r.get(h, "") for h in headings] for r in encontrados]

    tab1 = [
        [sg.Text(f"Encontrados: {len(encontrados)}")],
        [sg.Table(values=values,
                  headings=headings,
                  key="-CLI-TBL-",
                  auto_size_columns=True,
                  expand_x=True,
                  expand_y=True,
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
            out = sg.popup_get_file("Salvar CSV", save_as=True, default_extension=".csv", file_types=(("CSV","*.csv"),))
            if out:
                try:
                    import csv
                    with open(out, "w", newline="", encoding="utf-8") as f:
                        wr = csv.writer(f, delimiter=";")
                        wr.writerow(headings)
                        for r in encontrados:
                            wr.writerow([r.get(h,"") for h in headings])
                    sg.popup_ok(f"Exportado para: {out}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar: {e}")
        if ev == "-COPY-NF-":
            try:
                w["-NFOUND-"].Widget.event_generate("<<SelectAll>>")
                txt = w["-NFOUND-"].get()
                sg.clipboard_set(txt)
                sg.popup_ok("Lista copiada para a área de transferência.")
            except Exception:
                pass
    w.close()

def _show_nfse_dominio_window(enriched: List[Dict[str, Any]], base_rows: List[Dict[str, Any]]):
    # Mostra consulta cruzada de NFSe puxada do Domínio
    headings = ["NFE", "DATA", "SITUACAO", "VALOR", "TOMADOR_DOC", "TOMADOR_NOME"]
    values = []
    for r in enriched:
        values.append([r.get(h, "") for h in headings])

    layout = [
        [sg.Text(f"Registros do Domínio: {len(enriched)}")],
        [sg.Table(values=values,
                  headings=headings,
                  key="-NFSE-TBL-",
                  auto_size_columns=True,
                  expand_x=True,
                  expand_y=True,
                  display_row_numbers=False,
                  justification="left")],
        [sg.Push(), sg.Button("Exportar CSV", key="-EXP-NFSE-CSV-")],
        [sg.Push(), sg.Button("Fechar")]
    ]

    w = sg.Window("NFS-e no Domínio", layout, modal=True, finalize=True, size=(900, 500))
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


# ---------------- Janela principal ----------------

def main(input_path: str | None = None, theme: str | None = None) -> int:
    # Tema/escala
    theme = theme or os.getenv("APP_THEME") or "SystemDefault"
    sg.theme(theme)
    sw, sh = sg.Window.get_screen_size()
    scale = _compute_scaling(sw, sh)
    sg.set_options(dpi_awareness=True, scaling=scale, font=("Segoe UI", 10))
    win_size = (int(sw * 0.9), int(sh * 0.9))

    # Barra de ações
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
        sg.Text("Filtro (qualquer conteúdo: TOMADOR, NFE, descrição…):", size=(40, 1)),
        sg.Input(key="-FILTER-", expand_x=True, enable_events=True),
        sg.Text("Ordenar por:"),
        sg.Combo(COLUMNS, default_value="EMISSAO", key="-SORT-COL-", readonly=True, size=(18, 1)),
        sg.Combo(["Asc", "Desc"], default_value="Asc", key="-SORT-DIR-", readonly=True, size=(6, 1)),
        sg.Button("Aplicar", key="-APPLY-SORT-"),
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

    # ---- NOVO: barra de totais dinâmicos (linhas visíveis) ----
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
        bar_actions,
        row_input,
        row_filter,
        [table],
        [sg.Column([row_status], expand_x=True), frame_tot],
        [frame_logs],
        [sg.Push(), sg.Button("Sair", key="-EXIT-")],
    ]

    window = sg.Window("NFSe Painel", layout, size=win_size, resizable=True, finalize=True)

    # Estado
    all_rows: List[Dict[str, Any]] = []
    view_rows: List[Dict[str, Any]] = []
    sort_state = {"col": None, "asc": True}

    # Funções internas para a janela
    def _render(rows: List[Dict[str, Any]]):
        table_vals = [[str(r.get(col, "")) for col in COLUMNS] for r in rows]
        row_colors = _make_row_colors(rows)
        window["-TABLE-"].update(values=table_vals, row_colors=row_colors)
        _autosize_table(window["-TABLE-"], table_vals, COLUMNS)
        _update_totals(rows)

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

    while True:
        event, values = window.read()
        if event in (sg.WINDOW_CLOSED, "-EXIT-", sg.WIN_CLOSE_ATTEMPTED_EVENT):
            break

        try:
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
                    from utils.logs import log_emit
                    log_emit(window["-LOG-"], "error", "processamento_falhou", detalhe=msg)
                    window["-STATUS-"].update("Falhou.")
                    sg.popup_error(f"Falha no processamento:\n{msg}")
                    continue

                rows, counts, errors = payload
                all_rows = rows
                view_rows = list(all_rows)
                sort_state.update(col=None, asc=True)

                # Status
                window["-TOT-"].update(str(counts["total"]))
                window["-OK-"].update(str(counts["ok"]))
                window["-FAIL-"].update(str(counts["fail"]))
                if errors:
                    from utils.logs import log_emit
                    for e in errors:
                        log_emit(window["-LOG-"], "warn", "xml_erro", detalhe=e)
                window["-STATUS-"].update("Concluído.")

                _render(view_rows)

            # -------- Filtro/Ordenação ----------
            if event == "-FILTER-":
                q = values.get("-FILTER-", "")
                view_rows = _filter_rows(all_rows, q)
                _render(_sort_rows(view_rows, sort_state["col"], sort_state["asc"]))
            if event == "-APPLY-SORT-":
                col = values.get("-SORT-COL-")
                asc = (values.get("-SORT-DIR-") == "Asc")
                sort_state.update(col=col, asc=asc)
                view_rows = _sort_rows(view_rows, col, asc)
                _render(view_rows)

            # -------- Login empresa ----------
            if event == "-LOGIN-":
                cfg = _login_dialog()
                if cfg:
                    from utils.logs import log_emit
                    log_emit(window["-LOG-"], "info", "login_empresa_aplicado", **cfg)

            # -------- Exportar Cabeçalho + Tomador ----------
            if event == "-EXP-HEAD-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha para exportar.")
                    continue
                try:
                    enviados, erros = enviar_cabecalho_tomador_dominio(view_rows, sybase_cfg=G_SYBASE_CFG)
                    sg.popup_ok(f"Enviados: {enviados}\nFalhas: {erros}")
                except Exception as e:
                    sg.popup_error(
                        "Falha ao enviar Cabeçalho + Tomador.\n"
                        "Dica: abra 'Login empresa', teste a conexão e aplique para esta sessão.\n\n"
                        f"Erro: {e}"
                    )

            # -------- Importar Clientes ----------
            if event == "-IMP-CLI-":
                try:
                    encontrados, nao_encontrados = buscar_clientes_fornecedores(view_rows, sybase_cfg=G_SYBASE_CFG)
                    _show_clientes_window(encontrados, nao_encontrados)
                except Exception as e:
                    sg.popup_error(
                        "Falha ao consultar o Domínio.\n"
                        "Dica: abra 'Login empresa', teste a conexão e aplique para esta sessão.\n\n"
                        f"Erro: {e}\n\n"
                        "Se as tabelas/colunas do seu Domínio forem diferentes, edite\n"
                        "services/dominio_import.py (constantes TBL_* e COL_*)."
                    )

            # -------- Importar NFSe (Domínio) ----------
            if event == "-IMP-NFS-":
                if not all_rows:
                    sg.popup_error("Primeiro importe os XMLs para obter os números de NFSe.")
                    continue

                fonte = view_rows if view_rows else all_rows
                numeros = sorted({(r.get("NFE") or "").strip() for r in fonte if (r.get("NFE") or "").strip()})
                if not numeros:
                    sg.popup_error("Nenhum número de NFSe disponível para pesquisa (verifique os dados carregados).")
                    continue

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
                        "(constantes TBL_NFSE/COL_*)."
                    )

            # -------- Gerar Parcelas (manual) ----------
            if event == "-GERA-PARC-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha visível. Use o filtro (ex.: por descrição) e tente novamente.")
                    continue

                venc = _parcelas_dialog()
                if not venc:
                    continue  # cancelado

                try:
                    a, p = aplicar_parcelas_e_acumuladores(view_rows, venc)
                except Exception as e:
                    sg.popup_error(f"Falha ao aplicar parcelas: {e}")
                    continue

                # Reflete no conjunto completo (all_rows) por chave NFE+TOMADOR+EMISSAO
                index_all = {(rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO")): rr for rr in all_rows}
                for rr in view_rows:
                    key = (rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO"))
                    if key in index_all:
                        index_all[key]["ACUMULADOR"] = rr.get("ACUMULADOR")
                        index_all[key]["PARCELAS"] = rr.get("PARCELAS")

                # Atualiza a coluna visível "PARCELA" com a 1ª parcela (dd-mm-aaaa -> dd/mm/aaaa)
                for rr in view_rows:
                    parc = rr.get("PARCELAS")
                    if isinstance(parc, list) and parc:
                        venc0 = parc[0].get("venc") or ""
                        if len(venc0) == 10 and venc0[2] == "-" and venc0[5] == "-":
                            rr["PARCELA"] = f"{venc0[:2]}/{venc0[3:5]}/{venc0[6:]}"
                        else:
                            rr["PARCELA"] = venc0
                    else:
                        # mantém valor existente
                        rr.setdefault("PARCELA", rr.get("PARCELA", ""))
                for key, rr_all in index_all.items():
                    parc = rr_all.get("PARCELAS")
                    if isinstance(parc, list) and parc:
                        venc0 = parc[0].get("venc") or ""
                        if len(venc0) == 10 and venc0[2] == "-" and venc0[5] == "-":
                            rr_all["PARCELA"] = f"{venc0[:2]}/{venc0[3:5]}/{venc0[6:]}"
                        else:
                            rr_all["PARCELA"] = venc0
                    else:
                        rr_all.setdefault("PARCELA", rr_all.get("PARCELA", ""))

                _render(view_rows)
                sg.popup_ok(
                    f"Parcelas geradas (1 por NF) em {p} linha(s) visível(is).\n"
                    f"Acumuladores ajustados em {a} linha(s).\n"
                    f"Vencimento aplicado: {venc}"
                )

            # -------- Exportar Final ----------
            if event == "-EXP-FINAL-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha para exportar.")
                    continue
                try:
                    out_path = sg.popup_get_file(
                        "Salvar arquivo TXT",
                        save_as=True,
                        default_path=str(Path(DEFAULT_EXPORT_DIR) / "export_final.txt"),
                        default_extension=".txt",
                        file_types=(("Texto", "*.txt"), ("Todos", "*.*")),
                    )
                    if not out_path:
                        continue
                    enviados, falhas = export_final(view_rows, out_path, sybase_cfg=G_SYBASE_CFG)
                    sg.popup_ok(f"Exportado para {out_path}\nLinhas OK: {enviados}\nFalhas: {falhas}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar: {e}")

        except Exception as e:
            from traceback import format_exc
            from utils.logs import log_emit
            log_emit(window["-LOG-"], "error", "excecao_na_ui", detalhe=str(e))
            sg.popup_error("Ocorreu um erro inesperado.\n\n" + str(e) + "\n\n" + format_exc())

    window.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
