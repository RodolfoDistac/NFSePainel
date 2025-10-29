# panel.py
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Mapping
from decimal import Decimal, InvalidOperation
from datetime import date

import PySimpleGUI as sg

from config.settings import load_settings, to_env_dict
from services.dominio_export import export_final, enviar_cabecalho_tomador_dominio
from services.dominio_import import buscar_clientes_fornecedores
from services.dominio_nfse import buscar_nfse_por_numeros

# ======================= Colunas / Config =======================

COLUMNS: List[str] = [
    "TOMADOR", "NFE", "EMISSAO", "VALOR", "ALIQ",
    "INSS", "IR", "PIS", "COFINS", "CSLL",
    "ISS_RET", "ISS_NORMAL", "DISCRIMINACAO", "PARCELA", "ACUMULADOR",
]
_NUMERIC_COLS = {"VALOR", "ALIQ", "INSS", "IR", "PIS", "COFINS", "CSLL", "ISS_RET", "ISS_NORMAL"}

SETTINGS = load_settings()
DEFAULT_EXPORT_DIR = SETTINGS.export_dir

# sobrescreve credenciais do Domínio (sessão)
G_SYBASE_CFG: Optional[Mapping[str, str]] = None


# ======================= Utils =======================

def _compute_scaling(sw: int, sh: int) -> float:
    if sh >= 2160: return 1.6
    if sh >= 1440: return 1.3
    if sh >= 1080: return 1.1
    return 1.0

def _normalize_path(p: str) -> str:
    return (p or "").strip().strip('"').strip("'")

def _validate_input_path(p: str) -> Optional[str]:
    p = _normalize_path(p)
    if not p: return None
    path = Path(p)
    if path.is_dir(): return str(path)
    if path.is_file() and path.suffix.lower() in (".zip", ".xml"): return str(path)
    return None

def _digits_only(s: Optional[str]) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _brl_to_decimal(s: Optional[str]) -> Decimal:
    if not s: return Decimal("0")
    t = str(s).strip()
    if t == "": return Decimal("0")
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
    inteiro = f"{int(inteiro):,}".replace(",", ".")
    return f"{inteiro},{frac}"

# -------- datas (tolerantes) --------

def _mask_date_typing(raw: str) -> str:
    d = _digits_only(raw)[:8]
    if len(d) <= 2:  return d
    if len(d) <= 4:  return f"{d[:2]}/{d[2:]}"
    return f"{d[:2]}/{d[2:4]}/{d[4:]}"

def _parse_dd_mm_aaaa_tolerant(s: str) -> Optional[date]:
    """Aceita 30/09/2025, 30-09-2025, 30092025."""
    from datetime import datetime
    if not s: return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    d = _digits_only(s)
    if len(d) == 8:
        dd, mm, yyyy = int(d[:2]), int(d[2:4]), int(d[4:])
        try:
            return date(yyyy, mm, dd)
        except Exception:
            return None
    return None

def _format_dd_mm_aaaa(d: date) -> str:
    return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"

def _emissao_to_date(emissao: str | None) -> Optional[date]:
    from datetime import datetime
    if not emissao: return None
    s = emissao.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s[:19]).date()
    except Exception:
        return None

def _last_day_of_month(d: date) -> date:
    import calendar
    last = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last)

# -------- tabela e cálculo --------

def _filter_by_descr(rows: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    q = (query or "").lower().strip()
    if not q: return list(rows)
    return [r for r in rows if q in str(r.get("DISCRIMINACAO", "")).lower()]

def _sort_rows(rows: List[Dict[str, Any]], col: Optional[str], asc: bool) -> List[Dict[str, Any]]:
    if not col: return rows
    key = (lambda r: _brl_to_decimal(r.get(col))) if col in _NUMERIC_COLS else (lambda r: str(r.get(col, "")).lower())
    return sorted(rows, key=key, reverse=not asc)

def _row_colors(rows: List[Dict[str, Any]]):
    out = []
    for i, r in enumerate(rows):
        if str(r.get("STATUS", "")).lower() == "cancelada":
            out.append((i, "black", "#ffcccc"))
    return out

def _autosize_table(table_elem: sg.Table, values: List[List[str]], headings: List[str]) -> None:
    tv = table_elem.Widget
    try:
        import tkinter.font as tkfont
        f = tkfont.nametofont("TkDefaultFont")
        def w(text: str) -> int: return int(f.measure(text)) + 24
        maxw = []
        for ci, head in enumerate(headings):
            m = w(head)
            for row in values:
                if ci < len(row): m = max(m, w(str(row[ci])))
            m = max(60, min(600, m))
            maxw.append(m)
        for ci, width in enumerate(maxw):
            tv.column(ci, width=width, stretch=(headings[ci] == "DISCRIMINACAO"))
    except Exception:
        pass

def _totals(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    acc: Dict[str, Decimal] = {k: Decimal("0") for k in _NUMERIC_COLS}
    for r in rows:
        for k in _NUMERIC_COLS:
            acc[k] += _brl_to_decimal(r.get(k))
    return {k: _decimal_to_brl(v) for k, v in acc.items()}

# ======================= Import XMLs =======================

def _parse_all(input_str: str) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    """Lê ZIP/pasta/XMLs e retorna linhas já prontas para a grade."""
    from dataio.loaders import iter_xml_bytes
    from parsers.nfse_abrasf import NFSeParser

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
            r.setdefault("PARCELA", "")
            r.setdefault("ACUMULADOR", "")
            rows.append(r)
            counts["ok"] += 1
        except Exception as e:
            counts["fail"] += 1
            errors.append(f"{name}: {e}")

    return rows, counts, errors

def _safe_long_job(p: str):
    try:
        return ("ok", _parse_all(p))
    except Exception as e:
        return ("error", f"{type(e).__name__}: {e}")

# ======================= Login Domínio =======================

def _login_dialog() -> Optional[Mapping[str, str]]:
    base = to_env_dict(SETTINGS.sybase)
    layout = [
        [sg.Text("Conexão Domínio (SQL Anywhere / Sybase)", font=("Segoe UI", 11, "bold"))],
        [sg.Text("Driver"), sg.Input(base["SYBASE_DRIVER"], key="-DRV-", size=(30, 1))],
        [sg.Text("Host"), sg.Input(base["SYBASE_HOST"], key="-HOST-", size=(20, 1)),
         sg.Text("Porta"), sg.Input(base["SYBASE_PORT"], key="-PORT-", size=(7, 1))],
        [sg.Text("Banco"), sg.Input(base["SYBASE_DB"], key="-DB-", size=(20, 1))],
        [sg.Text("Usuário"), sg.Input(base["SYBASE_UID"], key="-UID-", size=(20, 1)),
         sg.Text("Senha"), sg.Input(base["SYBASE_PWD"], key="-PWD-", size=(20, 1), password_char="•")],
        [sg.Push(), sg.Button("Testar conexão", key="-TEST-"), sg.Button("Aplicar", key="-APPLY-"), sg.Button("Fechar")],
    ]
    w = sg.Window("Login empresa (Domínio)", layout, modal=True, finalize=True)
    while True:
        ev, v = w.read()
        if ev in (sg.WINDOW_CLOSED, "Fechar"):
            break
        if ev == "-TEST-":
            from infra.sybase import connect, ping
            cfg = {
                "SYBASE_DRIVER": v["-DRV-"], "SYBASE_HOST": v["-HOST-"], "SYBASE_PORT": v["-PORT-"],
                "SYBASE_DB": v["-DB-"], "SYBASE_UID": v["-UID-"], "SYBASE_PWD": v["-PWD-"], "SYBASE_DSN": "",
            }
            try:
                with connect(cfg) as con:
                    ok = ping(con)
                sg.popup_ok("Conexão OK!" if ok else "Falha no ping do Domínio.")
            except Exception as e:
                sg.popup_error(f"Erro ao conectar: {e}")
        if ev == "-APPLY-":
            cfg = {
                "SYBASE_DRIVER": v["-DRV-"], "SYBASE_HOST": v["-HOST-"], "SYBASE_PORT": v["-PORT-"],
                "SYBASE_DB": v["-DB-"], "SYBASE_UID": v["-UID-"], "SYBASE_PWD": v["-PWD-"], "SYBASE_DSN": "",
            }
            globals()["G_SYBASE_CFG"] = cfg
            sg.popup_ok("Credenciais aplicadas nesta sessão.")
            break
    w.close()
    return G_SYBASE_CFG

# ======================= UI Principal =======================

def main(input_path: str | None = None, theme: str | None = None) -> int:
    theme = theme or os.getenv("APP_THEME") or "SystemDefault"
    sg.theme(theme)
    sw, sh = sg.Window.get_screen_size()
    sg.set_options(dpi_awareness=True, scaling=_compute_scaling(sw, sh), font=("Segoe UI", 10))
    win_size = (int(sw * 0.9), int(sh * 0.9))

    bar = [[
        sg.Button("Login empresa", key="-LOGIN-"),
        sg.Button("Importar XMLs", key="-LOAD-BAR-"),
        sg.Button("Exportar Cabeçalho + Tomador", key="-EXP-HEAD-"),
        sg.Button("Importar Clientes", key="-IMP-CLI-"),
        sg.Button("Importar NFS Domínio", key="-IMP-NFS-"),
        sg.Button("Gerar Parcelas (manual)", key="-GERA-PARC-"),
        sg.Button("Exportar Final", key="-EXP-FINAL-"),
    ]]
    row_in = [[
        sg.Text("Entrada (pasta, .zip ou .xml):", size=(25, 1)),
        sg.Input(key="-INPUT-", expand_x=True),
        sg.FolderBrowse("Pasta", target="-INPUT-"),
        sg.FileBrowse("Arquivo", file_types=(("ZIP/XML", "*.zip;*.xml"), ("Todos", "*.*")), target="-INPUT-"),
        sg.Button("Carregar", key="-LOAD-", bind_return_key=True),
    ]]
    row_filter = [[
        sg.Text("Filtro (somente na descrição):", size=(25, 1)),
        sg.Input(key="-FILTER-", expand_x=True, enable_events=True),
        sg.Text("Clique no cabeçalho para ordenar", text_color="gray"),
    ]]

    table = sg.Table(
        values=[], headings=COLUMNS, key="-TABLE-",
        auto_size_columns=False, justification="left", display_row_numbers=False,
        enable_events=True, enable_click_events=True,
        select_mode=sg.TABLE_SELECT_MODE_EXTENDED,
        num_rows=20, expand_x=True, expand_y=True, vertical_scroll_only=False,
    )

    editor = sg.Frame(
        "Editor (PARCELA / ACUMULADOR)",
        [[
            sg.Text("Linhas selecionadas:"), sg.Text("0", key="-SEL-COUNT-", size=(6, 1)),
            sg.Text(" | Linha focal:"), sg.Text("", key="-EDT-INFO-", size=(60, 1)),
        ],[
            sg.Text("PARCELA (dd/mm/aaaa)", size=(22, 1)),
            sg.Input(key="-EDT-PARC-", size=(14, 1), enable_events=True),
            sg.Text("ACUMULADOR (opcional)"),
            sg.Input(key="-EDT-ACUM-", size=(8, 1)),
            sg.Button("Aplicar alteração", key="-EDT-APPLY-"),
        ]]],
        expand_x=True
    )

    row_stat = [
        sg.Text("Status:"), sg.Text("Aguardando…", key="-STATUS-", expand_x=True),
        sg.Text("Total:"), sg.Text("0", key="-TOT-"),
        sg.Text("  Sucesso:"), sg.Text("0", key="-OK-"),
        sg.Text("  Falha:"), sg.Text("0", key="-FAIL-"),
    ]

    totals_labels = [
        ("VALOR", "-TVAL-"), ("ALIQ", "-TALIQ-"), ("INSS", "-TINSS-"), ("IR", "-TIR-"),
        ("PIS", "-TPIS-"), ("COFINS", "-TCOF-"), ("CSLL", "-TCSLL-"), ("ISS_RET", "-TISSR-"), ("ISS_NORMAL", "-TISSN-")
    ]
    tr = []
    for lbl, k in totals_labels:
        tr += [sg.Text(f"{lbl}:"), sg.Text("0,00", key=k, size=(12, 1))]
    frame_tot = sg.Frame("Totais (linhas visíveis)", [[sg.Column([tr])]], expand_x=True)

    frame_logs = sg.Frame("Logs", [[sg.Multiline(key="-LOG-", size=(10, 8), expand_x=True, expand_y=True, autoscroll=True, write_only=True)]], expand_x=True, expand_y=True)

    layout = [bar, row_in, row_filter, [table], [editor], [sg.Column([row_stat], expand_x=True), frame_tot], [frame_logs], [sg.Push(), sg.Button("Sair", key="-EXIT-")]]
    window = sg.Window("NFSe Painel", layout, size=win_size, resizable=True, finalize=True)

    # header sort (Treeview)
    tv = window["-TABLE-"].Widget
    def _install_header_sort():
        def mk(col_name: str):
            def _h():
                nonlocal view_rows, sort_state
                if sort_state["col"] == col_name:
                    sort_state["asc"] = not sort_state["asc"]
                else:
                    sort_state["col"] = col_name
                    sort_state["asc"] = True
                view_rows = _sort_rows(view_rows, sort_state["col"], sort_state["asc"])
                _render(view_rows)
            return _h
        for i, col in enumerate(COLUMNS, start=1):
            try:
                tv.heading(f"#{i}", text=col, command=mk(col))
            except Exception:
                pass
    _install_header_sort()

    # Espaço alterna seleção do foco
    try: tv.bind("<space>", " SPACE")
    except Exception: pass

    # Estado
    all_rows: List[Dict[str, Any]] = []
    view_rows: List[Dict[str, Any]] = []
    sort_state = {"col": None, "asc": True}
    selected_idx: Optional[int] = None

    def _render(rows: List[Dict[str, Any]]):
        vals = [[str(r.get(c, "")) for c in COLUMNS] for r in rows]
        window["-TABLE-"].update(values=vals, row_colors=_row_colors(rows))
        _autosize_table(window["-TABLE-"], vals, COLUMNS)
        _install_header_sort()
        _update_totals(rows)

    def _update_totals(rows: List[Dict[str, Any]]):
        t = _totals(rows)
        window["-TVAL-"].update(t["VALOR"]); window["-TALIQ-"].update(t["ALIQ"]); window["-TINSS-"].update(t["INSS"])
        window["-TIR-"].update(t["IR"]); window["-TPIS-"].update(t["PIS"]); window["-TCOF-"].update(t["COFINS"])
        window["-TCSLL-"].update(t["CSLL"]); window["-TISSR-"].update(t["ISS_RET"]); window["-TISSN-"].update(t["ISS_NORMAL"])

    def _auto_acc_from_parcela(vr: Dict[str, Any], parcela_val: str) -> str:
        iss_ret = _brl_to_decimal(vr.get("ISS_RET"))
        base = "424" if iss_ret > 0 else "410"
        return ("425" if base == "424" else "411") if parcela_val else base

    def _load_editor_from_selection():
        nonlocal selected_idx
        sel = values.get("-TABLE-", [])
        window["-SEL-COUNT-"].update(str(len(sel)))
        if not sel:
            selected_idx = None
            window["-EDT-INFO-"].update(""); window["-EDT-PARC-"].update(""); window["-EDT-ACUM-"].update("")
            return
        selected_idx = sel[0]
        if selected_idx < 0 or selected_idx >= len(view_rows): return
        r = view_rows[selected_idx]
        window["-EDT-INFO-"].update(f"TOMADOR={r.get('TOMADOR','')} | NFE={r.get('NFE','')} | EMISSAO={r.get('EMISSAO','')}")
        # sugestão automática SOMENTE no editor (não grava até aplicar)
        if (r.get("PARCELA") or "") == "":
            em = _emissao_to_date(r.get("EMISSAO"))
            window["-EDT-PARC-"].update(_format_dd_mm_aaaa(_last_day_of_month(em)) if em else "")
        else:
            window["-EDT-PARC-"].update(r.get("PARCELA",""))
        window["-EDT-ACUM-"].update(r.get("ACUMULADOR",""))

    def _apply_editor_to_selection():
        sel = values.get("-TABLE-", []) or []
        if not sel:
            sg.popup_error("Selecione 1+ linhas antes de aplicar.")
            return
        parc_raw = (values.get("-EDT-PARC-") or "").strip()
        acum_force = _digits_only(values.get("-EDT-ACUM-") or "")
        # valida data (tolerante)
        if parc_raw:
            masked = _mask_date_typing(parc_raw)
            d = _parse_dd_mm_aaaa_tolerant(masked)
            if not d:
                sg.popup_error("PARCELA inválida. Use dd/mm/aaaa (ex.: 30/09/2025).")
                return
            parc_fmt = _format_dd_mm_aaaa(d)
        else:
            parc_fmt = ""

        for idx in sel:
            if 0 <= idx < len(view_rows):
                vr = view_rows[idx]
                vr["PARCELA"] = parc_fmt
                vr["ACUMULADOR"] = acum_force if acum_force else _auto_acc_from_parcela(vr, parc_fmt)
                # reflete em all_rows
                key = (vr.get("NFE"), vr.get("TOMADOR"), vr.get("EMISSAO"))
                for rr in all_rows:
                    if (rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO")) == key:
                        rr["PARCELA"] = vr["PARCELA"]
                        rr["ACUMULADOR"] = vr["ACUMULADOR"]
                        break

        _render(view_rows)
        sg.popup_ok("Alterações aplicadas nas linhas selecionadas.")

    # ======================= Loop =======================
    while True:
        event, values = window.read()
        if event in (sg.WINDOW_CLOSED, "-EXIT-", sg.WIN_CLOSE_ATTEMPTED_EVENT):
            break
        try:
            if event in ("-LOAD-", "-LOAD-BAR-"):
                raw = values.get("-INPUT-", "")
                valid = _validate_input_path(raw)
                if not valid:
                    sg.popup_error("Selecione pasta, .zip ou .xml válido.")
                    continue
                window["-STATUS-"].update("Processando…")
                window["-TABLE-"].update(values=[]); window["-LOG-"].update("")
                window["-TOT-"].update("0"); window["-OK-"].update("0"); window["-FAIL-"].update("0")
                window["-EDT-INFO-"].update(""); window["-EDT-PARC-"].update(""); window["-EDT-ACUM-"].update("")
                window.perform_long_operation(lambda: _safe_long_job(valid), "-DONE-")

            if event == "-DONE-":
                kind, payload = values[event]
                if kind == "error":
                    from utils.logs import log_emit
                    log_emit(window["-LOG-"], "error", "processamento_falhou", detalhe=str(payload))
                    window["-STATUS-"].update("Falhou.")
                    sg.popup_error(f"Falha no processamento:\n{payload}")
                    continue

                rows, counts, errors = payload
                all_rows = rows
                view_rows = list(all_rows)
                sort_state.update(col=None, asc=True)
                selected_idx = None

                window["-TOT-"].update(str(counts["total"]))
                window["-OK-"].update(str(counts["ok"]))
                window["-FAIL-"].update(str(counts["fail"]))
                if errors:
                    from utils.logs import log_emit
                    for e in errors: log_emit(window["-LOG-"], "warn", "xml_erro", detalhe=e)
                window["-STATUS-"].update("Concluído.")
                _render(view_rows)

            if event == "-FILTER-":
                view_rows = _sort_rows(_filter_by_descr(all_rows, values.get("-FILTER-", "")), sort_state["col"], sort_state["asc"])
                _render(view_rows)

            if event == "-TABLE- SPACE":
                try:
                    iid = tv.focus()
                    if iid:
                        sel = set(tv.selection())
                        (tv.selection_remove if iid in sel else tv.selection_add)(iid)
                        window["-TABLE-"].update(select_rows=[tv.index(x) for x in tv.selection()])
                        _load_editor_from_selection()
                except Exception:
                    pass

            if event == "-TABLE-":
                _load_editor_from_selection()

            if event == "-EDT-PARC-":
                raw = values.get("-EDT-PARC-", "")
                masked = _mask_date_typing(raw)
                if masked != raw:
                    window["-EDT-PARC-"].update(masked)

            if event == "-EDT-APPLY-":
                _apply_editor_to_selection()

            if event == "-LOGIN-":
                cfg = _login_dialog()
                if cfg:
                    from utils.logs import log_emit
                    log_emit(window["-LOG-"], "info", "login_empresa_aplicado", **cfg)

            if event == "-EXP-HEAD-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha para exportar.")
                    continue
                try:
                    enviados, erros = enviar_cabecalho_tomador_dominio(view_rows, sybase_cfg=G_SYBASE_CFG)
                    sg.popup_ok(f"Enviados: {enviados}\nFalhas: {erros}")
                except Exception as e:
                    sg.popup_error("Falha ao enviar Cabeçalho + Tomador.\nAbra 'Login empresa' e teste a conexão.\n\n" + str(e))

            if event == "-IMP-CLI-":
                try:
                    encontrados, nao = buscar_clientes_fornecedores(view_rows, sybase_cfg=G_SYBASE_CFG)
                    _show_clientes_window(encontrados, nao)
                except Exception as e:
                    sg.popup_error("Falha ao consultar o Domínio.\nAbra 'Login empresa' e teste a conexão.\n\n" + str(e))

            if event == "-IMP-NFS-":
                fonte = view_rows if view_rows else all_rows
                numeros = sorted({(r.get("NFE") or "").strip() for r in fonte if (r.get("NFE") or "").strip()})
                if not numeros:
                    sg.popup_error("Nenhum número de NFSe disponível para pesquisa.")
                    continue
                try:
                    dominio_rows = buscar_nfse_por_numeros(numeros, sybase_cfg=G_SYBASE_CFG)
                    if not dominio_rows: sg.popup_ok("Nenhum registro retornado pelo Domínio.")
                    else: _show_nfse_dominio_window(dominio_rows, fonte)
                except Exception as e:
                    sg.popup_error("Falha ao consultar NFS-e no Domínio.\nAbra 'Login empresa' e teste a conexão.\n\n" + str(e))

            if event == "-GERA-PARC-":
                from services.parcelas import aplicar_parcelas_e_acumuladores
                if not view_rows:
                    sg.popup_error("Nenhuma linha visível.")
                    continue
                sel_idx = values.get("-TABLE-", []) or []
                subset = [view_rows[i] for i in sel_idx if 0 <= i < len(view_rows)] if sel_idx else view_rows
                # dialog simples
                dlg = sg.Window("Gerar Parcela", [[sg.Text("Vencimento (dd/mm/aaaa):"), sg.Input(key="-V-", enable_events=True, size=(12,1))],[sg.Push(), sg.Button("Aplicar","-OK-"), sg.Button("Cancelar")]], modal=True, finalize=True)
                venc = None
                while True:
                    ev2, v2 = dlg.read()
                    if ev2 in (sg.WINDOW_CLOSED, "Cancelar"): break
                    if ev2 == "-V-": dlg["-V-"].update(_mask_date_typing(v2["-V-"]))
                    if ev2 == "-OK-":
                        d = _parse_dd_mm_aaaa_tolerant(v2["-V-"])
                        if not d: sg.popup_error("Data inválida. Use dd/mm/aaaa."); continue
                        venc = _format_dd_mm_aaaa(d); break
                dlg.close()
                if not venc: continue
                try:
                    a, p = aplicar_parcelas_e_acumuladores(subset, venc)
                except Exception as e:
                    sg.popup_error(f"Falha ao aplicar parcelas: {e}")
                    continue
                # reflete e atualiza primeira parcela em PARCELA
                idx_all = {(rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO")): rr for rr in all_rows}
                for rr in subset:
                    rr["PARCELA"] = venc
                    rr["ACUMULADOR"] = rr.get("ACUMULADOR")
                    key = (rr.get("NFE"), rr.get("TOMADOR"), rr.get("EMISSAO"))
                    if key in idx_all:
                        idx_all[key]["PARCELA"] = rr["PARCELA"]
                        idx_all[key]["ACUMULADOR"] = rr["ACUMULADOR"]
                _render(view_rows)
                sg.popup_ok(f"Parcelas geradas em {p} registro(s).\nAcumuladores ajustados em {a} registro(s).\nVencimento: {venc}")

            if event == "-EXP-FINAL-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha para exportar.")
                    continue
                try:
                    out = sg.popup_get_file("Salvar TXT", save_as=True, default_path=str(Path(DEFAULT_EXPORT_DIR)/"export_final.txt"), default_extension=".txt", file_types=(("Texto","*.txt"),("Todos","*.*")))
                    if not out: continue
                    enviados, falhas = export_final(view_rows, out, sybase_cfg=G_SYBASE_CFG)
                    sg.popup_ok(f"Exportado para {out}\nLinhas OK: {enviados}\nFalhas: {falhas}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar: {e}")

        except Exception as e:
            from traceback import format_exc
            from utils.logs import log_emit
            log_emit(window["-LOG-"], "error", "excecao_na_ui", detalhe=str(e))
            sg.popup_error("Erro inesperado:\n\n" + str(e) + "\n\n" + format_exc())

    window.close()
    return 0


# ====== auxiliares de janelas secundárias ======

def _show_clientes_window(encontrados: List[Dict[str, Any]], nao_encontrados: List[Dict[str, Any]]):
    heads = ["TIPO","DOC","RAZAO","FANTASIA","IE","MUNICIPIO","UF"]
    vals = [[r.get(h,"") for h in heads] for r in encontrados]
    lay1 = [[sg.Text(f"Encontrados: {len(encontrados)}")],[sg.Table(vals, heads, key="-CLI-TBL-", expand_x=True, expand_y=True, justification="left")],[sg.Push(), sg.Button("Exportar CSV","-EXP-CSV-")]]
    lay2 = [[sg.Text(f"Não encontrados: {len(nao_encontrados)}")],[sg.Multiline("\n".join(nao_encontrados), size=(60,8), disabled=True, key="-NAO-")]]
    w = sg.Window("Clientes / Fornecedores (Domínio)", [[sg.TabGroup([[sg.Tab("Encontrados", lay1), sg.Tab("Não encontrados", lay2)]], expand_x=True, expand_y=True)],[sg.Push(), sg.Button("Fechar")]], modal=True, finalize=True, size=(900,500))
    while True:
        ev,_ = w.read()
        if ev in (sg.WINDOW_CLOSED,"Fechar"): break
        if ev == "-EXP-CSV-":
            out = sg.popup_get_file("Salvar CSV", save_as=True, default_extension=".csv", file_types=(("CSV","*.csv"),))
            if out:
                import csv
                with open(out,"w",newline="",encoding="utf-8") as f:
                    wr=csv.writer(f,delimiter=";"); wr.writerow(heads)
                    for r in encontrados: wr.writerow([r.get(h,"") for h in heads])
                sg.popup_ok(f"Exportado: {out}")
    w.close()

def _show_nfse_dominio_window(rows: List[Dict[str, Any]], base_rows: List[Dict[str, Any]]):
    heads = ["NFE","DATA","SITUACAO","VALOR","TOMADOR_DOC","TOMADOR_NOME"]
    vals = [[r.get(h,"") for h in heads] for r in rows]
    w = sg.Window("NFS-e no Domínio", [[sg.Table(vals, heads, key="-NFSE-", expand_x=True, expand_y=True, justification="left")],[sg.Push(), sg.Button("Fechar")]], modal=True, finalize=True, size=(900,500))
    w.read(); w.close()


if __name__ == "__main__":
    raise SystemExit(main())
