# panel.py
from __future__ import annotations

import PySimpleGUI as sg
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

COLUMNS: List[str] = [
    "TOMADOR","NFE","EMISSAO","VALOR","ALIQ","INSS","IR","PIS","COFINS","CSLL","ISS_RET","ISS_NORMAL","DISCRIMINACAO"
]
_NUMERIC_COLS = {"VALOR","ALIQ","INSS","IR","PIS","COFINS","CSLL","ISS_RET","ISS_NORMAL"}

DEFAULT_EXPORT_DIR = Path("C:/A")  # ✔ padrão solicitado

def _compute_scaling(screen_w: int, screen_h: int) -> float:
    if screen_h >= 2160: return 1.6
    if screen_h >= 1440: return 1.3
    if screen_h >= 1080: return 1.1
    return 1.0

def _normalize_path(p: str) -> str:
    return (p or "").strip().strip('"').strip("'")

def _validate_input_path(raw_path: str) -> Optional[str]:
    p = Path(_normalize_path(raw_path))
    if p.exists() and (p.is_dir() or p.suffix.lower() in {".zip", ".xml"}):
        return str(p)
    return None

def _safe_long_job(input_str: str):
    try:
        rows, counts, errors = _parse_all(input_str)
        return ("ok", (rows, counts, errors))
    except Exception as e:
        return ("error", f"{type(e).__name__}: {e}")

def _parse_all(input_str: str) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
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
            if v is None: continue
            if q in str(v).lower(): return True
        return False
    return [r for r in rows if match(r)]

def _parse_brl_number(s: str) -> float:
    s = str(s or "").strip()
    if not s: return 0.0
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
        if str(r.get("STATUS","")).lower() == "cancelada":
            colors.append((idx, "black", "#ffcccc"))
    return colors

def _autosize_table(table_elem: sg.Table, values: List[List[str]], headings: List[str]) -> None:
    tv = table_elem.Widget  # ttk.Treeview
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

def _make_window(theme: Optional[str] = None) -> sg.Window:
    if theme:
        try: sg.theme(theme)
        except Exception: sg.theme("SystemDefault")
    else:
        sg.theme("SystemDefault")

    sw, sh = sg.Window.get_screen_size()
    scale = _compute_scaling(sw, sh)
    sg.set_options(dpi_awareness=True, scaling=scale, font=("Segoe UI", 10))
    win_size = (int(sw * 0.9), int(sh * 0.9))

    # Barra de ações (espelha Java)
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
        values=[], headings=COLUMNS, key="-TABLE-",
        auto_size_columns=False, justification="left",
        display_row_numbers=False, enable_events=True,
        num_rows=20, expand_x=True, expand_y=True,
        vertical_scroll_only=False, tooltip="Resultados das NFS-e",
    )
    row_status = [
        sg.Text("Status:"), sg.Text("Aguardando…", key="-STATUS-", expand_x=True),
        sg.Text("Total:"), sg.Text("0", key="-TOT-"),
        sg.Text("  Sucesso:"), sg.Text("0", key="-OK-"),
        sg.Text("  Falha:"), sg.Text("0", key="-FAIL-"),
    ]
    frame_logs = sg.Frame("Logs", [[sg.Multiline(
        key="-LOG-", size=(10, 8), expand_x=True, expand_y=True, autoscroll=True, write_only=True
    )]], expand_x=True, expand_y=True)

    layout = [*bar_actions, *row_input, *row_filter, [table], [sg.Column([row_status], expand_x=True)], [frame_logs], [sg.Push(), sg.Button("Sair", key="-EXIT-")]]

    window = sg.Window("Painel NFSe", layout, size=win_size, resizable=True, finalize=True, enable_close_attempted_event=True)
    try: window.maximize()
    except Exception: pass
    _attach_header_sort(window)
    return window

def _attach_header_sort(window: sg.Window) -> None:
    tv = window["-TABLE-"].Widget
    for i, col in enumerate(COLUMNS):
        try:
            tv.heading(str(i), text=col, command=lambda c=i: window.write_event_value("-SORT-", c))
        except Exception:
            pass

def main(input_path: Optional[str] = None, theme: Optional[str] = None) -> int:
    from utils.logs import log_emit
    from dataio.exporters import export_csv
    # imports tardios para evitar dependências na abertura
    from services.dominio_export import export_cabecalho_tomador, export_final

    window = _make_window(theme)
    all_rows: List[Dict[str, Any]] = []
    view_rows: List[Dict[str, Any]] = []
    sort_state = {"col": None, "asc": True}
    perfil = "local"  # conforme decisão 1

    if input_path:
        valid_input = _validate_input_path(input_path)
        if valid_input:
            window["-INPUT-"].update(valid_input)
            window["-STATUS-"].update("Processando…")
            window.perform_long_operation(lambda: _safe_long_job(valid_input), "-DONE-")
        else:
            sg.popup_error("O caminho informado via linha de comando não existe ou não é suportado.\nUse os botões Pasta/Arquivo e depois clique em Carregar.")

    def _render(rows: List[Dict[str, Any]]):
        table_vals = [[str(r.get(col, "")) for col in COLUMNS] for r in rows]
        row_colors = _make_row_colors(rows)
        window["-TABLE-"].update(values=table_vals, row_colors=row_colors)
        _autosize_table(window["-TABLE-"], table_vals, COLUMNS)

    while True:
        event, values = window.read()
        if event in (sg.WINDOW_CLOSED, "-EXIT-", sg.WIN_CLOSE_ATTEMPTED_EVENT):
            break
        try:
            if event in ("-LOAD-", "-LOAD-BAR-"):
                # ambos disparam a mesma lógica
                raw = values.get("-INPUT-", "")
                if not raw.strip():
                    sg.popup_error("Informe um caminho (Pasta/Arquivo) antes de carregar."); continue
                valid = _validate_input_path(raw)
                if not valid:
                    sg.popup_error("Caminho inválido. Selecione uma PASTA ou um ARQUIVO .zip / .xml válido."); continue
                window["-STATUS-"].update("Processando…")
                window["-TABLE-"].update(values=[])
                window["-TOT-"].update("0"); window["-OK-"].update("0"); window["-FAIL-"].update("0")
                window["-LOG-"].update("")
                window.perform_long_operation(lambda: _safe_long_job(valid), "-DONE-")

            if event == "-DONE-":
                from utils.logs import log_emit as emit
                kind, payload = values[event]
                if kind == "error":
                    msg = str(payload); emit(window["-LOG-"], "error", "processamento_falhou", detalhe=msg)
                    window["-STATUS-"].update("Falhou."); sg.popup_error(f"Falha no processamento:\n{msg}"); continue
                rows, counts, errors = payload
                all_rows = rows
                view_rows = list(all_rows)
                sort_state.update(col=None, asc=True)
                _render(view_rows)
                window["-STATUS-"].update("Concluído.")
                window["-TOT-"].update(str(counts.get("total", 0)))
                window["-OK-"].update(str(counts.get("ok", 0)))
                window["-FAIL-"].update(str(counts.get("fail", 0)))
                log_emit(window["-LOG-"], "info", "processamento_concluido", **counts)
                for err in errors[:50]:
                    log_emit(window["-LOG-"], "error", "falha_parse", detalhe=err)
                if len(errors) > 50:
                    log_emit(window["-LOG-"], "warn", "erros_suprimidos", restantes=len(errors) - 50)

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

            # ----- Barra de ações -----
            if event == "-LOGIN-":
                perfil = "local"
                sg.popup_ok("Perfil de conexão definido: LOCAL")

            if event == "-EXP-HEAD-":
                if not all_rows:
                    sg.popup_error("Nada para exportar. Importe os XMLs primeiro."); continue
                try:
                    out = export_cabecalho_tomador(all_rows, DEFAULT_EXPORT_DIR)
                    sg.popup_ok(f"Arquivo gerado:\n{out}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar cabeçalho+tomador:\n{e}")

            if event == "-IMP-CLI-":
                sg.popup_ok("Importar Clientes: em desenvolvimento (foco atual no fluxo de XML/Export).")

            if event == "-IMP-NFS-":
                sg.popup_ok("Importar NFS Domínio: em desenvolvimento.")

            if event == "-GERA-PARC-":
                if not view_rows:
                    sg.popup_error("Nenhuma linha visível. Use o filtro (ex.: por descrição) e tente novamente.")
                    continue
                # Regra: 410->411 (normal); 424->425 (retido). Aplica nas linhas visíveis não canceladas.
                changed = 0
                for r in view_rows:
                    if str(r.get("STATUS","")).lower() == "cancelada":
                        continue
                    acc = str(r.get("ACUMULADOR","")).strip()
                    if acc == "410":
                        r["ACUMULADOR"] = "411"
                        changed += 1
                    elif acc == "424":
                        r["ACUMULADOR"] = "425"
                        changed += 1
                    elif not acc:
                        # Deriva do ISS para robustez
                        is_ret = (r.get("ISS_RET","0,00") != "0,00")
                        r["ACUMULADOR"] = "425" if is_ret else "411"
                        changed += 1
                _render(view_rows)
                from utils.logs import log_emit as emit
                emit(window["-LOG-"], "info", "parcelas_geradas_manual", linhas_afetadas=changed)
                sg.popup_ok(f"Acumuladores ajustados (410→411 / 424→425) em {changed} linha(s) visível(is).\n"
                            "Dica: use o filtro por descrição para aplicar somente às notas a prazo.")

            if event == "-EXP-FINAL-":
                if not all_rows:
                    sg.popup_error("Nada para exportar. Importe os XMLs primeiro."); continue
                try:
                    out = export_final(all_rows, DEFAULT_EXPORT_DIR)
                    sg.popup_ok(f"Arquivo final gerado:\n{out}")
                except Exception as e:
                    sg.popup_error(f"Falha ao exportar:\n{e}")

            if event == "-EXPORT-":
                if not all_rows:
                    sg.popup_error("Nada para exportar. Carregue os dados primeiro."); continue
                out = sg.popup_get_file("Salvar CSV", save_as=True, default_extension=".csv", file_types=(("CSV", "*.csv"),))
                if out:
                    try:
                        export_csv(all_rows, Path(out), columns=COLUMNS)
                        sg.popup_ok(f"Exportado para: {out}")
                    except Exception as e:
                        sg.popup_error(f"Falha ao exportar: {e}")

        except Exception as e:
            from utils.logs import log_emit
            from traceback import format_exc
            log_emit(window["-LOG-"], "error", "excecao_na_ui", detalhe=str(e))
            sg.popup_error("Ocorreu um erro inesperado.\n\n" + str(e) + "\n\n" + format_exc())

    window.close(); return 0
