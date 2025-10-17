# dataio/exporters.py
"""
Exportadores de dados (CSV).
API principal:
    export_csv(rows, out_path, columns)
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Optional
import csv


__all__ = ["export_csv"]


def export_csv(
    rows: List[Dict[str, Any]],
    out_path: str | Path,
    columns: Optional[List[str]] = None,
    encoding: str = "utf-8-sig",      # BOM facilita abrir no Excel
    newline: str = "",
) -> Path:
    """
    Exporta uma lista de dicts para CSV.

    Args:
        rows: lista de registros (cada item é um dict).
        out_path: caminho do arquivo de saída.
        columns: ordem das colunas. Se None, usa ordenação por chave.
        encoding: encoding do arquivo (padrão 'utf-8-sig' para Excel).
        newline: parâmetro repassado ao open() (padrão recomendado para csv no Windows).

    Returns:
        Path do arquivo salvo.
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Determina colunas
    if columns is None:
        # fallback: ordena chaves alfabeticamente do primeiro registro
        if rows:
            keys = set().union(*(r.keys() for r in rows))
            columns = sorted(keys)
        else:
            columns = []

    with out.open("w", encoding=encoding, newline=newline) as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: _stringify(row.get(col, "")) for col in columns})

    return out


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)
