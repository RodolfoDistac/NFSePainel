# dataio/loaders.py
"""
Carregadores de entrada para NFSe.
Lê XMLs a partir de:
  - diretório (busca recursiva por *.xml e também abre todos os *.zip encontrados)
  - arquivo .zip (lendo apenas entradas .xml)
  - arquivo .xml único

APIs principais:
    iter_xml_bytes(input_path: str | Path) -> Iterator[tuple[str, bytes]]
    list_entries(input_path: str | Path) -> list[str]
    count_xml(input_path: str | Path) -> int
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator, Tuple, List
import zipfile

__all__ = ["iter_xml_bytes", "list_entries", "count_xml"]


def _iter_zip_xml(zp: Path) -> Iterator[Tuple[str, bytes]]:
    """Itera XMLs de dentro de um arquivo .zip."""
    # strict_timestamps=False evita warnings em alguns zips antigos (Py3.11+)
    with zipfile.ZipFile(zp, "r") as zf:
        for info in sorted(zf.infolist(), key=lambda i: i.filename.lower()):
            if info.is_dir():
                continue
            name = info.filename
            if name.lower().endswith(".xml"):
                # identifica origem como zip:entrada
                yield f"{zp.name}:{name}", zf.read(name)


def iter_xml_bytes(input_path: str | Path) -> Iterator[Tuple[str, bytes]]:
    """
    Itera (nome, conteudo_em_bytes) para cada XML encontrado no caminho informado.
    - Diretório: rglob('*.xml') + todos os XMLs dentro de cada rglob('*.zip')
    - .zip: lê entradas com sufixo .xml
    - .xml: único arquivo

    Levanta FileNotFoundError se o caminho não existir ou não for suportado.
    """
    p = Path(input_path)

    if not p.exists():
        raise FileNotFoundError(f"Caminho não encontrado: {p}")

    # Caso: ZIP
    if p.is_file() and p.suffix.lower() == ".zip":
        yield from _iter_zip_xml(p)
        return

    # Caso: diretório (recursivo)
    if p.is_dir():
        # 1) XMLs soltos
        for fp in sorted(p.rglob("*.xml"), key=lambda x: str(x).lower()):
            yield str(fp), fp.read_bytes()
        # 2) XMLs dentro de cada ZIP encontrado
        for zp in sorted(p.rglob("*.zip"), key=lambda x: str(x).lower()):
            yield from _iter_zip_xml(zp)
        return

    # Caso: XML único
    if p.is_file() and p.suffix.lower() == ".xml":
        yield str(p), p.read_bytes()
        return

    raise FileNotFoundError(
        f"Tipo de entrada não suportado (esperado pasta, .zip ou .xml): {p}"
    )


def list_entries(input_path: str | Path) -> List[str]:
    """
    Lista os nomes/paths dos XMLs que seriam lidos por iter_xml_bytes,
    sem carregar o conteúdo.
    """
    p = Path(input_path)

    if not p.exists():
        raise FileNotFoundError(f"Caminho não encontrado: {p}")

    if p.is_file() and p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p, "r") as zf:
            return [
                f"{p.name}:{info.filename}"
                for info in sorted(zf.infolist(), key=lambda i: i.filename.lower())
                if not info.is_dir() and info.filename.lower().endswith(".xml")
            ]

    if p.is_dir():
        entries: List[str] = []
        # XMLs soltos
        entries.extend(str(fp) for fp in sorted(p.rglob("*.xml"), key=lambda x: str(x).lower()))
        # XMLs dentro de ZIPs
        for zp in sorted(p.rglob("*.zip"), key=lambda x: str(x).lower()):
            with zipfile.ZipFile(zp, "r") as zf:
                entries.extend(
                    f"{zp.name}:{info.filename}"
                    for info in sorted(zf.infolist(), key=lambda i: i.filename.lower())
                    if not info.is_dir() and info.filename.lower().endswith(".xml")
                )
        return entries

    if p.is_file() and p.suffix.lower() == ".xml":
        return [str(p)]

    raise FileNotFoundError(
        f"Tipo de entrada não suportado (esperado pasta, .zip ou .xml): {p}"
    )


def count_xml(input_path: str | Path) -> int:
    """Retorna a contagem de XMLs detectados no caminho informado (inclui XMLs dentro de .zip)."""
    return len(list_entries(input_path))
