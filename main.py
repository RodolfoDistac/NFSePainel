from __future__ import annotations

import argparse
import sys
from pathlib import Path


__version__ = "0.1.0"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="nfse-gui",
        description="Painel visual para leitura e exportação de NFSe (ABRASF)"
    )
    ap.add_argument(
        "--input",
        help="Caminho inicial para carregar (pasta, .zip ou .xml). Opcional.",
        default=None,
    )
    ap.add_argument(
        "--theme",
        help="Tema do PySimpleGUI (ex.: SystemDefault, DarkBlue3). Opcional.",
        default=None,
    )
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Import tardio para permitir mensagem amigável se o panel.py não existir
    try:
        from panel import main as panel_main  # type: ignore
    except Exception as exc:
        sys.stderr.write(
            "ERRO: não encontrei `panel.py` (ou houve falha ao importá-lo).\n"
            "Crie o arquivo `panel.py` na mesma pasta deste `main.py` com a função "
            "`main(input_path: str | None = None, theme: str | None = None) -> int`.\n"
            f"Detalhes: {exc}\n"
        )
        return 1

    # Normaliza o caminho de entrada (se fornecido)
    input_path: str | None = None
    if args.input:
        input_path = str(Path(args.input))

    # Executa o painel e retorna o código de saída
    try:
        exit_code = int(panel_main(input_path=input_path, theme=args.theme))
        return exit_code
    except TypeError:
        # Compat com versões antigas do panel.main sem kwargs
        return panel_main(input_path, args.theme)  # type: ignore
    except SystemExit as e:
        return int(getattr(e, "code", 0))
    except Exception as exc:
        sys.stderr.write(f"ERRO em execução do painel: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
