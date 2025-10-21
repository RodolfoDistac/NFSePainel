# main.py
from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path
from typing import Optional


def _echo(msg: str) -> None:
    print(msg, file=sys.stderr)


def _load_env() -> None:
    """
    Carrega variáveis de ambiente a partir de:
      1) config/.env   (PRIORITÁRIO, com override=True)
      2) ./.env        (FALLBACK, sem override)
    Depois, se ORACLE_CLIENT_LIB_DIR estiver definido, força ORACLE_MODE=thick.
    """
    # Caminhos baseados neste arquivo
    base_dir = Path(__file__).resolve().parent
    cfg_env = base_dir / "config" / ".env"
    root_env = base_dir / ".env"

    try:
        from dotenv import load_dotenv  # type: ignore

        # 1) Prioriza config/.env (override=True para garantir que os valores do projeto prevaleçam)
        if cfg_env.exists():
            load_dotenv(dotenv_path=cfg_env, override=True)

        # 2) Fallback: ./.env (apenas preenche o que faltar)
        if root_env.exists():
            load_dotenv(dotenv_path=root_env, override=False)

    except Exception:
        # Se python-dotenv não estiver instalado, apenas continua sem .env
        pass

    # Se tiver Instant Client configurado, padroniza thick (evita cair em thin por engano)
    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR")
    if lib_dir:
        os.environ["ORACLE_MODE"] = "thick"


def _parse_args(argv: list[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Uso:
      python main.py [INPUT_PATH] [--theme THEME]
    """
    input_path: Optional[str] = None
    theme: Optional[str] = None

    it = iter(argv[1:])
    for token in it:
        if token == "--theme":
            try:
                theme = next(it)
            except StopIteration:
                _echo("Parâmetro --theme sem valor. Ignorando.")
            continue
        # primeiro argumento posicional vira input_path
        if input_path is None:
            input_path = token
        else:
            _echo(f"Aviso: argumento ignorado: {token}")

    # Se não veio tema por argumento, usa APP_THEME do .env (se houver)
    theme = theme or os.getenv("APP_THEME")
    return input_path, theme


def main() -> int:
    _load_env()

    input_path, theme = _parse_args(sys.argv)

    # Importa o painel e delega a execução
    try:
        import panel  # type: ignore
    except Exception as exc:
        _echo("ERRO: não encontrei `panel.py` (ou houve falha ao importá-lo).")
        _echo("Crie o arquivo `panel.py` na mesma pasta deste `main.py` com a função "
              "`main(input_path: str | None = None, theme: str | None = None) -> int`.")
        _echo(f"Detalhes: {exc}")
        _echo(traceback.format_exc())
        return 2

    # Executa o painel
    try:
        rc = int(panel.main(input_path=input_path, theme=theme))  # type: ignore[attr-defined]
        return rc
    except SystemExit as se:
        # Se o panel.py der sys.exit(...)
        try:
            return int(se.code) if se.code is not None else 0
        except Exception:
            return 1
    except Exception as exc:
        _echo(f"ERRO em execução do painel: {exc}")
        _echo(traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
