# parsers/nfse_abrasf.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import calendar
from typing import Dict, Optional, Sequence, Union
import xml.etree.ElementTree as ET


# ============
# Utilidades
# ============

def _digits_only(s: Optional[str]) -> str:
    if not s:
        return ""
    return "".join(ch for ch in s if ch.isdigit())


def _to_decimal(v: Union[str, float, int, Decimal, None]) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    if s == "":
        return Decimal("0")
    # normaliza decimal brasileiro
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _fmt_brl(d: Decimal) -> str:
    q = d.quantize(Decimal("0.00"))
    s = f"{q:.2f}"
    inteiro, frac = s.split(".")
    inteiro = int(inteiro)
    inteiro = f"{inteiro:,}".replace(",", ".")
    return f"{inteiro},{frac}"


def _fmt_data_br(value: Optional[str]) -> str:
    if not value:
        return ""
    s = value.strip()
    # tenta “YYYY-MM-DD HH:MM:SS” → “DD/MM/YYYY”
    try:
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    # tenta “YYYY-MM-DD”
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    # tenta “DD/MM/YYYY”
    try:
        dt = datetime.strptime(s[:10], "%d/%m/%Y")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    # tenta “M/YYYY” ou “MM/YYYY” (Competencia)
    try:
        if "/" in s and len(s) <= 7:
            mes, ano = s.split("/")
            mes = mes.zfill(2)
            return f"01/{mes}/{ano}"
    except Exception:
        pass
    return s


def _parse_emissao_iso(value: Optional[str]) -> Optional[date]:
    """Retorna a data (date) a partir de 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD' ou 'DD/MM/YYYY'."""
    if not value:
        return None
    s = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).date()
        except Exception:
            continue
    # Competência MM/YYYY → 1º dia do mês
    if "/" in s and len(s) <= 7:
        try:
            mes, ano = s.split("/")
            mes = mes.zfill(2)
            return datetime.strptime(f"01/{mes}/{ano}", "%d/%m/%Y").date()
        except Exception:
            pass
    return None


def _last_day_of_month(d: date) -> date:
    _, last = calendar.monthrange(d.year, d.month)
    return date(d.year, d.month, last)


# Alguns layouts trazem entidades “quebradas” (sem & e com ;). Fazemos um mapeamento simples.
_ENT_MAP: Dict[str, str] = {
    "Atilde;": "Ã", "atilde;": "ã",
    "Otilde;": "Õ", "otilde;": "õ",
    "Ccedil;": "Ç", "ccedil;": "ç",
    "Aacute;": "Á", "aacute;": "á",
    "Eacute;": "É", "eacute;": "é",
    "Iacute;": "Í", "iacute;": "í",
    "Oacute;": "Ó", "oacute;": "ó",
    "Uacute;": "Ú", "uacute;": "ú",
    "Agrave;": "À", "agrave;": "à",
    "Acirc;": "Â", "acirc;": "â",
    "Ecirc;": "Ê", "ecirc;": "ê",
    "Ocirc;": "Ô", "ocirc;": "ô",
    "Ucirc;": "Û", "ucirc;": "û",
    # fallback comuns sem ponto e vírgula:
    "Atilde": "Ã", "atilde": "ã",
    "Otilde": "Õ", "otilde": "õ",
    "Ccedil": "Ç", "ccedil": "ç",
    "Aacute": "Á", "aacute": "á",
    "Eacute": "É", "eacute": "é",
    "Iacute": "Í", "iacute": "í",
    "Oacute": "Ó", "oacute": "ó",
    "Uacute": "Ú", "uacute": "ú",
    "Agrave": "À", "agrave": "à",
}

def _fix_discriminacao(texto: Optional[str]) -> str:
    if not texto:
        return ""
    s = texto
    for k, v in _ENT_MAP.items():
        s = s.replace(k, v)
        s = s.replace(f"&{k}", v)  # caso venha com &
    # normaliza quebras
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()


def _local(tag: str) -> str:
    # remove namespace {…}
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _text(el: Optional[ET.Element]) -> Optional[str]:
    if el is None or el.text is None:
        return None
    t = el.text.strip()
    return t if t != "" else None


def _find_text_any(root: ET.Element, names: Sequence[str]) -> Optional[str]:
    """
    Procura em qualquer profundidade por nomes (ignorando namespace).
    Ex.: names=["NumeroNFe","NumeroNfse","Numero"]
    """
    for nm in names:
        for el in root.iter():
            if _local(el.tag) == nm:
                t = _text(el)
                if t:
                    return t
    return None


# ================
# Modelo de saída
# ================

@dataclass
class NFSeRow:
    # Ordem exigida:
    tomador: str = ""         # TOMADOR
    nfe: str = ""             # NFE
    emissao: str = ""         # EMISSAO
    valor: str = "0,00"       # VALOR
    aliq: str = "0,00"        # ALIQ
    inss: str = "0,00"        # INSS
    ir: str = "0,00"          # IR
    pis: str = "0,00"         # PIS
    cofins: str = "0,00"      # COFINS
    csll: str = "0,00"        # CSLL
    iss_ret: str = "0,00"     # ISS_RET
    iss_normal: str = "0,00"  # ISS_NORMAL
    discriminacao: str = ""   # DISCRIMINACAO
    vencimento: str = ""      # VENCIMENTO (pré-preenchido; no painel o rótulo vira "PARCELA")
    acumulador: str = ""      # ACUMULADOR (410/424 por padrão; 411/425 quando gerar parcela)

    def to_row(self) -> Dict[str, str]:
        return {
            "TOMADOR": self.tomador,
            "NFE": self.nfe,
            "EMISSAO": self.emissao,
            "VALOR": self.valor,
            "ALIQ": self.aliq,
            "INSS": self.inss,
            "IR": self.ir,
            "PIS": self.pis,
            "COFINS": self.cofins,
            "CSLL": self.csll,
            "ISS_RET": self.iss_ret,
            "ISS_NORMAL": self.iss_normal,
            "DISCRIMINACAO": self.discriminacao,
            "VENCIMENTO": self.vencimento,
            "ACUMULADOR": self.acumulador,
        }


class NFSeParser:
    """
    Parser focado no layout municipal (SIGISS) com os mapeamentos solicitados:

      TOMADOR         <- CPFCNPJTomador/CPF (fallback: /CNPJ)
      NFE             <- NumeroNFe
      EMISSAO         <- DataEmissaoNFe (fallback: DataEmissao ou Competencia)
      VALOR           <- ValorServicos
      ALIQ            <- AliquotaServicos  (se vier <=1, converte 0,02→2,00)
      INSS            <- ValorInss
      IR              <- ValorIr
      PIS             <- ValorPis
      COFINS          <- ValorCofins
      CSLL            <- ValorCsll
      ISS_RET/NORMAL  <- conforme ISSRetido (SIM → ISS_RET=ValorISS; NAO → ISS_NORMAL=ValorISS)
      DISCRIMINACAO   <- Discriminacao (com correção de entidades “quebradas”)

    Regras adicionais:
      - Pré-preenche VENCIMENTO com o último dia do mesmo mês da data de EMISSAO.
      - ACUMULADOR padrão:
          * se ISS_NORMAL > 0 => 410
          * se ISS_RET > 0    => 424
        (A troca para 411/425 será feita no painel quando “gerar parcela”.)
      - Se StatusNFe == "Cancelada": zerar todos os valores (UI pinta a linha em vermelho).
    """

    def parse(self, xml_data: Union[bytes, str], name_hint: str = "") -> NFSeRow:
        if isinstance(xml_data, bytes):
            root = ET.fromstring(xml_data)
        else:
            root = ET.fromstring(xml_data.encode("utf-8"))

        row = NFSeRow()

        # ---------- TOMADOR ----------
        tomador = ( _find_text_any(root, ["CPF"]) or
                    _find_text_any(root, ["CNPJ"]) or
                    _find_text_any(root, ["CPFCNPJTomador"]) )
        row.tomador = _digits_only(tomador)

        # ---------- NFE ----------
        row.nfe = (_find_text_any(root, ["NumeroNFe"]) or "").strip()

        # ---------- EMISSÃO ----------
        emissao_raw = (_find_text_any(root, ["DataEmissaoNFe"]) or
                       _find_text_any(root, ["DataEmissao"]) or
                       _find_text_any(root, ["Competencia"]))
        row.emissao = _fmt_data_br(emissao_raw)

        # ---------- VALORES PRINCIPAIS ----------
        v_serv = _to_decimal(_find_text_any(root, ["ValorServicos"]))
        row.valor = _fmt_brl(v_serv)

        aliq_raw = _to_decimal(_find_text_any(root, ["AliquotaServicos"]))
        aliq_pct = aliq_raw if aliq_raw > 1 else (aliq_raw * Decimal("100"))
        row.aliq = _fmt_brl(aliq_pct)

        row.inss   = _fmt_brl(_to_decimal(_find_text_any(root, ["ValorInss"])))
        row.ir     = _fmt_brl(_to_decimal(_find_text_any(root, ["ValorIr"])))
        row.pis    = _fmt_brl(_to_decimal(_find_text_any(root, ["ValorPis"])))
        row.cofins = _fmt_brl(_to_decimal(_find_text_any(root, ["ValorCofins"])))
        row.csll   = _fmt_brl(_to_decimal(_find_text_any(root, ["ValorCsll"])))

        # ---------- ISS (retido/normal) ----------
        v_iss = _to_decimal(_find_text_any(root, ["ValorISS"]))
        iss_retido = (_find_text_any(root, ["ISSRetido"]) or "").strip().upper()

        is_retido = False
        if iss_retido in ("SIM", "S", "TRUE", "1"):
            is_retido = True
        elif iss_retido in ("NAO", "N", "FALSE", "0", "NÃO"):
            is_retido = False
        else:
            is_retido = iss_retido == "1"

        if is_retido:
            row.iss_ret = _fmt_brl(v_iss)
            row.iss_normal = "0,00"
        else:
            row.iss_ret = "0,00"
            row.iss_normal = _fmt_brl(v_iss)

        # ---------- DISCRIMINAÇÃO ----------
        row.discriminacao = _fix_discriminacao(_find_text_any(root, ["Discriminacao"]) or "")

        # ---------- CANCELADA? zerar valores ----------
        status = (_find_text_any(root, ["StatusNFe"]) or "").strip().upper()
        if status == "CANCELADA":
            row.valor = "0,00"
            row.aliq = "0,00"
            row.inss = "0,00"
            row.ir = "0,00"
            row.pis = "0,00"
            row.cofins = "0,00"
            row.csll = "0,00"
            row.iss_ret = "0,00"
            row.iss_normal = "0,00"

        # ---------- PARCELA (pré-preencher VENCIMENTO) ----------
        venc = ""
        d = _parse_emissao_iso(emissao_raw)
        if d:
            ld = _last_day_of_month(d)
            venc = ld.strftime("%d/%m/%Y")
        row.vencimento = venc

        # ---------- ACUMULADOR padrão ----------
        # regra: iss normal 410 / iss ret 424
        iss_norm_dec = _to_decimal(row.iss_normal.replace(".", "").replace(",", "."))
        iss_ret_dec  = _to_decimal(row.iss_ret.replace(".", "").replace(",", "."))
        if iss_ret_dec > 0:
            row.acumulador = "424"
        else:
            # se não é retido, tratamos como normal (mesmo que seja 0)
            row.acumulador = "410"

        return row


# Retrocompatibilidade com importações existentes
RowNFSe = NFSeRow
__all__ = ["NFSeParser", "NFSeRow", "RowNFSe"]
