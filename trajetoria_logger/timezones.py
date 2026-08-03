"""Fuso horário do buffer local.

O PROBLEMA
----------
`created_at` do `rpa_buffer.db` era epoch UNIX. Epoch não diz nada a quem abre o arquivo, e
lido como UTC — que é o default de qualquer ferramenta — mostrava um log gravado às 08:58 em
Brasília como 11:58. O instante estava certo; a leitura, não.

A ESCOLHA
---------
`created_at` passou a ser TEXTO já no fuso local, com o offset dentro do próprio valor:

    2026-08-03T08:58:12.345678-03:00

Autoexplicativo: não sobra decisão de fuso para quem lê. E é UMA coluna só — não existe um
`created_at` "cru" ao lado de um "local".

Ordenação continua correta: neste formato de largura fixa a ordem lexicográfica é a ordem
cronológica, então o `ORDER BY created_at`, o índice e o corte por excedente seguem valendo
sem mudança. (Isso depende de o offset ser constante — é, ver abaixo.)

O `timestamp` do PAYLOAD não muda: continua epoch UNIX. É o que a API valida e o que vira
`TIMESTAMPTZ` no Postgres, e deslocá-lo em 3h faria `logged_at` divergir de `received_at`
(preenchido pelo servidor com `now()`), transformando qualquer medida de latência em lixo.
As duas coisas convivem: epoch no que trafega, texto local no que se lê. `created_at` é
derivado desse mesmo epoch — mesmo instante, escrito de outro jeito.

Resolução do fuso, em ordem:
  1. `zoneinfo.ZoneInfo(nome)` — respeita horário de verão caso ele volte;
  2. offset FIXO (default -03:00), quando a máquina não tem a base IANA. É o caso das
     máquinas de RPA: no Windows a base só existe se o pacote `tzdata` estiver instalado.

O fallback não é degradação hoje: o Brasil não tem horário de verão desde 2019, então os dois
caminhos dão o mesmo resultado. Ele existe porque a lib NUNCA pode derrubar o robô por um
detalhe de formatação. Quem quiser blindagem contra um retorno do horário de verão instala
`tzdata` na máquina do robô — nada mais muda. (Se isso acontecer, o offset deixa de ser
constante e só a ordenação de linhas gravadas ATRAVÉS da virada fica ambígua; o `timestamp`
epoch do payload, que é o que a API ordena, continua exato.)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo

# Fuso de Brasília: é onde os robôs rodam.
DEFAULT_TIMEZONE = "America/Sao_Paulo"
DEFAULT_UTC_OFFSET_HOURS = -3.0

# Formato de `created_at`. Largura fixa (milissegundos no lado SQL, microssegundos no lado
# Python) + offset explícito: legível, ordenável como texto e sem ambiguidade de fuso.
_SQLITE_FORMAT = "%Y-%m-%dT%H:%M:%f"


def resolve_timezone(
    name: str | None = DEFAULT_TIMEZONE,
    fallback_offset_hours: float = DEFAULT_UTC_OFFSET_HOURS,
) -> tzinfo:
    """Objeto de fuso a partir de um nome IANA, com offset fixo como rede de segurança.

    Nunca levanta: nome desconhecido ou base IANA ausente caem no offset fixo.
    """
    if name:
        try:
            from zoneinfo import ZoneInfo  # stdlib desde 3.9; a base pode faltar no Windows

            return ZoneInfo(name)
        except Exception:
            pass  # sem base IANA (ou nome inválido) -> offset fixo, ver docstring do módulo
    return timezone(timedelta(hours=float(fallback_offset_hours)))


def local_iso(epoch_seconds: float, tz: tzinfo) -> str:
    """Epoch -> ISO 8601 no fuso `tz`, COM o offset dentro do valor.

    Mesmo instante do epoch recebido: só a escrita muda.
    """
    return datetime.fromtimestamp(float(epoch_seconds), tz=tz).isoformat()


def sqlite_local_iso(source: str, offset_hours: float, from_epoch: bool = False) -> str:
    """Expressão SQL que produz o MESMO formato de `local_iso`, para o SQLite.

    Usada em dois lugares do buffer:
      * `DEFAULT` da coluna (`source="'now'"`) — rede de segurança para qualquer INSERT que
        não informe `created_at`; no caminho normal quem formata é o Python;
      * migração de banco antigo (`source="created_at"`, `from_epoch=True`), convertendo os
        epoch já gravados sem perder o instante.

    `offset_hours` vem de config (um float), então não há interpolação de texto externo aqui.
    """
    parts = [f"'{_SQLITE_FORMAT}'", source]
    if from_epoch:
        parts.append("'unixepoch'")
    parts.append(f"'{offset_hours:+g} hours'")
    return f"strftime({', '.join(parts)}) || '{offset_suffix(offset_hours)}'"


def offset_suffix(offset_hours: float) -> str:
    """Offset no formato ISO 8601 (`-03:00`), para colar no fim do timestamp."""
    total_minutes = int(round(float(offset_hours) * 60))
    sign = "-" if total_minutes < 0 else "+"
    total_minutes = abs(total_minutes)
    return f"{sign}{total_minutes // 60:02d}:{total_minutes % 60:02d}"
