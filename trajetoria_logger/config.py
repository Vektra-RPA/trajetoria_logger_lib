"""Configuração da biblioteca.

IMPORTANTE (decisão de arquitetura): a biblioteca é distribuída e roda na máquina do
CLIENTE. Ela NÃO lê variáveis de ambiente / .env do nosso projeto — a configuração é
recebida explicitamente via este dataclass no init do RPALoggerClient.

A lib fala com UM único destino: a nossa API, por HTTPS. Não há mais URL de broker aqui
(nem entregue pela API): o RabbitMQ virou detalhe interno do servidor.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .timezones import DEFAULT_TIMEZONE, DEFAULT_UTC_OFFSET_HOURS


@dataclass
class RPALoggerConfig:
    """Parâmetros que o RPA passa ao instanciar o cliente."""

    api_key: str
    api_base_url: str
    buffer_path: str = "./rpa_buffer.db"

    # ---- fuso do buffer local ----
    # Fuso em que o `created_at` do `rpa_buffer.db` é gravado (texto ISO 8601 com offset).
    # Default -03:00, para que um log das 08:58 seja lido como 08:58 e não como 11:58. O
    # `timestamp` do payload continua sendo epoch UNIX — é ele que a API valida. Ver
    # `timezones.py` para o porquê da separação.
    log_timezone: str = DEFAULT_TIMEZONE
    # Só entra em jogo se a máquina não tiver a base IANA (Windows sem `tzdata`) ou o nome
    # acima não existir. Também é o offset usado no DEFAULT da coluna, no SQLite.
    log_utc_offset_hours: float = DEFAULT_UTC_OFFSET_HOURS

    # ---- kill switch (R2) ----
    # Tempo máximo que o robô pode rodar SEM uma confirmação explícita da API. É o
    # "passe com validade": rede caindo não derruba cliente pagante, mas também não
    # autoriza rodar para sempre offline. O servidor pode encurtar/estender este prazo
    # (campo `lease_seconds` na resposta) — este valor é o fallback do cliente.
    license_grace_hours: float = 24.0
    license_check_interval_minutes: float = 30.0

    # ---- envio (HTTP) ----
    batch_size: int = 500          # logs por POST /logs/batch
    flush_interval_seconds: float = 1.0
    http_timeout_seconds: float = 15.0
    verify_tls: bool = True        # só desligue em teste local com certificado próprio
    max_buffered_logs: int = 100_000

    # Canal de aviso da lib para a APLICAÇÃO (logs descartados, licença expirando...).
    # Default None = silêncio: a lib NUNCA escreve na saída do cliente sem ser pedida.
    on_warning: Callable[[str], None] | None = None
