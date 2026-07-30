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


@dataclass
class RPALoggerConfig:
    """Parâmetros que o RPA passa ao instanciar o cliente."""

    api_key: str
    api_base_url: str
    buffer_path: str = "./rpa_buffer.db"

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
