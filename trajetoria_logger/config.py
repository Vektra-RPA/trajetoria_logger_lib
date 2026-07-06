"""Configuração da biblioteca (seção 5.1 do spec, revisada).

IMPORTANTE (decisão de arquitetura): a biblioteca é distribuída e roda na máquina do
CLIENTE. Ela NÃO lê variáveis de ambiente / .env do nosso projeto — a configuração é
recebida explicitamente via este dataclass no init do RPALoggerClient.

A conexão do RabbitMQ NÃO vem daqui: ela é entregue pela API ao validar a chave (chave
ativa). O dev informa apenas a chave e a URL da API.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RPALoggerConfig:
    """Parâmetros que o RPA passa ao instanciar o cliente."""

    api_key: str
    api_base_url: str
    buffer_path: str = "./rpa_buffer.db"
    # Reservado: hoje o kill switch só para em resposta explícita active=false, então a
    # graça não é usada para a decisão de parada. Mantido para uso/diagnóstico futuro.
    license_grace_hours: float = 24.0
    license_check_interval_minutes: float = 30.0
