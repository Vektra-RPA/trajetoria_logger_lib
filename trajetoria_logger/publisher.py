"""Publicação confiável no RabbitMQ com publisher confirms (seção 5.3, revisada).

Mudanças da revisão:
  * A URL do broker é DINÂMICA: vem da API em runtime (via update_url). Enquanto não há
    URL, publish() retorna False e os logs ficam no buffer.
  * publish(..., bulk=False) roteia para a fila padrão (rpa.log.<level>) ou, com bulk=True,
    para a fila bulk (rpa.bulk.<level>) — usada para backlog de execuções anteriores.

`publish` retorna True SOMENTE se o broker confirmar o roteamento para uma fila (R1).
"""
from __future__ import annotations

import json

import pika
import pika.exceptions

EXCHANGE = "rpa_logs"

# level -> nome usado na routing key
LEVEL_NAMES = {1: "critical", 2: "error", 3: "warning", 4: "info", 5: "debug"}


class Publisher:
    def __init__(self, url: str | None = None):
        self._url = url
        self._conn: pika.BlockingConnection | None = None
        self._channel = None

    def has_url(self) -> bool:
        return self._url is not None

    def update_url(self, url: str | None) -> None:
        """Define/atualiza a URL do broker (entregue pela API). Reconecta se mudou."""
        if url and url != self._url:
            self._url = url
            self._close_quietly()  # força reconexão com a nova URL

    def _ensure_connection(self) -> None:
        if self._conn is not None and self._conn.is_open:
            return
        params = pika.URLParameters(self._url)
        self._conn = pika.BlockingConnection(params)
        self._channel = self._conn.channel()
        self._channel.exchange_declare(
            exchange=EXCHANGE, exchange_type="topic", durable=True
        )
        self._channel.confirm_delivery()

    def publish(self, log_dict: dict, bulk: bool = False) -> bool:
        if not self._url:
            return False  # ainda sem conexão do Rabbit -> mantém no buffer
        level = int(log_dict.get("level", 4))
        prefix = "rpa.bulk" if bulk else "rpa.log"
        routing_key = f"{prefix}.{LEVEL_NAMES.get(level, 'info')}"
        body = json.dumps(log_dict).encode("utf-8")
        try:
            self._ensure_connection()
            # mandatory=True (R1): se não houver fila que roteie, o broker devolve e pika
            # levanta UnroutableError -> retornamos False e o log fica no buffer.
            self._channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2, content_type="application/json"
                ),
                mandatory=True,
            )
            return True
        except (pika.exceptions.UnroutableError, pika.exceptions.NackError):
            return False
        except Exception:
            # Falha de conexão/canal: descarta a conexão p/ reconectar e mantém no buffer.
            self._close_quietly()
            return False

    def _close_quietly(self) -> None:
        try:
            if self._conn is not None and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass
        finally:
            self._conn = None
            self._channel = None

    def close(self) -> None:
        self._close_quietly()
