"""Smoke test do caminho de ingestão: buffer local -> POST /logs/batch -> confirmação.

Prova, de forma isolada, os pontos críticos de R1 no novo desenho:
  1. o log é gravado no buffer SQLite ANTES de qualquer rede;
  2. ele só sai do buffer para os `log_id` que a API CONFIRMOU (`accepted`);
  3. com a API/broker fora, NADA sai do buffer (o teste 2 abaixo desliga isso na prática
     apontando para uma porta morta).

Rodar (a partir da raiz, com a venv ativa e a stack no ar):
    python examples/smoke_ingest.py <API_KEY>
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
import uuid

from dotenv import load_dotenv

from trajetoria_logger.buffer import Buffer
from trajetoria_logger.transport import HttpTransport

load_dotenv()
API_BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000")


def _entries(execution_id: str, n: int = 3) -> list[dict]:
    return [
        {
            "log_id": str(uuid.uuid4()),
            "level": level,
            "message": f"smoke log {i} (level={level})",
            "timestamp": time.time(),
            "context": {"i": i},
            "execution_id": execution_id,
        }
        for i, level in enumerate([1, 2, 4][:n])  # critical, error, info
    ]


def run(base_url: str, api_key: str, titulo: str) -> None:
    print(f"\n=== {titulo} -> {base_url} ===")
    buf_path = os.path.join(tempfile.gettempdir(), "rpa_smoke_buffer.db")
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(buf_path + suffix):
            os.remove(buf_path + suffix)

    execution_id = str(uuid.uuid4())
    buffer = Buffer(buf_path)
    buffer.add_many(_entries(execution_id), execution_id)
    print(f"buffer antes do envio: {buffer.count()} logs pendentes")

    transport = HttpTransport(base_url, api_key, timeout=5.0)
    pending = buffer.get_pending(limit=10)
    result = transport.send_batch(execution_id, pending)

    print(f"  aceitos pela API: {len(result.accepted)}")
    if result.dropped:
        print(f"  rejeitados (permanente): {result.dropped}")
    if result.error:
        print(f"  erro: {result.error}")
    if result.license:
        print(f"  licença: active={result.license.active} lease={result.license.lease_seconds}s")

    # R1: só sai do buffer o que foi confirmado.
    buffer.mark_sent(set(result.accepted) | set(result.dropped))
    restantes = buffer.count()
    transport.close()
    buffer.close()
    print(f"buffer após o envio: {restantes} logs pendentes")


def main() -> None:
    api_key = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("RPA_API_KEY", "")
    if not api_key:
        print("uso: python examples/smoke_ingest.py <API_KEY>")
        raise SystemExit(2)

    run(API_BASE_URL, api_key, "1) caminho feliz (esperado: 3 aceitos, 0 pendentes)")
    run("http://127.0.0.1:9", api_key, "2) API inalcançável (esperado: 0 aceitos, 3 pendentes)")
    print("\nConfira os logs gravados:  GET /logs  (com o token de admin)")


if __name__ == "__main__":
    main()
