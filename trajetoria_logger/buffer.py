"""Buffer local store-and-forward em SQLite (seção 5.2, revisada).

REGRA CRÍTICA (R1): um log só sai do buffer via `mark_sent`, e `mark_sent` só é chamado
APÓS a confirmação positiva do broker. Nunca antes.

O buffer é o "arquivo temporário" persistente: sobrevive entre execuções (o arquivo
`rpa_buffer.db` fica no disco). Cada log guarda o `execution_id` da execução que o gerou —
o worker usa isso para rotear logs de execuções ANTERIORES para a fila bulk.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

# DECISÃO: teto do buffer local. Acima disso, descartamos primeiro info/debug antigos,
# preservando crítico/erro (ver _enforce_cap_locked).
MAX_PENDING = 100_000


class Buffer:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS pending_logs (
                   log_id       TEXT PRIMARY KEY,
                   payload      TEXT NOT NULL,
                   created_at   REAL NOT NULL,
                   attempts     INTEGER NOT NULL DEFAULT 0,
                   execution_id TEXT
               )"""
        )
        # Migração idempotente para bancos criados antes da coluna execution_id.
        cols = [r[1] for r in self._conn.execute("PRAGMA table_info(pending_logs)").fetchall()]
        if "execution_id" not in cols:
            self._conn.execute("ALTER TABLE pending_logs ADD COLUMN execution_id TEXT")
        self._conn.commit()

    def add(self, log_dict: dict, execution_id: str) -> None:
        """Grava o log ANTES de qualquer tentativa de envio (R1)."""
        payload = json.dumps(log_dict)
        created_at = float(log_dict.get("timestamp") or time.time())
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO pending_logs "
                "(log_id, payload, created_at, attempts, execution_id) VALUES (?, ?, ?, 0, ?)",
                (log_dict["log_id"], payload, created_at, execution_id),
            )
            self._conn.commit()
            self._enforce_cap_locked()

    def get_pending(self, limit: int = 100) -> list[tuple[str | None, dict]]:
        """Retorna [(execution_id, log_dict)] ainda não confirmados (mais antigos primeiro)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT execution_id, payload FROM pending_logs ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [(r[0], json.loads(r[1])) for r in rows]

    def mark_sent(self, log_id: str) -> None:
        """Remove do buffer. CRÍTICO (R1): só após confirm positivo do broker."""
        with self._lock:
            self._conn.execute("DELETE FROM pending_logs WHERE log_id = ?", (log_id,))
            self._conn.commit()

    def increment_attempts(self, log_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE pending_logs SET attempts = attempts + 1 WHERE log_id = ?",
                (log_id,),
            )
            self._conn.commit()

    def count(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM pending_logs").fetchone()[0]

    def _enforce_cap_locked(self) -> None:
        total = self._conn.execute("SELECT COUNT(*) FROM pending_logs").fetchone()[0]
        if total <= MAX_PENDING:
            return
        excess = total - MAX_PENDING
        # DECISÃO: descartar primeiro os mais antigos de level >= 4 (info/debug),
        # preservando crítico/erro. json_extract lê o nível de dentro do payload.
        deleted = self._conn.execute(
            """DELETE FROM pending_logs WHERE log_id IN (
                   SELECT log_id FROM pending_logs
                   WHERE CAST(json_extract(payload, '$.level') AS INTEGER) >= 4
                   ORDER BY created_at ASC LIMIT ?
               )""",
            (excess,),
        ).rowcount
        remaining = excess - (deleted or 0)
        if remaining > 0:
            self._conn.execute(
                """DELETE FROM pending_logs WHERE log_id IN (
                       SELECT log_id FROM pending_logs ORDER BY created_at ASC LIMIT ?
                   )""",
                (remaining,),
            )
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
