"""Buffer local store-and-forward em SQLite.

REGRA CRÍTICA (R1): um log só sai do buffer via `mark_sent`, e `mark_sent` só é chamado
APÓS a API confirmar que aceitou aquele `log_id`. Nunca antes.

O buffer é o "arquivo temporário" persistente: sobrevive entre execuções (o arquivo
`rpa_buffer.db` fica no disco). Cada log guarda o `execution_id` da execução que o gerou —
o servidor usa isso para separar o stream em tempo real do backlog (fila bulk).

Também guarda o estado do kill switch (tabela `meta`): o `last_ok` da licença precisa
sobreviver a reinícios, senão cada restart do robô zeraria o período de graça.

FUSO: `created_at` é TEXTO ISO 8601 no fuso local, com o offset dentro do valor
(`2026-08-03T08:58:12.345678-03:00`), e não epoch. Epoch era ilegível para quem abre o
arquivo e, lido como UTC, mostrava um log das 08:58 como 11:58. A ordenação não se perde:
neste formato de largura fixa a ordem lexicográfica É a cronológica, então `ORDER BY
created_at`, o índice e o corte por excedente seguem valendo. O `timestamp` do payload
continua epoch — é o que a API valida. Ver `timezones.py`.

DECISÕES de desempenho (o volume real chega a dezenas de milhares de logs por etapa):
  * WAL + synchronous=NORMAL: `log()` é chamado no caminho quente do RPA e commita a cada
    chamada. Em WAL/NORMAL o commit não paga fsync, e os dados sobrevivem a crash do
    PROCESSO (o cenário real: exceção, kill, robô fechado). Perda só ocorreria em queda de
    energia/SO — risco aceito em troca de ordens de magnitude de throughput.
  * A contagem de pendentes é mantida em memória; sem isso seria um COUNT(*) por log.
  * Índice em `created_at`: o worker ordena por ele a cada ciclo.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import tzinfo
from typing import Iterable

from .timezones import (
    DEFAULT_UTC_OFFSET_HOURS,
    local_iso,
    resolve_timezone,
    sqlite_local_iso,
)


class Buffer:
    def __init__(
        self,
        path: str,
        max_pending: int = 100_000,
        tz: tzinfo | None = None,
        utc_offset_hours: float = DEFAULT_UTC_OFFSET_HOURS,
    ):
        self._path = path
        self._max_pending = int(max_pending)
        # Fuso em que `created_at` é escrito. Default -03:00 (ver timezones.py).
        self._tz = tz if tz is not None else resolve_timezone()
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        # O DEFAULT é rede de segurança para INSERT que não informe `created_at`; no caminho
        # normal quem formata é o Python, a partir do `timestamp` do próprio log (mais fiel:
        # o instante do log, não o instante do INSERT).
        self._conn.execute(
            f"""CREATE TABLE IF NOT EXISTS pending_logs (
                   log_id       TEXT PRIMARY KEY,
                   payload      TEXT NOT NULL,
                   created_at   TEXT NOT NULL
                                DEFAULT ({sqlite_local_iso("'now'", utc_offset_hours)}),
                   attempts     INTEGER NOT NULL DEFAULT 0,
                   execution_id TEXT
               )"""
        )
        # Migrações idempotentes de bancos antigos. `execution_id` vem primeiro porque a
        # conversão de `created_at` copia a tabela e precisa da coluna já existindo.
        cols = {
            r[1]: (r[2] or "").upper()
            for r in self._conn.execute("PRAGMA table_info(pending_logs)").fetchall()
        }
        if "execution_id" not in cols:
            self._conn.execute("ALTER TABLE pending_logs ADD COLUMN execution_id TEXT")
        if cols.get("created_at") != "TEXT":
            self._migrate_created_at_to_local(utc_offset_hours)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_created_at ON pending_logs (created_at)"
        )
        # O worker conta os logs pendentes de UMA execução antes de enviar o status dela.
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_execution ON pending_logs (execution_id)"
        )
        # Estado que precisa sobreviver a reinícios (hoje: last_ok/lease do kill switch).
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        # Status final das execuções (`finish()`), até a API confirmar. A PK em
        # `execution_id` é o que garante UM status por execução: um segundo `finish()` cai
        # no INSERT OR IGNORE e não gera envio nenhum. `finished_at` é epoch, como o
        # `timestamp` dos logs — é o instante que trafega; NULL quando o desenvolvedor optou
        # por não enviá-lo (a API usa então o instante em que recebeu). `created_at` é o
        # instante do `finish()` para leitura humana e ordenação, sempre preenchido.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS pending_executions (
                   execution_id TEXT PRIMARY KEY,
                   status       TEXT NOT NULL,
                   message      TEXT,
                   finished_at  REAL,
                   created_at   TEXT NOT NULL,
                   attempts     INTEGER NOT NULL DEFAULT 0
               )"""
        )
        self._conn.commit()
        self._pending = self._conn.execute("SELECT COUNT(*) FROM pending_logs").fetchone()[0]

    # ------------------------------------------------------------------ gravação
    def add(self, log_dict: dict, execution_id: str) -> None:
        """Grava o log ANTES de qualquer tentativa de envio (R1)."""
        self.add_many([log_dict], execution_id)

    def add_many(self, log_dicts: Iterable[dict], execution_id: str) -> None:
        rows = []
        for d in log_dicts:
            # `created_at` sai do epoch do PRÓPRIO log (não da hora do INSERT): assim um
            # backlog reinserido mantém o instante em que o log de fato aconteceu.
            epoch = float(d.get("timestamp") or time.time())
            rows.append(
                (
                    d["log_id"],
                    json.dumps(d, ensure_ascii=False),
                    local_iso(epoch, self._tz),
                    execution_id,
                )
            )
        if not rows:
            return
        with self._lock:
            cur = self._conn.executemany(
                "INSERT OR IGNORE INTO pending_logs "
                "(log_id, payload, created_at, attempts, execution_id) VALUES (?, ?, ?, 0, ?)",
                rows,
            )
            self._conn.commit()
            self._pending += _changes(cur.rowcount, len(rows))
            if self._pending > self._max_pending:
                self._enforce_cap_locked()

    # ------------------------------------------------------------------ leitura
    def get_pending(self, limit: int = 500) -> list[dict]:
        """Logs ainda não confirmados, mais antigos primeiro.

        Cada dict sai com `execution_id` garantido: bancos antigos guardavam esse campo
        só na coluna, então ele é injetado no payload quando faltar.

        Auto-limpeza: linha ilegível (corrupção de disco) é REMOVIDA aqui. Sem isso ela
        ficaria eternamente no topo da ordenação, ocupando a janela do LIMIT sem nunca ser
        enviada — a drenagem do buffer travaria com logs válidos atrás dela.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT log_id, execution_id, payload FROM pending_logs "
                "ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
            out: list[dict] = []
            corrupt: list[tuple[str]] = []
            for log_id, execution_id, payload in rows:
                entry = _parse_payload(payload)
                if entry is None:
                    corrupt.append((log_id,))
                    continue
                entry.setdefault("execution_id", execution_id)
                out.append(entry)
            if corrupt:
                self._conn.executemany("DELETE FROM pending_logs WHERE log_id = ?", corrupt)
                self._conn.commit()
                self._pending = max(0, self._pending - len(corrupt))
        return out

    def count(self) -> int:
        with self._lock:
            return self._pending

    # ------------------------------------------------------------------ confirmação
    def mark_sent(self, log_ids: Iterable[str]) -> None:
        """Remove do buffer. CRÍTICO (R1): só após a API confirmar cada log_id."""
        ids = [(i,) for i in log_ids]
        if not ids:
            return
        with self._lock:
            cur = self._conn.executemany("DELETE FROM pending_logs WHERE log_id = ?", ids)
            self._conn.commit()
            self._pending = max(0, self._pending - _changes(cur.rowcount, len(ids)))

    def increment_attempts(self, log_ids: Iterable[str]) -> None:
        """Contador de tentativas — diagnóstico (`attempts` é lido por quem investiga)."""
        ids = [(i,) for i in log_ids]
        if not ids:
            return
        with self._lock:
            self._conn.executemany(
                "UPDATE pending_logs SET attempts = attempts + 1 WHERE log_id = ?", ids
            )
            self._conn.commit()

    def pending_logs_for(self, execution_id: str) -> int:
        """Logs ainda não confirmados de UMA execução (o status espera por eles)."""
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM pending_logs WHERE execution_id = ?", (execution_id,)
            ).fetchone()[0]

    # ------------------------------------------------------------------ status de execução
    def add_execution(
        self, execution_id: str, status: str, message: str | None, finished_at: float | None
    ) -> bool:
        """Grava o status final ANTES de qualquer rede, como os logs (R1).

        Devolve False se esta execução já tinha status: o primeiro vence e nada é enviado
        de novo — a API também deduplica, mas quem garante envio único é a lib.
        """
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO pending_executions "
                "(execution_id, status, message, finished_at, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    execution_id,
                    status,
                    message,
                    None if finished_at is None else float(finished_at),
                    local_iso(time.time() if finished_at is None else finished_at, self._tz),
                ),
            )
            self._conn.commit()
            return _changes(cur.rowcount, 1) > 0

    def get_pending_executions(self) -> list[dict]:
        """Status ainda não confirmados, mais antigos primeiro (inclui runs anteriores)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT execution_id, status, message, finished_at FROM pending_executions "
                "ORDER BY created_at ASC"  # largura fixa: ordem lexicográfica = cronológica
            ).fetchall()
        return [
            {"execution_id": e, "status": s, "message": m, "finished_at": f}
            for e, s, m, f in rows
        ]

    def count_executions(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM pending_executions").fetchone()[0]

    def mark_execution_sent(self, execution_id: str) -> None:
        """Remove do buffer. CRÍTICO (R1): só após a API confirmar (202) ou rejeitar (422)."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM pending_executions WHERE execution_id = ?", (execution_id,)
            )
            self._conn.commit()

    def increment_execution_attempts(self, execution_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE pending_executions SET attempts = attempts + 1 WHERE execution_id = ?",
                (execution_id,),
            )
            self._conn.commit()

    # ------------------------------------------------------------------ meta (kill switch)
    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
            self._conn.commit()

    # ------------------------------------------------------------------ interno
    def _migrate_created_at_to_local(self, utc_offset_hours: float) -> None:
        """Converte `created_at` de epoch (REAL) para texto local, em banco já existente.

        O SQLite não muda o tipo declarado de uma coluna, então a tabela é RECRIADA e as
        linhas copiadas com o epoch convertido — mesmo instante, escrita nova. Sem isso a
        coluna ficaria com números e textos misturados, e o SQLite ordena todo número antes
        de todo texto: o backlog antigo passaria a furar a fila indefinidamente.

        Roda uma vez por arquivo, e só sobre o que ainda está pendente (o que já subiu foi
        apagado), então é uma cópia curta. `DROP TABLE` leva os índices junto — quem chama
        recria o índice logo depois.
        """
        converted = sqlite_local_iso("created_at", utc_offset_hours, from_epoch=True)
        self._conn.executescript(
            f"""
            BEGIN;
            CREATE TABLE pending_logs_migrado (
                log_id       TEXT PRIMARY KEY,
                payload      TEXT NOT NULL,
                created_at   TEXT NOT NULL
                             DEFAULT ({sqlite_local_iso("'now'", utc_offset_hours)}),
                attempts     INTEGER NOT NULL DEFAULT 0,
                execution_id TEXT
            );
            INSERT INTO pending_logs_migrado
                   (log_id, payload, created_at, attempts, execution_id)
            SELECT log_id, payload, {converted}, attempts, execution_id FROM pending_logs;
            DROP TABLE pending_logs;
            ALTER TABLE pending_logs_migrado RENAME TO pending_logs;
            COMMIT;
            """
        )

    def _enforce_cap_locked(self) -> None:
        excess = self._pending - self._max_pending
        if excess <= 0:
            return
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
        removed = deleted if deleted and deleted > 0 else 0
        remaining = excess - removed
        if remaining > 0:
            extra = self._conn.execute(
                """DELETE FROM pending_logs WHERE log_id IN (
                       SELECT log_id FROM pending_logs ORDER BY created_at ASC LIMIT ?
                   )""",
                (remaining,),
            ).rowcount
            removed += extra if extra and extra > 0 else 0
        self._conn.commit()
        self._pending = max(0, self._pending - removed)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def _changes(rowcount: int | None, fallback: int) -> int:
    """Nº de linhas afetadas. sqlite3 devolve -1/None quando não sabe -> usa o fallback."""
    if rowcount is None or rowcount < 0:
        return fallback
    return rowcount


def _parse_payload(payload: str) -> dict | None:
    """dict do log, ou None se a linha não for um log utilizável."""
    try:
        entry = json.loads(payload)
    except (ValueError, TypeError):
        return None
    if not isinstance(entry, dict) or "log_id" not in entry:
        return None
    return entry
