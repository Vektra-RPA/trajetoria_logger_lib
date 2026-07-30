"""Cliente que o RPA usa + worker interno.

Fluxo:
  - No boot, valida a chave na API. Para (RPALicenseError) em DOIS casos: a API responde
    `active=false`, ou não há resposta E o período de graça já expirou (ver license.py).
  - `log()` SEMPRE grava no buffer local primeiro (R1) e retorna. Nenhuma I/O de rede
    acontece na thread do RPA.
  - O worker drena o buffer em BATCHES via `POST /logs/batch`. Um log só sai do buffer
    quando a API confirma aquele `log_id` (ou o rejeita permanentemente).
  - Kill switch cooperativo: `is_active` + `raise_if_inactive()`, sem `sys.exit()`.

A lib não conhece mais o RabbitMQ: quem publica no broker é a API. O `robot_id` também
deixou de ser enviado — o servidor o deriva da chave, então não é forjável pelo cliente.
"""
from __future__ import annotations

import threading
import time
import uuid

from .buffer import Buffer
from .config import RPALoggerConfig
from .license import LicenseGuard, RPALicenseError
from .transport import HttpTransport, LicenseSnapshot

LEVEL_CRITICAL = 1
LEVEL_ERROR = 2
LEVEL_WARNING = 3
LEVEL_INFO = 4
LEVEL_DEBUG = 5

_BACKOFF_START = 1.0
_BACKOFF_MAX = 30.0
# Piso entre chamadas de licença: protege a API de ser martelada se o intervalo
# configurado for absurdamente baixo (ou zero).
_MIN_LICENSE_INTERVAL = 5.0


class RPALoggerClient:
    def __init__(self, config: RPALoggerConfig):
        self._config = config
        # Identifica ESTA execução: o servidor usa isso para separar o stream em tempo
        # real do backlog de execuções anteriores que ficaram no buffer.
        self._execution_id = str(uuid.uuid4())

        self._buffer = Buffer(config.buffer_path, config.max_buffered_logs)
        self._transport = HttpTransport(
            config.api_base_url,
            config.api_key,
            timeout=config.http_timeout_seconds,
            verify_tls=config.verify_tls,
        )
        self._license = LicenseGuard(self._buffer, config.license_grace_hours)

        self._license_interval = max(
            _MIN_LICENSE_INTERVAL, float(config.license_check_interval_minutes) * 60.0
        )
        self._next_license_check = 0.0

        # Boot: valida antes de qualquer coisa. Se tiver que parar, não deixa recurso aberto.
        try:
            self._license.record(self._transport.fetch_license())
            self._license.raise_if_stopped()
        except BaseException:
            self._transport.close()
            self._buffer.close()
            raise

        self._active = threading.Event()
        self._active.set()
        self._stop = threading.Event()
        self._worker = threading.Thread(
            target=self._run_worker, name="rpa-logger-worker", daemon=True
        )
        self._worker.start()

    # ------------------------------------------------------------------ API pública
    def log(self, message: str, level: int = LEVEL_INFO, **context) -> None:
        entry = {
            "log_id": str(uuid.uuid4()),
            "level": int(level),
            "message": str(message),
            "timestamp": time.time(),
            "context": context or {},
            "execution_id": self._execution_id,
        }
        self._buffer.add(entry, self._execution_id)  # SEMPRE grava local primeiro (R1)

    def critical(self, message: str, **context) -> None:
        self.log(message, LEVEL_CRITICAL, **context)

    def error(self, message: str, **context) -> None:
        self.log(message, LEVEL_ERROR, **context)

    def warning(self, message: str, **context) -> None:
        self.log(message, LEVEL_WARNING, **context)

    def info(self, message: str, **context) -> None:
        self.log(message, LEVEL_INFO, **context)

    def debug(self, message: str, **context) -> None:
        self.log(message, LEVEL_DEBUG, **context)

    @property
    def is_active(self) -> bool:
        return self._active.is_set()

    @property
    def robot_id(self) -> str | None:
        """Quem o servidor diz que somos (só conhecido após uma validação bem-sucedida)."""
        return self._license.robot_id

    @property
    def execution_id(self) -> str:
        return self._execution_id

    @property
    def pending_logs(self) -> int:
        """Logs ainda não confirmados pela API (inclui backlog de execuções anteriores)."""
        return self._buffer.count()

    def raise_if_inactive(self) -> None:
        """Checkpoint cooperativo do kill switch: o RPA chama entre etapas."""
        if not self._active.is_set():
            self._license.raise_if_stopped()
            # Estado inconsistente (flag caiu mas a política já não pede parada):
            # a mensagem genérica preserva o contrato de interromper a execução.
            raise RPALicenseError("Licença revogada — execução interrompida.")

    def close(self, drain_timeout: float = 10.0) -> int:
        """Tenta drenar o buffer antes de encerrar; depois para o worker.

        Devolve quantos logs continuam NÃO confirmados. Diferente de zero não é perda: eles
        ficam no arquivo do buffer e sobem na próxima execução do robô (fila bulk).
        """
        deadline = time.time() + max(0.0, drain_timeout)
        while time.time() < deadline and self._buffer.count() > 0:
            time.sleep(0.2)  # o worker (ainda vivo) continua drenando
        remaining = self._buffer.count()
        self._stop.set()
        # O worker pode estar dentro de um POST: espera o suficiente para ele sair sozinho
        # antes de fechar o SQLite embaixo dele.
        self._worker.join(timeout=self._config.http_timeout_seconds + 5.0)
        self._transport.close()
        self._buffer.close()
        return remaining

    # ------------------------------------------------------------------ worker interno
    def _warn(self, message: str) -> None:
        hook = self._config.on_warning
        if hook is None:
            return
        try:
            hook(message)
        except Exception:
            pass  # o hook é da aplicação: um erro nele não pode derrubar o worker

    def _apply_license(self, snapshot: LicenseSnapshot | None) -> None:
        if snapshot is None:
            return
        self._license.record(snapshot)
        # Resposta explícita (inclusive a que vem "de carona" no POST) conta como
        # verificação: adia o GET periódico.
        self._next_license_check = time.time() + self._license_interval
        reason = self._license.stop_reason()
        if reason:
            self._active.clear()  # parada cooperativa; quem decide sair é o RPA
            self._warn(reason)

    def _check_license_deadline(self) -> None:
        """Sem resposta há tempo demais -> a graça expira e a execução deve parar."""
        reason = self._license.stop_reason()
        if reason and self._active.is_set():
            self._active.clear()
            self._warn(reason)

    def _run_worker(self) -> None:
        backoff = _BACKOFF_START
        self._next_license_check = time.time() + self._license_interval
        try:
            while not self._stop.is_set():
                if time.time() >= self._next_license_check:
                    self._next_license_check = time.time() + self._license_interval
                    self._apply_license(self._transport.fetch_license())
                self._check_license_deadline()

                entries = self._buffer.get_pending(limit=self._config.batch_size)
                if not entries:
                    self._stop.wait(self._config.flush_interval_seconds)
                    continue

                result = self._transport.send_batch(self._execution_id, entries)
                self._apply_license(result.license)

                settled = set(result.accepted) | set(result.dropped)
                if settled:
                    self._buffer.mark_sent(settled)  # só após confirmação (R1)
                for log_id, reason in result.dropped.items():
                    self._warn(f"log {log_id} descartado pela API: {reason}")

                if result.accepted:
                    backoff = _BACKOFF_START
                    continue  # provavelmente há mais backlog: envia o próximo batch já

                self._buffer.increment_attempts([e["log_id"] for e in entries])
                if result.error:
                    self._warn(f"envio falhou (logs preservados no buffer): {result.error}")
                self._stop.wait(result.retry_after or backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)
        finally:
            self._transport.close()
