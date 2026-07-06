"""Cliente que o RPA usa (seção 5.5, revisada) + worker interno.

Fluxo (revisado):
  - No boot, valida a chave na API. Se a API responder EXPLICITAMENTE active=false, o
    construtor levanta RPALicenseError e o robô não inicia. Qualquer outro caso (ativa,
    inexistente, API fora) -> o robô inicia (fail-open).
  - Ao validar uma chave ativa, a API entrega a `rabbitmq_url`; a lib passa a publicar.
  - log() SEMPRE grava no buffer local primeiro (R1). O worker drena:
      * logs desta execução           -> fila padrão (rpa.log.<level>)
      * logs de execuções ANTERIORES  -> fila bulk   (rpa.bulk.<level>)
  - Kill switch cooperativo (seção 9): is_active + raise_if_inactive(), sem sys.exit().
"""
from __future__ import annotations

import threading
import time
import uuid

from .buffer import Buffer
from .config import RPALoggerConfig
from .license import LicenseValidator, RPALicenseError
from .publisher import Publisher

LEVEL_CRITICAL = 1
LEVEL_ERROR = 2
LEVEL_WARNING = 3
LEVEL_INFO = 4
LEVEL_DEBUG = 5


class RPALoggerClient:
    def __init__(self, config: RPALoggerConfig):
        self._config = config
        # Identifica ESTA execução: distingue os logs atuais (fila padrão) do backlog de
        # execuções anteriores que ficaram no buffer (fila bulk).
        self._execution_id = str(uuid.uuid4())

        self._buffer = Buffer(config.buffer_path)
        self._publisher = Publisher()  # a URL do Rabbit virá da API
        self._license = LicenseValidator(config.api_base_url, config.api_key)

        # Boot: só PARA se a API disser explicitamente que a chave está inativa.
        try:
            self._license.verify()  # pode levantar RPALicenseError (chave inativa)
        except Exception:
            self._buffer.close()
            raise
        self._robot_id = self._license.robot_id or config.api_key
        if self._license.rabbitmq_url:
            self._publisher.update_url(self._license.rabbitmq_url)

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
            "robot_id": self._robot_id,
            "level": int(level),
            "message": str(message),
            "timestamp": time.time(),
            "context": context or {},
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

    def raise_if_inactive(self) -> None:
        """Checkpoint cooperativo do kill switch: o RPA chama entre etapas."""
        if not self._active.is_set():
            raise RPALicenseError("Licença revogada — execução interrompida.")

    def close(self, drain_timeout: float = 10.0) -> None:
        """Tenta drenar o buffer antes de encerrar; depois para o worker."""
        deadline = time.time() + drain_timeout
        while time.time() < deadline and self._buffer.count() > 0:
            time.sleep(0.2)  # o worker (ainda vivo) continua drenando
        self._stop.set()
        self._worker.join(timeout=5.0)  # o worker fecha o publisher ao sair
        self._buffer.close()

    # ------------------------------------------------------------------ worker interno
    def _run_worker(self) -> None:
        backoff = 1.0
        interval = self._config.license_check_interval_minutes * 60.0
        last_license_check = time.time()
        try:
            while not self._stop.is_set():
                # Revalida a licença quando: (a) chegou o intervalo, ou (b) ainda não temos
                # a URL do Rabbit (precisamos obtê-la da API para conseguir publicar).
                if (not self._publisher.has_url()) or (
                    time.time() - last_license_check >= interval
                ):
                    last_license_check = time.time()
                    try:
                        self._license.verify()
                        self._publisher.update_url(self._license.rabbitmq_url)
                    except RPALicenseError:
                        self._active.clear()  # parada cooperativa (chave ficou inativa)

                pending = self._buffer.get_pending(limit=100)
                if not pending:
                    self._stop.wait(1.0)
                    continue

                progress = False
                for execution_id, entry in pending:
                    if self._stop.is_set():
                        break
                    # Logs de execuções anteriores (buffer sobreviveu a um run sem Rabbit)
                    # vão para a fila bulk; os desta execução vão para a fila padrão.
                    bulk = execution_id != self._execution_id
                    if self._publisher.publish(entry, bulk=bulk):
                        self._buffer.mark_sent(entry["log_id"])  # só após confirm (R1)
                        progress = True
                    else:
                        self._buffer.increment_attempts(entry["log_id"])

                if progress:
                    backoff = 1.0
                else:
                    # nada saiu (sem URL / broker fora): backoff exponencial.
                    self._stop.wait(backoff)
                    backoff = min(backoff * 2, 30.0)
        finally:
            self._publisher.close()
