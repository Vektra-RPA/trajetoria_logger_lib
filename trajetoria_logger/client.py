"""Cliente que o RPA usa + worker interno.

Fluxo:
  - No boot, valida a chave na API. Para (RPALicenseError) em DOIS casos: a API responde
    `active=false`, ou não há resposta E o período de graça já expirou (ver license.py).
  - `log()` SEMPRE grava no buffer local primeiro (R1) e retorna. Nenhuma I/O de rede
    acontece na thread do RPA.
  - O worker drena o buffer em BATCHES via `POST /logs/batch`. Um log só sai do buffer
    quando a API confirma aquele `log_id` (ou o rejeita permanentemente).
  - Kill switch cooperativo: `is_active` + `raise_if_inactive()`, sem `sys.exit()`.
  - `finish(status)` grava o status FINAL da execução no buffer (sem rede, como `log()`). O
    worker o envia por `POST /executions` só depois que todos os logs DAQUELA execução
    subiram, para que quem lê o status já encontre os logs completos.

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
from .timezones import resolve_timezone
from .transport import ExecutionResult, HttpTransport, LicenseSnapshot

LEVEL_CRITICAL = 1
LEVEL_ERROR = 2
LEVEL_WARNING = 3
LEVEL_INFO = 4
LEVEL_DEBUG = 5

# Status final de uma execução (`finish()`), no vocabulário da API.
STATUS_SUCCESS = "success"
STATUS_PARTIAL = "partial"
STATUS_ERROR = "error"
_STATUSES = (STATUS_SUCCESS, STATUS_PARTIAL, STATUS_ERROR)

# Teto de espera do `close()` para drenar logs + status. 5 min cobre algumas dezenas de
# milhares de logs acumulados no fim da execução; com tudo confirmado, retorna na hora.
DEFAULT_DRAIN_TIMEOUT = 300.0

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
        # Já houve `finish()` nesta execução? A PK do buffer também barra o segundo status;
        # a flag só evita ir ao disco para descobrir isso.
        self._finished = False

        # O `timestamp` do log continua epoch UNIX (é o que a API valida); o fuso abaixo
        # governa só como o buffer ESCREVE `created_at`. Ver `timezones.py`.
        self._buffer = Buffer(
            config.buffer_path,
            config.max_buffered_logs,
            tz=resolve_timezone(config.log_timezone, config.log_utc_offset_hours),
            utc_offset_hours=config.log_utc_offset_hours,
        )
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

    def finish(
        self, status: str, message: str | None = None, send_finished_at: bool = True
    ) -> None:
        """Registra o status FINAL desta execução: `success`, `partial` ou `error`.

        Como `log()`, só grava no buffer local e retorna — o envio é do worker, depois que
        os logs desta execução subirem. Se a API estiver fora, o status fica no buffer e
        sobe na próxima execução do robô, com o `finished_at` original (o instante desta
        chamada).

        `finished_at` é enviado AUTOMATICAMENTE (o instante desta chamada). Com
        `send_finished_at=False` ele não vai, e a API registra como finalização o instante em
        que RECEBEU o status — que, num reenvio a partir do buffer, pode ser bem depois.

        Um status por execução: uma segunda chamada é IGNORADA (vale o primeiro) e avisa por
        `on_warning`. Não levanta, porque costuma rodar num `finally` — derrubar o robô no
        encerramento por causa disso seria pior que o aviso.

        Status fora do vocabulário levanta `ValueError`: é erro de programação, não de
        execução, e precisa aparecer no primeiro teste.
        """
        if status not in _STATUSES:
            raise ValueError(f"status inválido: {status!r} (use {', '.join(_STATUSES)})")
        if self._finished:
            self._warn(f"finish({status!r}) ignorado: esta execução já tem status")
            return
        self._finished = True
        if message is not None:
            message = str(message)
        finished_at = time.time() if send_finished_at else None
        if not self._buffer.add_execution(self._execution_id, status, message, finished_at):
            self._warn(f"finish({status!r}) ignorado: esta execução já tem status")

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
    def pending_executions(self) -> int:
        """Status de execução ainda não confirmados (inclui os de execuções anteriores)."""
        return self._buffer.count_executions()

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

    def close(
        self,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
        status: str | None = None,
        message: str | None = None,
        send_finished_at: bool = True,
    ) -> int:
        """Tenta drenar o buffer (logs E status) antes de encerrar; depois para o worker.

        `drain_timeout` é o TETO de espera, não uma espera fixa: com tudo confirmado, o
        `close()` retorna na hora. O default (5 min) é para o robô que gera dezenas de
        milhares de logs no fim conseguir entregar tudo — inclusive o status, que só sobe
        depois dos logs. Com a API fora, porém, o encerramento espera o prazo inteiro; quem
        não pode esperar passa um valor menor (o que sobrar sobe na próxima execução).

        `status` é atalho para `finish(status, message, send_finished_at)` antes de drenar.
        Sem status nesta execução, nada é enviado — ausência de status é informação (o
        gerenciador vê "execução sem status") — e `on_warning` avisa.

        Devolve quantos logs continuam NÃO confirmados. Diferente de zero não é perda: eles
        ficam no arquivo do buffer e sobem na próxima execução do robô (fila bulk). O mesmo
        vale para um status não confirmado (`pending_executions`).
        """
        invalid: ValueError | None = None
        if status is not None:
            try:
                self.finish(status, message, send_finished_at)
            except ValueError as exc:
                # Status inválido não pode deixar worker e SQLite abertos: encerra tudo e
                # só então propaga o erro de programação.
                invalid = exc
        elif not self._finished:
            self._warn("execução encerrada sem status (chame finish() ou close(status=...))")
        deadline = time.time() + max(0.0, drain_timeout)
        # Com a chave desativada a API recusa tudo (403): esperar o prazo seria só travar o
        # encerramento de um robô que o kill switch acabou de parar.
        while (
            time.time() < deadline
            and self._active.is_set()
            and (self._buffer.count() > 0 or self._buffer.count_executions() > 0)
        ):
            time.sleep(0.2)  # o worker (ainda vivo) continua drenando
        remaining = self._buffer.count()
        self._stop.set()
        # O worker pode estar dentro de um POST: espera o suficiente para ele sair sozinho
        # antes de fechar o SQLite embaixo dele.
        self._worker.join(timeout=self._config.http_timeout_seconds + 5.0)
        self._transport.close()
        self._buffer.close()
        if invalid is not None:
            raise invalid
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
                    # Buffer de logs vazio: vez dos status. Falha aqui usa o mesmo backoff.
                    failure = self._flush_executions()
                    if failure is None:
                        self._stop.wait(self._config.flush_interval_seconds)
                    else:
                        self._stop.wait(failure.retry_after or backoff)
                        backoff = min(backoff * 2, _BACKOFF_MAX)
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
                    # Com backlog de outra execução ainda subindo, o status de uma execução
                    # já drenada não precisa esperar o buffer inteiro esvaziar.
                    self._flush_executions()
                    continue  # provavelmente há mais backlog: envia o próximo batch já

                self._buffer.increment_attempts([e["log_id"] for e in entries])
                if result.error:
                    self._warn(f"envio falhou (logs preservados no buffer): {result.error}")
                self._stop.wait(result.retry_after or backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)
        finally:
            self._transport.close()

    def _flush_executions(self) -> ExecutionResult | None:
        """Envia os status cujas execuções não têm mais log pendente no buffer.

        Logs primeiro: um status só sai quando todos os logs da MESMA execução já foram
        confirmados, para que quem lê o status (gerenciador, notificador) encontre os logs
        completos. Devolve a falha que interrompeu a rodada (para o backoff), ou None.
        """
        for entry in self._buffer.get_pending_executions():
            execution_id = entry["execution_id"]
            if self._buffer.pending_logs_for(execution_id):
                continue
            result = self._transport.send_execution(entry)
            self._apply_license(result.license)
            if result.accepted or result.dropped:
                self._buffer.mark_execution_sent(execution_id)  # só após resposta (R1)
                if result.dropped:
                    self._warn(f"status da execução {execution_id} descartado: {result.dropped}")
                continue
            self._buffer.increment_execution_attempts(execution_id)
            self._warn(f"envio do status falhou (preservado no buffer): {result.error}")
            return result
        return None
