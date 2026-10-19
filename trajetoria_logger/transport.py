"""Transporte HTTP: o ÚNICO canal de saída da lib (substitui o publish direto no RabbitMQ).

Por que HTTP e não AMQP a partir do cliente:
  * o cliente deixa de receber credencial do broker (antes a API devolvia a `RABBITMQ_URL`
    completa, com usuário e senha, para qualquer chave ativa);
  * o `robot_id` passa a ser derivado no servidor a partir da chave — não é mais um campo
    do payload que o cliente poderia forjar;
  * não é preciso expor a porta AMQP na internet (TCP puro não passa por proxy HTTP);
  * a lib perde a dependência de `pika` — sobra `requests`.

R1 continua garantido ponta a ponta: a API só responde `accepted` para os `log_id` que o
broker CONFIRMOU ter persistido. Tudo que não aparecer em `accepted` fica no buffer local.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import requests

# Contrato: a chave do robô vai em header próprio, separado do `Authorization: Bearer`
# usado pelo token de ADMIN — para que os dois nunca sejam confundidos no servidor.
API_KEY_HEADER = "X-API-Key"


@dataclass(frozen=True)
class LicenseSnapshot:
    """Resposta EXPLÍCITA do servidor sobre a chave. Ausência de resposta é `None`, nunca
    um snapshot com active=False — confundir os dois derrubaria cliente pagante."""

    active: bool
    robot_id: str | None = None
    lease_seconds: float | None = None


@dataclass
class BatchResult:
    accepted: set[str] = field(default_factory=set)
    # log_id -> motivo. Rejeição PERMANENTE (o servidor nunca vai aceitar este log):
    # sai do buffer, senão ficaria em reenvio eterno bloqueando o resto.
    dropped: dict[str, str] = field(default_factory=dict)
    license: LicenseSnapshot | None = None
    retry_after: float | None = None
    error: str | None = None


@dataclass
class ExecutionResult:
    accepted: bool = False
    # Motivo da rejeição PERMANENTE (422): sai do buffer, senão travaria em reenvio eterno.
    dropped: str | None = None
    license: LicenseSnapshot | None = None
    retry_after: float | None = None
    error: str | None = None


class HttpTransport:
    """Fala com a nossa API. Mantém a sessão aberta (keep-alive) entre batches."""

    def __init__(
        self,
        api_base_url: str,
        api_key: str,
        timeout: float = 15.0,
        verify_tls: bool = True,
    ):
        self._base = api_base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = float(timeout)
        self._session = requests.Session()
        self._session.verify = bool(verify_tls)
        self._session.headers.update(
            {API_KEY_HEADER: api_key, "Content-Type": "application/json"}
        )

    # ------------------------------------------------------------------ licença (R2)
    def fetch_license(self) -> LicenseSnapshot | None:
        """`GET /keys/{key}/status`.

        Retorna:
          * LicenseSnapshot(active=True/False) -> resposta EXPLÍCITA do servidor;
          * None -> não houve resposta útil (rede fora, 404, 5xx). Quem chama trata isso
            como "não sei" e aplica o período de graça — nunca como "inativa".
        """
        try:
            resp = self._session.get(
                f"{self._base}/keys/{self._api_key}/status", timeout=self._timeout
            )
        except requests.RequestException:
            return None
        if resp.status_code != 200:
            # 404 (chave rotacionada/removida) e 5xx não são "inativa": entram na graça.
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        return _snapshot_from(data)

    # ------------------------------------------------------------------ envio (R1)
    def send_batch(self, execution_id: str, entries: list[dict]) -> BatchResult:
        """`POST /logs/batch`. Só marca como aceito o que o servidor confirmar."""
        if not entries:
            return BatchResult()
        body = json.dumps(
            {"execution_id": execution_id, "logs": entries}, ensure_ascii=False
        ).encode("utf-8")
        try:
            resp = self._session.post(
                f"{self._base}/logs/batch", data=body, timeout=self._timeout
            )
        except requests.RequestException as exc:
            # Sem resposta: NADA sai do buffer. Inclui timeout depois do servidor ter
            # gravado — o reenvio é inofensivo porque o log_id deduplica no servidor.
            return BatchResult(error=f"{type(exc).__name__}: {exc}")

        if resp.status_code == 403:
            # Kill switch explícito: a chave está desativada.
            return BatchResult(license=LicenseSnapshot(active=False), error="chave desativada")
        if resp.status_code == 401:
            # Chave desconhecida (rotacionada?). Não é "inativa" -> graça, sem parar.
            return BatchResult(error="chave não reconhecida pela API")
        if resp.status_code in (429, 503):
            return BatchResult(
                retry_after=_retry_after(resp), error=f"API ocupada/indisponível ({resp.status_code})"
            )
        if resp.status_code >= 400:
            return BatchResult(error=f"HTTP {resp.status_code}: {resp.text[:200]}")

        try:
            data = resp.json()
        except ValueError:
            return BatchResult(error="resposta da API não é JSON")

        accepted = {str(i) for i in (data.get("accepted") or [])}
        dropped = {
            str(r.get("log_id")): str(r.get("reason") or "rejeitado")
            for r in (data.get("rejected") or [])
            if isinstance(r, dict) and r.get("log_id")
        }
        return BatchResult(
            accepted=accepted,
            dropped=dropped,
            license=_snapshot_from(data.get("license") or {}),
        )

    # ------------------------------------------------------------------ status de execução
    def send_execution(self, entry: dict) -> ExecutionResult:
        """`POST /executions`. Mesma tradução de códigos de `send_batch`, para um item só."""
        body = json.dumps(entry, ensure_ascii=False).encode("utf-8")
        try:
            resp = self._session.post(
                f"{self._base}/executions", data=body, timeout=self._timeout
            )
        except requests.RequestException as exc:
            # Sem resposta: fica no buffer. Reenviar é seguro — a API deduplica por execução.
            return ExecutionResult(error=f"{type(exc).__name__}: {exc}")

        if resp.status_code == 403:
            return ExecutionResult(license=LicenseSnapshot(active=False), error="chave desativada")
        if resp.status_code == 401:
            return ExecutionResult(error="chave não reconhecida pela API")
        if resp.status_code == 422:
            # Payload que a API nunca vai aceitar: reenviar não resolve, descarta.
            return ExecutionResult(dropped=f"rejeitado pela API: {resp.text[:200]}")
        if resp.status_code in (429, 503):
            return ExecutionResult(
                retry_after=_retry_after(resp), error=f"API ocupada/indisponível ({resp.status_code})"
            )
        if resp.status_code >= 400:
            return ExecutionResult(error=f"HTTP {resp.status_code}: {resp.text[:200]}")

        try:
            data = resp.json()
        except ValueError:
            data = {}
        return ExecutionResult(accepted=True, license=_snapshot_from(data.get("license") or {}))

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass


def _snapshot_from(data: dict) -> LicenseSnapshot | None:
    active = data.get("active")
    if not isinstance(active, bool):
        return None
    lease = data.get("lease_seconds")
    return LicenseSnapshot(
        active=active,
        robot_id=data.get("robot_id"),
        lease_seconds=float(lease) if isinstance(lease, (int, float)) else None,
    )


def _retry_after(resp: requests.Response) -> float | None:
    try:
        return max(0.0, float(resp.headers.get("Retry-After", "")))
    except (TypeError, ValueError):
        return None
