"""Kill switch (R2): fail-open COM VALIDADE.

Esta classe é POLÍTICA PURA — não faz HTTP (isso é do `transport.py`). Ela recebe
snapshots do servidor e decide uma única coisa: a execução deve parar?

A distinção que não pode ser relaxada:
  * `active=False` (resposta EXPLÍCITA)  -> para na hora. Cliente cancelou.
  * SEM resposta (rede fora, 404, 5xx)   -> NÃO para de imediato. Pode ser só instabilidade
                                            de rede, e derrubar cliente pagante por isso é
                                            o pior erro possível aqui.

O que a versão anterior não tinha: o fail-open era INFINITO — API fora para sempre
significava robô rodando para sempre. Agora existe um "passe com validade":

    para  <=>  active=False  OU  (agora - último_ok) > graça

A graça vem do servidor (`lease_seconds`, que assim controla a política comercialmente) e
cai para `license_grace_hours` do config enquanto o servidor nunca respondeu.

`last_ok` é PERSISTIDO no buffer (tabela `meta`): sem isso, reiniciar o robô zeraria o
prazo e o fail-open voltaria a ser infinito na prática.

Robô que NUNCA validou (primeira execução, API inacessível) roda ancorado no `first_seen`
— ou seja, ele inicia, mas não passa da graça sem nunca ter falado com a API.
"""
from __future__ import annotations

import threading
import time

from .buffer import Buffer
from .transport import LicenseSnapshot

_META_LAST_OK = "license_last_ok"
_META_LEASE = "license_lease_seconds"
_META_FIRST_SEEN = "license_first_seen"


class RPALicenseError(Exception):
    """Levantada quando a execução do RPA deve ser interrompida."""


class LicenseGuard:
    def __init__(self, buffer: Buffer, grace_hours: float, robot_id_fallback: str = ""):
        self._buffer = buffer
        self._lock = threading.Lock()
        self._default_grace = max(0.0, float(grace_hours) * 3600.0)
        self._robot_id = robot_id_fallback or None
        # None = ainda não houve resposta explícita nesta execução.
        self._active: bool | None = None

        self._last_ok = _read_float(buffer, _META_LAST_OK)
        self._lease = _read_float(buffer, _META_LEASE)
        first_seen = _read_float(buffer, _META_FIRST_SEEN)
        if first_seen is None:
            first_seen = time.time()
            buffer.set_meta(_META_FIRST_SEEN, repr(first_seen))
        self._first_seen = first_seen

    # ------------------------------------------------------------------ leitura
    @property
    def robot_id(self) -> str | None:
        with self._lock:
            return self._robot_id

    @property
    def last_ok(self) -> float | None:
        with self._lock:
            return self._last_ok

    def seconds_left(self) -> float:
        """Quanto tempo ainda pode rodar sem confirmação da API (inf se confirmada agora)."""
        with self._lock:
            return self._seconds_left_locked()

    # ------------------------------------------------------------------ escrita
    def record(self, snapshot: LicenseSnapshot | None) -> None:
        """Aplica o que o servidor disse. `None` = não disse nada: estado preservado."""
        if snapshot is None:
            return
        with self._lock:
            if snapshot.robot_id:
                self._robot_id = snapshot.robot_id
            if not snapshot.active:
                self._active = False  # resposta explícita: decisão final
                return
            self._active = True
            self._last_ok = time.time()
            if snapshot.lease_seconds is not None:
                self._lease = float(snapshot.lease_seconds)
                self._buffer.set_meta(_META_LEASE, repr(self._lease))
            self._buffer.set_meta(_META_LAST_OK, repr(self._last_ok))

    # ------------------------------------------------------------------ decisão
    def stop_reason(self) -> str | None:
        """Motivo para parar, ou None para continuar."""
        with self._lock:
            if self._active is False:
                return "Chave desativada — execução interrompida."
            if self._seconds_left_locked() <= 0:
                grace_h = self._grace_locked() / 3600.0
                return (
                    "Não foi possível validar a licença dentro do período de graça "
                    f"({grace_h:.1f}h) — execução interrompida."
                )
            return None

    def raise_if_stopped(self) -> None:
        reason = self.stop_reason()
        if reason:
            raise RPALicenseError(reason)

    # ------------------------------------------------------------------ interno
    def _grace_locked(self) -> float:
        return self._lease if self._lease is not None else self._default_grace

    def _seconds_left_locked(self) -> float:
        anchor = self._last_ok if self._last_ok is not None else self._first_seen
        return self._grace_locked() - (time.time() - anchor)


def _read_float(buffer: Buffer, key: str) -> float | None:
    raw = buffer.get_meta(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
