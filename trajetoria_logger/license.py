"""Kill switch (seção 5.4 do spec, REVISADO). Requisito R2.

Regra do kill switch (revisada com o usuário) — bem mais simples e leniente:

  * 200 active=true   -> OK. Guarda robot_id, rabbitmq_url (entregue pela API) e last_ok.
  * 200 active=false  -> RPALicenseError. É a ÚNICA condição de parada (chave inativa).
  * 404 (inexistente) -> NÃO para. A chave pode ter sido alterada/rotacionada; não
                         derrubamos a execução do robô por isso.
  * erro de rede/timeout/5xx -> NÃO para (fail-open). Sem resposta explícita de "inativa",
                         o robô continua; o log degrada para o buffer local e é enviado
                         depois (fila bulk) quando a conexão voltar.

Ou seja: só uma resposta EXPLÍCITA de "inativa" interrompe o robô. Tudo mais → continua.
"""
from __future__ import annotations

import threading
import time

import requests


class RPALicenseError(Exception):
    """Levantada quando a execução do RPA deve ser interrompida (chave inativa)."""


class LicenseValidator:
    def __init__(self, api_base_url: str, api_key: str):
        self._url = f"{api_base_url.rstrip('/')}/keys/{api_key}/status"
        self._lock = threading.Lock()
        self._last_ok: float | None = None
        self._robot_id: str | None = None
        self._rabbitmq_url: str | None = None  # conexão do Rabbit entregue pela API

    @property
    def robot_id(self) -> str | None:
        with self._lock:
            return self._robot_id

    @property
    def rabbitmq_url(self) -> str | None:
        with self._lock:
            return self._rabbitmq_url

    @property
    def last_ok(self) -> float | None:
        with self._lock:
            return self._last_ok

    def verify(self) -> None:
        """Valida a chave. Levanta RPALicenseError SOMENTE se a chave estiver inativa."""
        try:
            resp = requests.get(self._url, timeout=5)
        except requests.RequestException:
            # API inacessível: sem resposta explícita -> não para (fail-open).
            return

        if resp.status_code == 200:
            data = resp.json()
            if data.get("active") is True:
                with self._lock:
                    self._last_ok = time.time()
                    self._robot_id = data.get("robot_id") or self._robot_id
                    self._rabbitmq_url = data.get("rabbitmq_url") or self._rabbitmq_url
                return
            # Resposta explícita: chave inativa -> ÚNICA condição de parada.
            raise RPALicenseError("Chave desativada — execução interrompida.")

        # 404 (inexistente) ou outro status (5xx etc.): não é "inativa" -> não para.
        return
