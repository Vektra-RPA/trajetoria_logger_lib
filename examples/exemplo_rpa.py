"""RPA de exemplo usando a RPALoggerClient.

Demonstra o uso real da biblioteca:
  - a APLICAÇÃO (este RPA) decide de onde vem a config e a passa para a lib;
  - emite logs de vários níveis ao longo das etapas;
  - chama `raise_if_inactive()` entre etapas (checkpoint cooperativo do kill switch);
  - trata RPALicenseError como fim de execução;
  - informa o status FINAL da execução (success / partial / error) no encerramento;
  - liga `on_warning` para ver o que a lib tem a dizer (a lib nunca imprime sozinha).

Uso:
    python examples/exemplo_rpa.py <API_KEY> [n_etapas]

Config opcional via ambiente DO PRÓPRIO RPA (não da lib):
    API_BASE_URL              (default http://127.0.0.1:8000)
    RPA_BUFFER_PATH           (default ./rpa_buffer.db)
    RPA_LICENSE_CHECK_MINUTES (default 30)  -- baixe para testar o kill switch mid-run
    RPA_STEP_SLEEP            (default 0.5) -- segundos por etapa
"""
from __future__ import annotations

import os
import sys
import time

from dotenv import load_dotenv

from trajetoria_logger import (
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_SUCCESS,
    RPALicenseError,
    RPALoggerClient,
    RPALoggerConfig,
)

load_dotenv()  # a aplicação carrega seu próprio ambiente (a LIB nunca faz isso)


def main() -> None:
    api_key = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("RPA_API_KEY", "")
    n_steps = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    config = RPALoggerConfig(
        api_key=api_key,
        api_base_url=os.environ.get("API_BASE_URL", "http://127.0.0.1:8000"),
        buffer_path=os.environ.get("RPA_BUFFER_PATH", "./rpa_buffer.db"),
        license_check_interval_minutes=float(
            os.environ.get("RPA_LICENSE_CHECK_MINUTES", "30")
        ),
        on_warning=lambda msg: print(f"[lib] {msg}"),
    )
    step_sleep = float(os.environ.get("RPA_STEP_SLEEP", "0.5"))

    # Boot: o construtor levanta RPALicenseError se a chave estiver INATIVA, ou se a API
    # estiver inacessível E o período de graça já tiver expirado.
    try:
        client = RPALoggerClient(config)
    except RPALicenseError as exc:
        print(f"[RPA] não iniciou: {exc}")
        return

    print(f"[RPA] execução {client.execution_id} (robot_id={client.robot_id})")
    if client.pending_logs:
        print(f"[RPA] {client.pending_logs} logs de execuções anteriores no buffer (vão como bulk)")

    emitted = 0
    # Status final da execução. Começa como erro: se algo escapar sem ser tratado, é o que
    # deve ficar registrado.
    status, status_msg = STATUS_ERROR, None
    try:
        client.info("RPA iniciou", task="exemplo")
        emitted += 1
        for step in range(1, n_steps + 1):
            client.raise_if_inactive()  # checkpoint do kill switch entre etapas
            client.info(f"etapa {step} iniciada", step=step)
            emitted += 1
            time.sleep(step_sleep)  # simula trabalho
            client.debug(f"etapa {step} detalhe", step=step)
            emitted += 1
            if step % 3 == 0:
                client.warning(f"etapa {step} com aviso", step=step)
                emitted += 1
            print(f"[RPA] etapa {step}/{n_steps} concluída")
        client.info("RPA finalizou com sucesso", task="exemplo")
        emitted += 1
        print(f"[RPA] concluído — {emitted} logs emitidos")
        status = STATUS_SUCCESS
    except RPALicenseError as exc:
        print(f"[RPA] PARADA COOPERATIVA no checkpoint: {exc}")
        status, status_msg = STATUS_PARTIAL, f"interrompido pelo kill switch: {exc}"
    except Exception as exc:
        client.critical("RPA falhou", erro=str(exc), tipo=type(exc).__name__)
        status_msg = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # close(status=...) registra o status final e drena logs + status. O status só sobe
        # depois dos logs desta execução. O que não for confirmado não é perda: fica no
        # buffer e sobe na próxima execução.
        restantes = client.close(status=status, message=status_msg)
        print(f"[RPA] encerrado. Logs emitidos: {emitted}; não confirmados: {restantes}")


if __name__ == "__main__":
    main()
