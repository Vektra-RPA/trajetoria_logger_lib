"""trajetoria_logger — logging com garantia de entrega + kill switch para RPAs.

Uso mínimo:

    from trajetoria_logger import RPALoggerClient, RPALoggerConfig, RPALicenseError

    client = RPALoggerClient(RPALoggerConfig(api_key="...", api_base_url="https://..."))
    client.info("robô iniciou", task="exemplo")
    client.raise_if_inactive()      # checkpoint do kill switch entre etapas
    client.finish("success")        # status final da execução: success | partial | error
    client.close()                  # drena o buffer antes de sair

Documentação completa: o README.md é instalado JUNTO com o pacote, ao lado deste arquivo
(`trajetoria_logger.README_PATH`). Para achá-lo numa máquina qualquer:

    python -c "import trajetoria_logger; print(trajetoria_logger.README_PATH)"
"""
from pathlib import Path

from .client import (
    LEVEL_CRITICAL,
    LEVEL_DEBUG,
    LEVEL_ERROR,
    LEVEL_INFO,
    LEVEL_WARNING,
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_SUCCESS,
    RPALoggerClient,
)
from .config import RPALoggerConfig
from .license import RPALicenseError

__all__ = [
    "RPALoggerClient",
    "RPALoggerConfig",
    "RPALicenseError",
    "LEVEL_CRITICAL",
    "LEVEL_ERROR",
    "LEVEL_WARNING",
    "LEVEL_INFO",
    "LEVEL_DEBUG",
    "STATUS_SUCCESS",
    "STATUS_PARTIAL",
    "STATUS_ERROR",
    "README_PATH",
]

__version__ = "1.2.0"

# Copiado para dentro do pacote no build (ver `setup.py`). Num clone ou instalação editável
# (`pip install -e .`) a cópia não existe, e o caminho cai no original, na raiz do repositório.
_PKG_DIR = Path(__file__).resolve().parent
README_PATH = (
    _PKG_DIR / "README.md" if (_PKG_DIR / "README.md").exists() else _PKG_DIR.parent / "README.md"
)
