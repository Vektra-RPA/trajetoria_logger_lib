"""trajetoria_logger — logging com garantia de entrega + kill switch para RPAs.

Uso mínimo:

    from trajetoria_logger import RPALoggerClient, RPALoggerConfig, RPALicenseError

    client = RPALoggerClient(RPALoggerConfig(api_key="...", api_base_url="https://..."))
    client.info("robô iniciou", task="exemplo")
    client.raise_if_inactive()      # checkpoint do kill switch entre etapas
    client.close()                  # drena o buffer antes de sair
"""
from .client import (
    LEVEL_CRITICAL,
    LEVEL_DEBUG,
    LEVEL_ERROR,
    LEVEL_INFO,
    LEVEL_WARNING,
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
]

__version__ = "2.0.0"
