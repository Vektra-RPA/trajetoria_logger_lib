"""rpa_logger — biblioteca de logging + kill switch para RPAs (base de testes, Opção B)."""
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
