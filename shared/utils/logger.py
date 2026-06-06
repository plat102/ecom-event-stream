import sys
from loguru import logger
from shared.config.settings import settings

logger.remove()

if settings.LOG_FORMAT == "json":
    logger.add(sys.stderr, serialize=True)
else:
    logger.add(
        sys.stderr,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[component]}</cyan> | {message}",
    )


def get_logger(name: str):
    return logger.bind(component=name)
