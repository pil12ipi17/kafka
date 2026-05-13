import logging

from app.logging_json import configure_logging
from app.schema_registry import register_contracts

LOGGER = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    register_contracts()
    LOGGER.info("schema_bootstrap_completed")


if __name__ == "__main__":
    main()
