import uvicorn
from tikblog_runtime import configure_logging

from unsubscribeme.api import create_app
from unsubscribeme.config import Settings


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)

    app = create_app(settings)
    # log_config=None keeps uvicorn from installing its own handlers over our JSON logging.
    uvicorn.run(app, host=settings.host, port=settings.port, log_config=None)


if __name__ == "__main__":
    main()
