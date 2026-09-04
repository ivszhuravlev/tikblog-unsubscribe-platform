FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app app

COPY pyproject.toml README.md ./
COPY libs ./libs
RUN pip install --no-cache-dir .

COPY --chown=app:app services/unsubscribe-consumer/unsubscribe_consumer ./unsubscribe_consumer

USER app
CMD ["python", "-m", "unsubscribe_consumer.main"]
