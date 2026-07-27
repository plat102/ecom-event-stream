FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    # streamlit puts the script's dir on sys.path, not WORKDIR, so `import shared` needs this.
    PYTHONPATH=/app

WORKDIR /app

RUN pip install poetry==1.8.3

COPY pyproject.toml poetry.lock ./

# --no-root: `shared` is copied in below rather than installed, so this step needs no source.
RUN poetry install --only main --no-root

COPY shared/ ./shared/
COPY apps/dashboard/ ./apps/dashboard/

EXPOSE 8501

CMD ["streamlit", "run", "apps/dashboard/src/app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
