FROM apache/airflow:2.10.4-python3.12

USER root
# netcat for the in-container connectivity checks; curl ships with the base image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
# The constraint file keeps provider versions compatible with this Airflow release.
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"
