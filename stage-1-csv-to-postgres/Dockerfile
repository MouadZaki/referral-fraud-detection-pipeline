# Stage 1 Airflow image — pinned Airflow 2.11 (Python 3.11) plus the
# DAG's runtime dependencies, baked at build time so container starts
# are reproducible (no runtime pip installs via _PIP_ADDITIONAL_REQUIREMENTS).
#
# Versions are pinned and installed against the official Airflow
# constraints file for this exact Airflow/Python combo, which keeps
# them compatible with everything already in the image — including the
# bundled apache-airflow-providers-postgres (PostgresHook) and its
# psycopg2 driver.

FROM apache/airflow:2.11.0-python3.11

ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.11

USER airflow
RUN pip install --no-cache-dir \
        "apache-airflow==2.11.0" \
        "pandas==2.1.4" \
        "pyyaml==6.0.2" \
        "psycopg2-binary==2.9.10" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
