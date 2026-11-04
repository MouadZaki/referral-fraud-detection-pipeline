# Stage 2 Airflow image — pinned Airflow 2.11 (Python 3.11) plus the
# future DAG's runtime dependencies, baked at build time (no runtime
# pip installs).
#
# pandas/PyYAML cover CSV validation; pyarrow is the Parquet writer;
# apache-airflow-providers-amazon provides S3Hook for MinIO. Versions
# are pinned or locked by the official Airflow constraints file for
# this exact Airflow/Python combo, keeping them compatible with the
# providers bundled in the base image.

FROM apache/airflow:2.11.0-python3.11

ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.11

USER airflow
RUN pip install --no-cache-dir \
        "apache-airflow==2.11.0" \
        "pandas==2.1.4" \
        "pyyaml==6.0.2" \
        "pyarrow==16.1.0" \
        "apache-airflow-providers-amazon" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
