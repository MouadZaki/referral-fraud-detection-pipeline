# Referral fraud-detection pipeline — Springer Capital Stage 0.
#
# The image installs the project (with pandas) plus pyspark, which the
# assignment requires the image to include. The pipeline itself is a
# Pandas implementation and never starts a Spark session, so no JRE is
# bundled; if Spark support is added later, install a JRE here.
#
# Source CSVs are NOT baked in — they arrive via a read-only bind mount
# at /data; the report is written back to the host via a bind mount at
# /output. See README "Run with Docker".

FROM python:3.11-slim

WORKDIR /app

# Project metadata + package source only. Tests, data, output, venv,
# git metadata and credentials are excluded by .dockerignore.
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir . \
    && pip install --no-cache-dir "pyspark>=3.5,<4"

# Mount points: host data/raw read-only -> /data, host output -> /output.
ENV DATA_DIR=/data \
    OUTPUT_DIR=/output

ENTRYPOINT ["referral-pipeline"]
CMD ["--data-dir", "/data", "--output-dir", "/output"]
