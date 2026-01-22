FROM apache/airflow:3.1.5-python3.12

USER root

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy dependency files
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies using uv into the system environment
# We use --system to install into the container's python environment
RUN uv export --frozen --format requirements-txt > requirements.txt && \
    uv pip install --system -r requirements.txt

# Add system site-packages to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/usr/python/lib/python3.12/site-packages"

USER airflow