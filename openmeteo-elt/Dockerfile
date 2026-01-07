FROM python:3.14-slim

WORKDIR /app

# Install dependencies
# We install directly to avoid complexity with uv/poetry for this simple script
# and to bypass potential "requires-python" strict checks if 3.14 is not available.
RUN pip install --no-cache-dir pandas httpx sqlalchemy psycopg2-binary

COPY extract.py .

# Create output directory
RUN mkdir -p csvs

CMD ["tail", "-f", "/dev/null"]
