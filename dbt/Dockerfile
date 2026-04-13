FROM ubuntu:22.04

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /opt/dbt

COPY . .

RUN uv sync

ENV PATH="/opt/dbt/.venv/bin:$PATH"

ENTRYPOINT ["dbt"]
