check: format-check lint typecheck

format:
    uv run ruff format

format-check:
    uv run ruff format --check

lint:
    uv run ruff check

typecheck:
    uv run mypy src
    uv run basedpyright src

fix:
    uv run ruff check --fix
    uv run ruff format

test *ARGS:
    uv run pytest tests/ {{ARGS}}

test-matrix:
    #!/usr/bin/env bash
    set -euo pipefail
    for v in 11 12 13 14 15 16 17 18; do
      docker compose down -v
      PG_VERSION=$v docker compose up -d --wait
      echo "testing on postgres $v"
      just test -q || echo "FAILED on postgres $v"
    done

cov-run:
    uv run coverage run -m pytest tests/

cov-report:
    uv run coverage combine
    uv run coverage report

cov: cov-run cov-report
