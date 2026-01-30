# mitos
This repository contains an experimental Python port of the OHDSI Circe cohort
expression builder. The codebase uses Pydantic for schema validation and
Ibis to construct queries against OMOP CDM vocabularies.

## Project Layout

- `src/mitos/`: Python package with the Pydantic models, concept-set
  compiler, and a small CLI (`Mitos` or `python -m mitos.cli`) that can
  print the SQL for a cohort's primary criteria or execute the pipeline.
- `cohorts/`: Example ATLAS cohort JSON exports used for local testing.
- `cohorts/phenotypes/`: Small phenotype samples used by smoke tests and
  quick parity checks.
- `tests/`: Pytest suite that round-trips cohort JSON and validates the
  concept-set compiler.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
pytest
```

To inspect the SQL for a cohort's primary criteria or fetch the events as
Polars:

```bash
Mitos --json cohorts/6243-dementia-outcome-v1.json --mode sql
# or
python -m mitos.cli --connection duckdb:///tmp/cdm.duckdb --mode polars
```

## Compare Row Counts with Circe

Use `scripts/compare_cohort_counts.py` to emit both SQL variants, execute
them against your backend, and assert row-count parity. The script reads a
`profiles.yaml` (default name overridable via `--config`) that describes
connection details and OHDSI schemas.

```bash
python3 scripts/compare_cohort_counts.py --profile local-duckdb
```

Example `profiles.yaml`:

```yaml
default_profile: local-duckdb
profiles:
  local-duckdb:
    backend: duckdb
    database: PATH/TO/DATABASE/database-1M_filtered.duckdb
    cdm_schema: main
    vocab_schema: main
    result_schema: cohorts  # optional
    cohort_table: circe_cohort
    cohort_id: 1
    json_path: cohorts/6243-dementia-outcome-v1.json
    python_sql_out: /tmp/python.sql
    circe_sql_out: /tmp/circe.sql
```

Keys under a profile must match the expected backend config (currently
`duckdb` or `databricks`), and environment variables are expanded if you
use `${VAR}` placeholders. CLI flags override the YAML fields (e.g.
`--json`, `--cdm-schema`, `--result-schema`, `--circe-sql-out`). The
script will also ensure the target cohort table exists, run Circe SQL via
R, and clean up the inserted cohort rows after counting.

## CI / Releases

- PR CI runs the unit test suite (`.github/workflows/ci.yml`) and builds the Postgres Docker image (`.github/workflows/docker.yml`).
- Oracle parity tests that require R/CirceR are run on a schedule or manually (`.github/workflows/oracle.yml`).
- Pushing a tag matching `v*` creates a GitHub Release with sdist/wheel artifacts (`.github/workflows/release.yml`).
