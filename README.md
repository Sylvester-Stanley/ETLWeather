# ETLWeather

An Apache Airflow ETL pipeline that ingests current weather observations from the **Open-Meteo API** into **PostgreSQL** on a daily schedule.

Built on Astro Runtime 12.1.1 (Apache Airflow 2.10.2) using the TaskFlow API.

```
Open-Meteo API  ──►  extract  ──►  transform  ──►  load  ──►  PostgreSQL
   (JSON/HTTPS)      HttpHook     pure Python    PostgresHook   weather_data
```

---

## Documentation

| Document | What it covers |
|---|---|
| **[Case Study](docs/CASE_STUDY.md)** | Architecture, task-by-task walkthrough, verified findings and recommendations |
| **[Implementation Guide](docs/IMPLEMENTATION_GUIDE.md)** | Step-by-step VS Code runbook: setup, run, verify, troubleshoot |
| **[Combined PDF](docs/Weather_ETL_Pipeline_Case_Study.pdf)** | Both documents as a single 22-page report |

---

## Quick start

Requires Docker Desktop and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).

```bash
git clone https://github.com/Sylvester-Stanley/ETLWeather.git
cd ETLWeather

cp .env.example .env      # defines the two Airflow connections — do not skip
astro dev start           # starts Airflow + the weather_db warehouse
```

Open <http://localhost:8080> (`admin` / `admin`), enable **`weather_etl_pipeline`**, and trigger it.

Verify the rows landed:

```bash
docker exec -it weather_db psql -U postgres -d weather \
  -c "SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 5;"
```

> **Why `.env` matters.** Airflow ships a built-in connection named `postgres_default` that points at its own metadata database. Without the override in `.env`, the DAG appears to succeed but writes `weather_data` into Airflow's internal database. See [Case Study §6.3](docs/CASE_STUDY.md#63-postgres_default-collides-with-an-airflow-built-in--high-severity-silent).

---

## Testing

```bash
AIRFLOW_HOME=$(pwd) pytest tests/ -v
```

`AIRFLOW_HOME` must point at the project root, or `DagBag` finds no DAGs and every test silently skips.

- `tests/dags/test_weather_etl.py` — 9 unit tests covering all three tasks. No Docker or network needed; runs in about a second.
- `tests/dags/test_dag_example.py` — Astronomer's stock integrity checks. **Two currently fail** on this DAG (no tags, no retries) — a real finding, documented in [Case Study §6.1](docs/CASE_STUDY.md#61-no-retry-policy--high-severity).

A Docker-free end-to-end check against the live API:

```bash
python include/scripts/smoke_test_pipeline.py          # extract + transform
python include/scripts/smoke_test_pipeline.py --load   # also insert into Postgres
```

---

## Project layout

```
dags/etlweather.py                     the weather_etl_pipeline DAG
docker-compose.override.yml            warehouse attached to the Airflow network
.env.example                           connection definitions template
include/sql/init_weather_db.sql        warehouse bootstrap schema
include/sql/verify_weather_data.sql    post-run verification queries
include/scripts/smoke_test_pipeline.py Docker-free pipeline check
tests/dags/test_weather_etl.py         unit tests for this DAG
docs/                                  case study, guide, diagrams, PDF
```

## Regenerating the docs

```bash
pip install reportlab pillow
python docs/tools/make_diagrams.py     # rebuild the PNG diagrams
python docs/tools/md_to_pdf.py docs/CASE_STUDY.md docs/IMPLEMENTATION_GUIDE.md \
    -o docs/Weather_ETL_Pipeline_Case_Study.pdf
```

---

## Notes on the target database

The repository's original `docker-compose.yml` starts Postgres as a **separate Compose project**, so Airflow cannot reach it by service name. `docker-compose.override.yml` supersedes it by attaching the warehouse (`weather_db`, host port **5434**) to Astro's `airflow` network. Details in [Case Study §6.4](docs/CASE_STUDY.md#64-the-bundled-docker-composeyml-does-not-serve-this-dag--medium-severity).

## License

See [LICENSE](LICENSE).
