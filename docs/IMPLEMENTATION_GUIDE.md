# Implementation Guide — Running ETLWeather in VS Code

A step-by-step runbook to get `weather_etl_pipeline` running on your machine, verify that data actually lands in PostgreSQL, and diagnose it when it does not.

Companion document: **[Case Study](CASE_STUDY.md)** explains *why* the pipeline is built this way and what its weaknesses are. This guide is purely operational.

> **Reading the callouts**
> ⚠️ = a step where people commonly lose an hour. Do not skim these.

---

## Contents

- [0. Prerequisites](#0-prerequisites)
- [1. Open the project in VS Code](#1-open-the-project-in-vs-code)
- [2. Local Python environment (IntelliSense + fast tests)](#2-local-python-environment-intellisense--fast-tests)
- [3. Configure Airflow connections](#3-configure-airflow-connections)
- [4. Start the stack](#4-start-the-stack)
- [5. Run the pipeline](#5-run-the-pipeline)
- [6. Verify the data landed](#6-verify-the-data-landed)
- [7. Run the tests](#7-run-the-tests)
- [8. Everyday commands](#8-everyday-commands)
- [9. Troubleshooting](#9-troubleshooting)
- [10. Suggested exercises](#10-suggested-exercises)

---

## 0. Prerequisites

| Tool | Why | Check |
|---|---|---|
| **Docker Desktop** (or Podman) | Astro runs Airflow in containers | `docker ps` |
| **Astro CLI** | Starts and manages the local Airflow environment | `astro version` |
| **Python 3.11 or 3.12** | Local venv for IntelliSense and unit tests | `python --version` |
| **VS Code** | Editor | — |
| **Git** | Clone the repository | `git --version` |

Install the Astro CLI:

```bash
# macOS
brew install astro

# Linux
curl -sSL install.astronomer.io | sudo bash -s

# Windows (PowerShell as Administrator)
winget install -e --id Astronomer.Astro
```

⚠️ **Docker Desktop must be running before any `astro dev` command.** Give it at least **4 GB** of RAM (Settings → Resources); Airflow's four containers are cramped below that.

> Astro Runtime 12.1.1 pins **Airflow 2.10.2**, so everything below is Airflow 2 syntax. Do not upgrade the `Dockerfile` to a Runtime 13+/Airflow 3 image without first applying the migration notes in Case Study §6.5 — the DAG uses two APIs that Airflow 3 removed.

---

## 1. Open the project in VS Code

```bash
git clone https://github.com/Sylvester-Stanley/ETLWeather.git
cd ETLWeather
code .
```

When VS Code prompts *"This workspace has extension recommendations"*, click **Install All**. The repository ships `.vscode/extensions.json` recommending Python, Pylance, Docker, SQLTools with the PostgreSQL driver, and YAML support.

`.vscode/settings.json` is pre-configured to enable pytest, set `AIRFLOW_HOME` to the project root in every integrated terminal (this matters — see §7), and register a SQLTools connection to the warehouse.

---

## 2. Local Python environment (IntelliSense + fast tests)

Airflow itself runs inside Docker, but a local virtual environment gives you autocomplete, go-to-definition on `HttpHook`/`PostgresHook`, and a test suite that runs in about a second without starting containers.

```bash
python -m venv .venv

# macOS / Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
```

Install Airflow matching the Runtime exactly. Airflow requires its constraints file — without it, pip will resolve incompatible transitive dependencies:

```bash
AIRFLOW_VERSION=2.10.2
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
            apache-airflow-providers-http \
            apache-airflow-providers-postgres \
            pytest requests psycopg2-binary \
            --constraint "${CONSTRAINT_URL}"
```

<details>
<summary>Windows PowerShell equivalent</summary>

```powershell
$AIRFLOW_VERSION="2.10.2"
$PYTHON_VERSION=(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
$CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"

pip install "apache-airflow==$AIRFLOW_VERSION" apache-airflow-providers-http apache-airflow-providers-postgres pytest requests psycopg2-binary --constraint $CONSTRAINT_URL
```
</details>

Select the interpreter in VS Code: **Ctrl/Cmd+Shift+P → Python: Select Interpreter → `./.venv/bin/python`**.

Quick check that the API contract and transform logic work, with no Docker involved:

```bash
python include/scripts/smoke_test_pipeline.py
```

Expected tail:

```
[2/3] TRANSFORM
  [PASS] row has the expected 6 columns
  temperature   : 17.4 C
  weathercode   : 3 -> Overcast
  [PASS] all range checks passed

RESULT: all stages passed
```

---

## 3. Configure Airflow connections

The DAG references two connection IDs but defines neither. Create them **before** starting Airflow:

```bash
cp .env.example .env
```

`.env` is git-ignored and is read automatically by `astro dev start`. It defines:

| Connection ID | Type | Points at |
|---|---|---|
| `open_meteo_api` | HTTP | `https://api.open-meteo.com` |
| `postgres_default` | Postgres | the `weather_db` container, database `weather` |

⚠️ **This is the single most important step in the guide.** Airflow ships a built-in connection called `postgres_default` that points at **Airflow's own metadata database**. If you skip this file, the DAG will still appear to succeed — and will write `weather_data` into Airflow's internal database, where `astro dev kill` destroys it. The `.env` entry overrides the built-in, because environment variables take precedence over the metadata database.

Two details in the Postgres connection that trip people up:

- `"host": "weather_db"` — the **Compose service name**, resolved over the shared Docker network. Not `localhost` (that is the container itself) and not `host.docker.internal` (an unnecessary round trip through the host, and unreliable on Linux).
- `"port": 5432` — the **container** port. The host port `5434` is only for connecting from your laptop.

> Connections defined by environment variable do **not** appear in the Airflow UI under Admin → Connections. That is expected, not a bug. To manage them in the UI instead, delete them from `.env` and add them through the UI using the same values.

---

## 4. Start the stack

```bash
astro dev start
```

This builds the image and starts five containers: Airflow's webserver, scheduler, triggerer and metadata Postgres, plus the `weather_db` warehouse contributed by `docker-compose.override.yml`.

First run takes several minutes while the Runtime image downloads. Expect:

```
Airflow Webserver: http://localhost:8080
Postgres Database: localhost:5432/postgres
The default Airflow UI credentials are: admin:admin
```

Confirm all five are up:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

You should see a container named `weather_db` publishing `0.0.0.0:5434->5432/tcp`. If it is missing, `docker-compose.override.yml` was not picked up — you are probably not in the project root.

⚠️ **Port conflicts** are the most common startup failure. If 8080 or 5432 are already taken:

```bash
astro config set webserver.port 8081
astro config set postgres.port 5433
astro dev restart
```

The warehouse uses host port **5434** specifically to stay clear of both.

---

## 5. Run the pipeline

1. Open <http://localhost:8080> and log in with `admin` / `admin`.
2. Find **`weather_etl_pipeline`** in the DAG list.
3. Toggle it **on** (the switch on the left). It is paused by default.
4. Click the DAG name, then the **▶ Trigger DAG** button (top right).

Watch the **Grid** view. Three squares should turn dark green in sequence:

```
extract_weather_data → transform_weather_data → load_weather_data
```

Click any square → **Logs** to see that task's output. In the extract log you will find the resolved request URL; in the load log, the executed SQL.

### Running from the command line instead

Test a single task without a full DAG run — useful for tight iteration, as it does not write to the metadata database:

```bash
astro dev bash                          # shell into the scheduler container
airflow tasks test weather_etl_pipeline extract_weather_data 2026-07-28
airflow tasks test weather_etl_pipeline transform_weather_data 2026-07-28
airflow tasks test weather_etl_pipeline load_weather_data 2026-07-28
exit
```

`airflow tasks test` runs the task in the foreground and prints logs straight to your terminal.

---

## 6. Verify the data landed

**Do not trust green squares alone.** A task that writes to the wrong database still shows green. Confirm the rows exist where you expect them:

```bash
docker exec -it weather_db psql -U postgres -d weather -c "SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 5;"
```

Expected:

```
 latitude | longitude | temperature | windspeed | winddirection | weathercode |         timestamp
----------+-----------+-------------+-----------+---------------+-------------+----------------------------
  51.5074 |   -0.1278 |        17.4 |      11.2 |           243 |           3 | 2026-07-28 07:46:25.028
```

For a fuller report — row counts, decoded WMO codes, duplicate detection and range assertions:

```bash
docker exec -i weather_db psql -U postgres -d weather < include/sql/verify_weather_data.sql
```

### Querying from VS Code

With the SQLTools extension installed, open the **SQLTools** sidebar icon → **ETLWeather warehouse (local)** → **Connect**. The connection is pre-configured (`localhost:5434`, database `weather`, user/password `postgres`). You can then open `include/sql/verify_weather_data.sql` and run statements with **Ctrl/Cmd+E, Ctrl/Cmd+E**.

### Proving the whole path independently

To confirm extract, transform *and* load outside Airflow entirely:

```bash
python include/scripts/smoke_test_pipeline.py --load
```

This performs a real API call and a real insert into `localhost:5434`. If this passes but the DAG fails, the problem is Airflow configuration (connections, networking), not your code or the database.

---

## 7. Run the tests

```bash
pytest tests/ -v
```

⚠️ **`AIRFLOW_HOME` must point at the project root.** `.vscode/settings.json` sets this for VS Code's integrated terminal. In an external terminal, set it yourself:

```bash
AIRFLOW_HOME=$(pwd) pytest tests/ -v
```

Without it, `DagBag` finds no DAGs, every parametrised test is skipped, and the suite reports a **misleading pass**.

Two suites are present:

**`tests/dags/test_weather_etl.py`** — 9 unit tests written for this pipeline. They execute the real task callables with hooks mocked out, replaying the actual load SQL against in-memory SQLite. No Docker, no network, under a second:

```
test_dag_has_three_tasks_in_a_linear_chain PASSED
test_dag_schedule_is_daily_without_catchup PASSED
test_extract_builds_the_expected_endpoint PASSED
test_extract_raises_on_non_200 PASSED
test_transform_flattens_to_the_warehouse_row_shape PASSED
test_transform_echoes_requested_coordinates_not_api_coordinates PASSED
test_transform_fails_loudly_when_current_weather_is_absent PASSED
test_load_creates_table_and_inserts_row PASSED
test_load_is_append_only_so_reruns_duplicate_rows PASSED
```

**`tests/dags/test_dag_example.py`** — Astronomer's stock integrity checks. **These currently fail for `weather_etl_pipeline`, and that is a genuine finding, not a broken test:**

```
FAILED test_dag_tags[dags/etlweather.py]    - AssertionError: ... has no tags
FAILED test_dag_retries[dags/etlweather.py] - TypeError: '>=' not supported between 'NoneType' and 'int'
```

To make both pass, edit `dags/etlweather.py` (see Case Study §6.1):

```python
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['etl', 'weather', 'open-meteo']) as dags:
```

with `from datetime import timedelta` added at the top. Re-run `pytest tests/ -v` — all should pass.

---

## 8. Everyday commands

| Task | Command |
|---|---|
| Start | `astro dev start` |
| Stop (keep data) | `astro dev stop` |
| Restart after changing `.env` or Compose | `astro dev restart` |
| **Destroy everything including the metadata DB** | `astro dev kill` |
| Shell into the scheduler | `astro dev bash` |
| Follow scheduler logs | `astro dev logs --scheduler --follow` |
| List parsed DAGs | `astro dev bash -c "airflow dags list"` |
| Show import errors | `astro dev bash -c "airflow dags list-import-errors"` |
| Test one task | `astro dev bash -c "airflow tasks test weather_etl_pipeline extract_weather_data 2026-07-28"` |
| psql into the warehouse | `docker exec -it weather_db psql -U postgres -d weather` |
| Run tests | `AIRFLOW_HOME=$(pwd) pytest tests/ -v` |

> Changes to files in `dags/` are picked up automatically within about 30 seconds — no restart needed. Changes to `.env`, `requirements.txt`, `Dockerfile` or Compose files **do** require `astro dev restart`.

---

## 9. Troubleshooting

### `The conn_id 'open_meteo_api' isn't defined`

The connection is missing. Confirm `.env` exists (not just `.env.example`) and restart:

```bash
ls -la .env && astro dev restart
```

Verify it reached the container:

```bash
astro dev bash -c "env | grep AIRFLOW_CONN"
```

### The DAG succeeds but `weather_data` is empty

Almost certainly the `postgres_default` collision (Case Study §6.3) — the rows went into Airflow's metadata database. Check:

```bash
docker exec -it postgres psql -U postgres -d postgres -c "\dt"
```

If `weather_data` appears there, your `.env` override was not applied. Confirm the container sees it, then `astro dev restart`.

### `could not translate host name "weather_db"`

The warehouse container is not on the Airflow network. Confirm it is running and attached:

```bash
docker ps --filter name=weather_db
docker network inspect $(docker network ls --filter name=airflow -q) --format '{{range .Containers}}{{.Name}} {{end}}'
```

`weather_db` must appear in that list. If not, ensure `docker-compose.override.yml` is at the project root and run `astro dev restart`.

### `Ports are not available` / `address already in use`

Something already owns 8080 or 5432:

```bash
astro config set webserver.port 8081
astro config set postgres.port 5433
astro dev restart
```

### `Broken DAG` banner in the UI

```bash
astro dev bash -c "airflow dags list-import-errors"
```

Note that a `RemovedInAirflow3Warning` about `days_ago` or `schedule_interval` is a **warning**, not an import error — the DAG still loads on Airflow 2.10.2.

### Task fails with `Failed to fetch weather data: 429`

Open-Meteo rate-limits its free tier. Wait a minute and re-run. This is exactly the scenario retries would absorb — see Case Study §6.1.

### Tests all skip and report a pass

`AIRFLOW_HOME` is unset. Use `AIRFLOW_HOME=$(pwd) pytest tests/ -v`.

### Containers keep restarting or the scheduler is slow

Docker Desktop is under-provisioned. Raise memory to 4 GB or more in Settings → Resources, then `astro dev restart`.

### Reset the warehouse to an empty state

```bash
astro dev stop
docker volume rm etlweather_weather_db_data
astro dev start
```

The volume name is prefixed with the project directory name; run `docker volume ls` to confirm.

---

## 10. Suggested exercises

Ordered by difficulty. Each corresponds to a finding in the Case Study.

1. **Add retries and tags** (§6.1) — make the stock test suite pass. Trivial, high value.
2. **Modernise the deprecated APIs** (§6.5) — swap `days_ago(1)` for `pendulum.datetime(...)` and `schedule_interval=` for `schedule=`. Confirm the warnings disappear.
3. **Preserve observation time** (§4.4) — carry `current_weather['time']` through the transform into a new `observation_time` column, so you record when the weather *happened*, not when you wrote the row.
4. **Make the load idempotent** (§6.2) — add a primary key on `(latitude, longitude, observation_time)` and switch to `INSERT ... ON CONFLICT ... DO UPDATE`. Prove it by inverting `test_load_is_append_only_so_reruns_duplicate_rows` to assert a count of **1**.
5. **Add a data-quality gate** (§6.6) — insert a validation task between transform and load that raises on out-of-range values. Encode the same rules as `include/sql/verify_weather_data.sql`.
6. **Support multiple cities** (§6.7) — convert to dynamic task mapping with `.expand()` and add a `city` column. One DAG, N parallel extracts.
7. **Decode weather codes at load time** — join to a `wmo_codes` lookup table so analysts read "Overcast" instead of `3`.
8. **Add alerting** — configure `on_failure_callback` to post to Slack or email, so failures reach you without watching the UI.
