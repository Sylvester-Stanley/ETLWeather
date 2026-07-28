"""Unit tests for the weather_etl_pipeline DAG.

These tests exercise the *real* task callables from dags/etlweather.py with the
Airflow hooks swapped out, so they run in under a second with no Docker, no
Postgres and no network access:

    pytest tests/dags/test_weather_etl.py -v

The load test replays the DAG's actual CREATE TABLE / INSERT statements against
an in-memory SQLite database. SQLite uses `?` placeholders where psycopg2 uses
`%s`, so the fake cursor rewrites them; everything else is the production SQL.
"""

from __future__ import annotations

import sqlite3
from unittest import mock

import pytest
from airflow.models import DagBag

DAG_ID = "weather_etl_pipeline"

SAMPLE_RESPONSE = {
    "latitude": 51.5,
    "longitude": -0.120000124,
    "generationtime_ms": 0.0349,
    "utc_offset_seconds": 0,
    "timezone": "GMT",
    "elevation": 23.0,
    "current_weather": {
        "time": "2026-07-28T07:00",
        "interval": 900,
        "temperature": 17.4,
        "windspeed": 11.2,
        "winddirection": 243,
        "is_day": 1,
        "weathercode": 3,
    },
}


@pytest.fixture(scope="module")
def dag():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
    loaded = dagbag.dags.get(DAG_ID)
    assert loaded is not None, f"{DAG_ID} not found in dags/"
    return loaded


@pytest.fixture(scope="module")
def callables(dag):
    return {t.task_id: t.python_callable for t in dag.tasks}


# --------------------------------------------------------------------------- #
# Structure
# --------------------------------------------------------------------------- #

def test_dag_has_three_tasks_in_a_linear_chain(dag):
    tasks = {t.task_id: t for t in dag.tasks}
    assert set(tasks) == {
        "extract_weather_data",
        "transform_weather_data",
        "load_weather_data",
    }
    assert tasks["extract_weather_data"].downstream_task_ids == {"transform_weather_data"}
    assert tasks["transform_weather_data"].downstream_task_ids == {"load_weather_data"}
    assert tasks["load_weather_data"].downstream_task_ids == set()


def test_dag_schedule_is_daily_without_catchup(dag):
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False


# --------------------------------------------------------------------------- #
# Extract
# --------------------------------------------------------------------------- #

class _Response:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else SAMPLE_RESPONSE

    def json(self):
        return self._payload


def _patched_extract(callables, hook_cls):
    fn = callables["extract_weather_data"]
    with mock.patch.dict(fn.__globals__, {"HttpHook": hook_cls}):
        return fn()


def test_extract_builds_the_expected_endpoint(callables):
    seen = {}

    class FakeHttpHook:
        def __init__(self, http_conn_id=None, method=None):
            seen["conn_id"] = http_conn_id
            seen["method"] = method

        def run(self, endpoint):
            seen["endpoint"] = endpoint
            return _Response()

    payload = _patched_extract(callables, FakeHttpHook)

    assert seen["conn_id"] == "open_meteo_api"
    assert seen["method"] == "GET"
    assert seen["endpoint"] == (
        "/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true"
    )
    assert payload["current_weather"]["temperature"] == 17.4


def test_extract_raises_on_non_200(callables):
    class FailingHook:
        def __init__(self, http_conn_id=None, method=None):
            pass

        def run(self, endpoint):
            return _Response(status_code=503)

    with pytest.raises(Exception, match="Failed to fetch weather data: 503"):
        _patched_extract(callables, FailingHook)


# --------------------------------------------------------------------------- #
# Transform
# --------------------------------------------------------------------------- #

def test_transform_flattens_to_the_warehouse_row_shape(callables):
    row = callables["transform_weather_data"](SAMPLE_RESPONSE)

    assert set(row) == {
        "latitude", "longitude", "temperature",
        "windspeed", "winddirection", "weathercode",
    }
    assert row["temperature"] == 17.4
    assert row["windspeed"] == 11.2
    assert row["winddirection"] == 243
    assert row["weathercode"] == 3


def test_transform_echoes_requested_coordinates_not_api_coordinates(callables):
    """Documents real behaviour: the DAG stores what it ASKED for.

    Open-Meteo snaps coordinates to its model grid (51.5074 -> 51.5), but the
    DAG re-attaches its own module-level constants, so the stored latitude is
    the requested value as a string, not the grid value from the response.
    """
    row = callables["transform_weather_data"](SAMPLE_RESPONSE)

    assert row["latitude"] == "51.5074"
    assert row["longitude"] == "-0.1278"
    assert isinstance(row["latitude"], str)
    assert SAMPLE_RESPONSE["latitude"] == 51.5  # what the API actually returned


def test_transform_fails_loudly_when_current_weather_is_absent(callables):
    with pytest.raises(KeyError):
        callables["transform_weather_data"]({"latitude": 51.5, "hourly": {}})


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #

@pytest.fixture
def sqlite_conn():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def test_load_creates_table_and_inserts_row(callables, sqlite_conn):
    seen = {}

    class _Cursor:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            # psycopg2 paramstyle -> sqlite paramstyle
            return self._cur.execute(sql.replace("%s", "?"), params or [])

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cursor(sqlite_conn.cursor())

        def commit(self):
            seen["committed"] = True
            sqlite_conn.commit()

    class FakePostgresHook:
        def __init__(self, postgres_conn_id=None):
            seen["conn_id"] = postgres_conn_id

        def get_conn(self):
            return _Conn()

    row = callables["transform_weather_data"](SAMPLE_RESPONSE)
    fn = callables["load_weather_data"]
    with mock.patch.dict(fn.__globals__, {"PostgresHook": FakePostgresHook}):
        fn(row)

    assert seen["conn_id"] == "postgres_default"
    assert seen.get("committed") is True

    stored = sqlite_conn.execute(
        "SELECT latitude, longitude, temperature, windspeed, winddirection, weathercode "
        "FROM weather_data"
    ).fetchall()
    assert stored == [(51.5074, -0.1278, 17.4, 11.2, 243.0, 3)]


def test_load_is_append_only_so_reruns_duplicate_rows(callables, sqlite_conn):
    """Documents the idempotency gap: running the same day twice stores it twice."""

    class _Cursor:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            return self._cur.execute(sql.replace("%s", "?"), params or [])

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cursor(sqlite_conn.cursor())

        def commit(self):
            sqlite_conn.commit()

    class FakePostgresHook:
        def __init__(self, postgres_conn_id=None):
            pass

        def get_conn(self):
            return _Conn()

    row = callables["transform_weather_data"](SAMPLE_RESPONSE)
    fn = callables["load_weather_data"]
    with mock.patch.dict(fn.__globals__, {"PostgresHook": FakePostgresHook}):
        fn(row)
        fn(row)  # simulate a re-run / clear-and-retry of the same logical date

    count = sqlite_conn.execute("SELECT COUNT(*) FROM weather_data").fetchone()[0]
    assert count == 2, "load has no idempotency key, so the row is duplicated"
