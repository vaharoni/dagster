import os
import sqlite3
import tempfile

import pytest
from dagster import AssetExecutionContext, AssetOut, file_relative_path
from dagster._core.definitions import build_assets_job
from dagster_embedded_elt.sling.asset_decorator import sling_asset
from dagster_embedded_elt.sling.resources import (
    SlingMode,
    SlingResource,
    SlingSourceConnection,
    SlingTargetConnection,
)


@pytest.fixture
def temp_db():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        dbpath = os.path.join(tmpdir_path, "sqlite.db")
        yield dbpath


@pytest.fixture
def sling_sqlite_resource(temp_db):
    return SlingResource(
        source_connection=SlingSourceConnection(type="file"),
        target_connection=SlingTargetConnection(
            type="sqlite", connection_string=f"sqlite://{temp_db}"
        ),
    )


@pytest.fixture
def sqlite_connection(temp_db):
    yield sqlite3.connect(temp_db)


@pytest.fixture
def test_csv():
    return os.path.abspath(file_relative_path(__file__, "test.csv"))


def test_build_sling_asset_decorator(
    test_csv: str, sling_sqlite_resource: SlingResource, sqlite_connection: sqlite3.Connection
):
    @sling_asset(outs={"main_tbl": AssetOut()})
    def main_tbl(context: AssetExecutionContext, sling: SlingResource):
        source_stream = f"file://{test_csv}"
        target_object = "main.tbl"
        mode = SlingMode.INCREMENTAL
        primary_key = "SPECIES_CODE"

        yield from sling.stream(
            source_stream=source_stream,
            target_object=target_object,
            mode=mode,
            primary_key=[primary_key],
        )

    sling_job = build_assets_job(
        "sling_job",
        [main_tbl],
        resource_defs={"sling": sling_sqlite_resource},
    )
    runs = 1
    expected = 3
    counts = None
    for _ in range(runs):
        res = sling_job.execute_in_process()
        assert res.success
        counts = sqlite_connection.execute("SELECT count(1) FROM main.tbl").fetchone()[0]
    assert counts == expected
