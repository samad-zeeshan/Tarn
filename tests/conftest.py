"""
Shared fixtures. One Spark session for the whole run.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

from pipeline.common import spark_session

REPO = Path(__file__).resolve().parent.parent
SAMPLE_AUTH = REPO / "data" / "sample" / "auth_sample.csv.gz"
SAMPLE_REDTEAM = REPO / "data" / "sample" / "redteam_sample.csv.gz"


def _reachable(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def spark():
    # local[2] rather than local[*]. The tests assert on values, not speed, and pinning the
    # parallelism keeps shuffle behaviour reproducible across machines.
    session = spark_session("tests", shuffle_partitions=2)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_auth_path() -> str:
    if not SAMPLE_AUTH.exists():
        pytest.skip(f"{SAMPLE_AUTH} missing, run `python data/fetch.py sample`")
    return str(SAMPLE_AUTH)


@pytest.fixture(scope="session")
def sample_redteam_path() -> str:
    if not SAMPLE_REDTEAM.exists():
        pytest.skip(f"{SAMPLE_REDTEAM} missing, run `python data/fetch.py sample`")
    return str(SAMPLE_REDTEAM)


@pytest.fixture(scope="session")
def sample_lake(spark, sample_auth_path, sample_redteam_path, tmp_path_factory) -> str:
    """Build a real lake from the committed CI slice, once, for the whole suite."""
    from pipeline.sessionize import build_lake, build_redteam

    out = tmp_path_factory.mktemp("lake")
    auth_path = str(out / "auth")
    build_lake(spark, sample_auth_path, auth_path, coalesce=0)
    build_redteam(spark, sample_redteam_path, str(out / "redteam"))
    return auth_path


@pytest.fixture(scope="session")
def neo4j_available() -> bool:
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    host = uri.split("//")[-1].split(":")[0]
    port = int(uri.rsplit(":", 1)[-1])
    return _reachable(host, port)


@pytest.fixture(scope="session")
def kafka_available() -> bool:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    host, port = bootstrap.split(":")
    return _reachable(host, int(port))
