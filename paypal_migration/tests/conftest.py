import os
import sys
import pytest

# Ensure project root is on path so `src` can be imported when running pytest
PROJECT_ROOT = os.path.dirname(os.path.abspath(os.path.join(__file__, "..")))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    spark = get_spark("pytest_session", shuffle_partitions=1)
    yield spark
    spark.stop()