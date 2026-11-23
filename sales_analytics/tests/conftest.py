"""Shared pytest fixtures for the sales_analytics test suite.
"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("sales-analytics-tests")
        .getOrCreate()
    )
