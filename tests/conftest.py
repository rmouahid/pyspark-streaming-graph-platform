import os
import sys
from datetime import datetime

import pytest

# Les modules du projet s'importent depuis la racine (config, pipeline, simulator)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale partagée par tous les tests (démarrage coûteux)."""
    from pipeline.spark_session import get_spark_session

    session = get_spark_session("tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def make_events(spark):
    """Construit un DataFrame statique d'événements au schéma du stream."""
    from pipeline.schema import EVENT_SCHEMA

    def build(*events):
        rows = [
            {
                **event,
                "timestamp": datetime.fromisoformat(event["timestamp"]) if event.get("timestamp") else None,
            }
            for event in events
        ]
        return spark.createDataFrame(rows, schema=EVENT_SCHEMA)

    return build


def event(**overrides):
    base = {
        "timestamp": "2026-06-04T14:32:01",
        "user_id": "usr_1000",
        "user_city": "Paris",
        "product_id": "prod_2000",
        "product_cat": "Mode",
        "seller_id": "sel_300",
        "action_type": "VUES",
        "price": 42.0,
    }
    return {**base, **overrides}
