import json
import os
from datetime import datetime

import pytest

from pipeline.schema import EVENT_SCHEMA
from simulator.event_producer import ACTIONS, CATEGORIES, PRICE_RANGES, WEIGHTS, generate_event, write_event


@pytest.fixture
def events():
    return [generate_event() for _ in range(500)]


def test_event_has_exactly_the_schema_fields(events):
    expected = {field.name for field in EVENT_SCHEMA.fields}

    for e in events:
        assert set(e) == expected


def test_field_types_and_formats(events):
    for e in events:
        datetime.fromisoformat(e["timestamp"])
        assert e["user_id"].startswith("usr_") and e["user_id"][4:].isdigit()
        assert e["product_id"].startswith("prod_") and e["product_id"][5:].isdigit()
        assert e["seller_id"].startswith("sel_") and e["seller_id"][4:].isdigit()
        assert isinstance(e["user_city"], str) and e["user_city"]
        assert isinstance(e["price"], float)


def test_timestamp_carries_its_utc_offset(events):
    # Sans décalage explicite, Spark interprète l'heure dans le fuseau de la machine
    for e in events:
        assert datetime.fromisoformat(e["timestamp"]).utcoffset().total_seconds() == 0


def test_category_action_and_price_are_consistent(events):
    for e in events:
        assert e["product_cat"] in CATEGORIES
        assert e["action_type"] in ACTIONS
        low, high = PRICE_RANGES[e["product_cat"]]
        assert low <= e["price"] <= high


def test_every_category_has_a_price_range():
    assert set(PRICE_RANGES) == set(CATEGORIES)
    assert len(ACTIONS) == len(WEIGHTS) and sum(WEIGHTS) == pytest.approx(1.0)


def test_views_dominate_likes_which_dominate_purchases(events):
    counts = {a: sum(e["action_type"] == a for e in events) for a in ACTIONS}

    assert counts["VUES"] > counts["AIME"] > counts["ACHAT"]


def test_event_is_serialisable_as_json(events):
    for e in events:
        assert json.loads(json.dumps(e, ensure_ascii=False)) == e


def test_write_event_leaves_only_complete_json_files(tmp_path):
    for _ in range(20):
        write_event(generate_event(), str(tmp_path))

    files = os.listdir(tmp_path)
    assert len(files) == 20  # noms uniques, pas d'écrasement
    for name in files:
        assert name.endswith(".json") and not name.startswith((".", "_"))
        with open(tmp_path / name, encoding="utf-8") as f:
            json.load(f)
