import csv

from graphframes import GraphFrame

from conftest import event
from pipeline import graph as graph_module
from pipeline.graph import build_edges, build_graph, build_vertices, compute_metrics


def rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


def test_vertices_are_users_products_and_sellers(make_events):
    df = make_events(event(user_id="usr_1", user_city="Lyon", product_id="prod_1", product_cat="Mode", seller_id="sel_1"))

    assert rows(build_vertices(df), "id", "type", "label") == [
        ("prod_1", "Product", "Mode"),
        ("sel_1", "Seller", "sel_1"),
        ("usr_1", "User", "Lyon"),
    ]


def test_vertices_are_deduplicated(make_events):
    df = make_events(event(), event(), event(user_id="usr_2"))

    ids = [r["id"] for r in build_vertices(df).collect()]
    assert sorted(ids) == ["prod_2000", "sel_300", "usr_1000", "usr_2"]


def test_null_ids_create_no_vertex(make_events):
    df = make_events(event(user_id=None, seller_id=None))

    assert rows(build_vertices(df), "type") == [("Product",)]


def test_edges_link_users_to_products_by_action_and_sellers_to_products(make_events):
    df = make_events(
        event(user_id="usr_1", product_id="prod_1", seller_id="sel_1", action_type="VUES"),
        event(user_id="usr_1", product_id="prod_1", seller_id="sel_1", action_type="ACHAT"),
    )

    assert rows(build_edges(df), "src", "dst", "relationship") == [
        ("sel_1", "prod_1", "PROPOSE"),  # une seule arête vendeur → produit
        ("usr_1", "prod_1", "ACHAT"),
        ("usr_1", "prod_1", "VUES"),
    ]


def test_every_simulator_action_produces_an_edge(make_events):
    from simulator.event_producer import ACTIONS

    df = make_events(*[event(action_type=a, seller_id=None) for a in ACTIONS])

    assert {r["relationship"] for r in build_edges(df).collect()} == set(ACTIONS)


def test_unknown_actions_and_null_ids_create_no_user_edge(make_events):
    df = make_events(event(action_type="INCONNU"), event(user_id=None), event(action_type=None))

    assert {r["relationship"] for r in build_edges(df).collect()} == {"PROPOSE"}


def test_graph_is_a_graphframe_whose_edges_reference_existing_vertices(make_events):
    df = make_events(event(), event(user_id="usr_2", product_id="prod_9", seller_id="sel_9"))

    g = build_graph(df)

    assert isinstance(g, GraphFrame)
    ids = {r["id"] for r in g.vertices.collect()}
    assert all(r["src"] in ids and r["dst"] in ids for r in g.edges.collect())
    assert g.inDegrees.count() > 0  # le JAR GraphFrames est chargé


def test_compute_metrics_writes_readable_csv_files(make_events, tmp_path, monkeypatch):
    vertices_path, edges_path = tmp_path / "v.csv", tmp_path / "e.csv"
    monkeypatch.setattr(graph_module, "GRAPH_VERTICES_PATH", str(vertices_path))
    monkeypatch.setattr(graph_module, "GRAPH_EDGES_PATH", str(edges_path))

    compute_metrics(build_graph(make_events(event())))

    with open(vertices_path, encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 3
    with open(edges_path, encoding="utf-8") as f:
        assert {r["relationship"] for r in csv.DictReader(f)} == {"VUES", "PROPOSE"}
    assert sorted(p.name for p in tmp_path.iterdir()) == ["e.csv", "v.csv"]  # pas de fichier temporaire restant
