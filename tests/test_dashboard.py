from types import SimpleNamespace

import pytest

from dashboard import app


@pytest.fixture
def graph_paths(tmp_path, monkeypatch):
    vertices = tmp_path / "vertices.csv"
    edges = tmp_path / "edges.csv"
    monkeypatch.setattr(app, "GRAPH_VERTICES_PATH", vertices)
    monkeypatch.setattr(app, "GRAPH_EDGES_PATH", edges)
    vertices.write_text("id,type,label\nu,User,Paris\np,Product,Mode\n", encoding="utf-8")
    edges.write_text("src,dst,relationship\nu,p,VUES\n", encoding="utf-8")
    return vertices, edges


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("contents", ["", '"guillemet non fermé', "invalid\nvalue\n"])
def test_load_graph_handles_unreadable_csvs(graph_paths, index, contents):
    graph_paths[index].write_text(contents, encoding="utf-8")

    assert app.load_graph() == (None, None)


def test_load_graph_counts_only_edges_with_existing_vertices(graph_paths):
    graph_paths[1].write_text(
        "src,dst,relationship\nu,p,VUES\nu,missing,VUES\nmissing,p,ACHAT\n",
        encoding="utf-8",
    )

    vertices, edges = app.load_graph()

    assert list(vertices["id"]) == ["u", "p"]
    assert list(edges["relationship"]) == ["VUES"]


def test_dashboard_refreshes_after_reading_empty_csv(graph_paths, monkeypatch):
    graph_paths[0].write_text("", encoding="utf-8")
    calls = []
    monkeypatch.setattr(app, "time", SimpleNamespace(sleep=calls.append))
    monkeypatch.setattr(app.st, "rerun", lambda: calls.append("rerun"))

    app.main()

    assert calls == [app.REFRESH_S, "rerun"]
