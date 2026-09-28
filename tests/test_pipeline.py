import json

from conftest import event
from pipeline.schema import EVENT_SCHEMA
from pipeline.streaming import build_action_window_agg, read_stream


def counts_by_window(df):
    return sorted(
        (str(r["window"]["start"]), str(r["window"]["end"]), r["action_type"], r["event_count"])
        for r in df.collect()
    )


def test_events_are_counted_per_sliding_window_and_action(make_events):
    df = make_events(
        event(timestamp="2026-06-04T14:32:01", action_type="VUES"),
        event(timestamp="2026-06-04T14:32:10", action_type="VUES"),
        event(timestamp="2026-06-04T14:32:40", action_type="ACHAT"),
    )

    result = counts_by_window(build_action_window_agg(df))

    # Fenêtres de 1 min glissant de 30 s : chaque événement tombe dans deux fenêtres
    assert result == [
        ("2026-06-04 14:31:30", "2026-06-04 14:32:30", "VUES", 2),
        ("2026-06-04 14:32:00", "2026-06-04 14:33:00", "ACHAT", 1),
        ("2026-06-04 14:32:00", "2026-06-04 14:33:00", "VUES", 2),
        ("2026-06-04 14:32:30", "2026-06-04 14:33:30", "ACHAT", 1),
    ]


def test_output_columns(make_events):
    df = build_action_window_agg(make_events(event()))

    assert df.columns == ["window", "action_type", "event_count"]


def test_events_without_timestamp_are_not_counted(make_events):
    df = make_events(event(), event(timestamp=None))

    assert sum(r["event_count"] for r in build_action_window_agg(df).collect()) == 2  # 1 événement × 2 fenêtres


def test_empty_batch_gives_no_window(make_events):
    assert build_action_window_agg(make_events()).count() == 0


def test_stream_reader_uses_the_declared_schema(spark, tmp_path):
    stream = read_stream(spark, str(tmp_path))

    assert stream.isStreaming
    assert stream.schema == EVENT_SCHEMA


def test_stream_reads_simulator_files_in_utc(spark, tmp_path):
    from simulator.event_producer import write_event

    write_event(event(timestamp="2026-06-04T14:32:01+00:00"), str(tmp_path))
    run_stream_once(spark, tmp_path, "utc_check")

    (row,) = spark.sql("select date_format(timestamp, 'yyyy-MM-dd HH:mm:ss') as ts from utc_check").collect()
    assert row["ts"] == "2026-06-04 14:32:01"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def run_stream_once(spark, path, name):
    query = (
        read_stream(spark, str(path)).writeStream
        .format("memory").queryName(name)
        .option("checkpointLocation", str(path / "_checkpoint"))
        .start()
    )
    query.processAllAvailable()
    query.stop()
    return spark.table(name).collect()


def test_malformed_json_files_are_dropped(spark, tmp_path):
    from simulator.event_producer import write_event

    write_event(event(timestamp="2026-06-04T14:32:01+00:00"), str(tmp_path))
    (tmp_path / "truncated.json").write_text('{"timestamp": "2026-06-04T14:32:02+00:00", "user_id": ')
    (tmp_path / "wrong_type.json").write_text(json.dumps(event(price="cher")))

    rows = run_stream_once(spark, tmp_path, "malformed_check")

    assert [r["user_id"] for r in rows] == ["usr_1000"]


def test_simulator_temporary_files_are_ignored(spark, tmp_path):
    (tmp_path / ".event_1.json.tmp").write_text('{"timestamp": ')

    assert run_stream_once(spark, tmp_path, "hidden_check") == []


def test_process_batch_writes_the_graph_and_skips_empty_batches(make_events, tmp_path, monkeypatch):
    from pipeline import graph as graph_module
    from pipeline.streaming import _process_batch

    vertices_path = tmp_path / "v.csv"
    monkeypatch.setattr(graph_module, "GRAPH_VERTICES_PATH", str(vertices_path))
    monkeypatch.setattr(graph_module, "GRAPH_EDGES_PATH", str(tmp_path / "e.csv"))

    _process_batch(make_events(), 0)
    assert not vertices_path.exists()

    _process_batch(make_events(event()), 1)
    assert vertices_path.exists()
