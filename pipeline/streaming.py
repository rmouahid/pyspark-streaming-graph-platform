# Pipeline Spark Structured Streaming : lecture du stream JSON, agrégation par fenêtre glissante
# et construction du graphe GraphFrames à chaque micro-batch.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, window

from config.settings import (
    CHECKPOINT_PATH,
    DATA_PATH,
    SLIDE_DURATION,
    WATERMARK_DELAY,
    WINDOW_DURATION,
)
from pipeline.graph import build_graph, compute_metrics
from pipeline.schema import EVENT_SCHEMA


def read_stream(spark: SparkSession, path: str = DATA_PATH) -> DataFrame:
    return (
        spark.readStream
        .schema(EVENT_SCHEMA)
        # Limite les fichiers traités par déclenchement pour éviter une surcharge au démarrage.
        .option("maxFilesPerTrigger", 10)
        # Un JSON invalide ou mal typé est ignoré au lieu de produire une ligne de nulls.
        .option("mode", "DROPMALFORMED")
        .json(path)
    )


def build_action_window_agg(stream_df: DataFrame) -> DataFrame:
    return (
        stream_df
        # Le watermark doit être déclaré avant groupBy pour que Spark borne l'état de la fenêtre.
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
            # Fenêtre glissante : durée 1 min, avance toutes les 30s.
            window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("action_type"),
        )
        .agg(count("*").alias("event_count"))
    )


def _process_batch(batch_df: DataFrame, epoch_id: int) -> None:
    # Le batch est lu trois fois (test de vacuité, sommets, arêtes) : on le garde en
    # mémoire plutôt que de relire les fichiers JSON à chaque fois.
    batch_df.persist()
    try:
        # On ignore les micro-batches vides pour ne pas écrire un graphe vide.
        if batch_df.isEmpty():
            return
        compute_metrics(build_graph(batch_df))
    finally:
        batch_df.unpersist()


def start_queries(spark: SparkSession, stream_df: DataFrame):
    # q1 — démontre : Windowing (glissant), Watermarking, Output Mode "update"
    # "update" émet uniquement les lignes modifiées à chaque déclenchement.
    q1 = (
        build_action_window_agg(stream_df).writeStream
        .outputMode("update")
        .format("memory")
        .queryName("action_counts")
        .option("checkpointLocation", CHECKPOINT_PATH + "action_counts")
        .trigger(processingTime="5 seconds")
        .start()
    )
    # q2 — démontre : GraphFrames via foreachBatch (export CSV pour le dashboard)
    q2 = (
        stream_df.writeStream
        .foreachBatch(_process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "graph")
        .trigger(processingTime="5 seconds")
        .start()
    )
    return q1, q2


if __name__ == "__main__":
    from pipeline.spark_session import get_spark_session

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    stream_df = read_stream(spark)
    start_queries(spark, stream_df)

    print("Pipeline démarrée. Ctrl+C pour arrêter.")
    spark.streams.awaitAnyTermination()
