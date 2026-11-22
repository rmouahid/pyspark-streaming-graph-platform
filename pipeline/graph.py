# Construction et export du graphe GraphFrames à partir d'un micro-batch d'événements.
# Les sommets (User, Product, Seller) et les arêtes typées sont extraits puis écrits en CSV
# pour être lus par le dashboard.

import csv
import os
from tempfile import NamedTemporaryFile

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import GRAPH_EDGES_PATH, GRAPH_VERTICES_PATH


def build_vertices(batch_df: DataFrame) -> DataFrame:
    users = batch_df.filter(F.col("user_id").isNotNull()).select(
        F.col("user_id").alias("id"),
        F.lit("User").alias("type"),
        F.col("user_city").alias("label"),
    )
    products = batch_df.filter(F.col("product_id").isNotNull()).select(
        F.col("product_id").alias("id"),
        F.lit("Product").alias("type"),
        F.col("product_cat").alias("label"),
    )
    sellers = batch_df.filter(F.col("seller_id").isNotNull()).select(
        F.col("seller_id").alias("id"),
        F.lit("Seller").alias("type"),
        F.col("seller_id").alias("label"),
    )
    # Un même nœud peut apparaître dans plusieurs événements du batch, on déduplique.
    return users.union(products).union(sellers).dropDuplicates(["id"])


def build_edges(batch_df: DataFrame) -> DataFrame:
    user_edges = batch_df.filter(
        F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("action_type").isin("AIME", "VUES", "ACHAT")
    ).select(
        F.col("user_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.col("action_type").alias("relationship"),
    )
    seller_edges = batch_df.filter(
        F.col("seller_id").isNotNull() & F.col("product_id").isNotNull()
    ).select(
        F.col("seller_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.lit("PROPOSE").alias("relationship"),
    # Un vendeur ne propose qu'une seule arête par produit dans le graphe.
    ).dropDuplicates(["src", "dst"])
    return user_edges.union(seller_edges)


def build_graph(batch_df: DataFrame) -> GraphFrame:
    return GraphFrame(build_vertices(batch_df), build_edges(batch_df))


def _write_csv(df: DataFrame, path: str) -> None:
    # df.write.csv() produit un répertoire de part-files ; on utilise collect()
    # pour écrire un fichier unique directement lisible par le dashboard.
    rows = df.collect()
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    with NamedTemporaryFile(mode="w", dir=directory, newline="", encoding="utf-8",
                            delete=False) as tmp:
        tmp_name = tmp.name
        try:
            writer = csv.DictWriter(tmp, fieldnames=df.columns)
            writer.writeheader()
            writer.writerows(row.asDict() for row in rows)
        except BaseException:
            tmp.close()
            os.unlink(tmp_name)
            raise
    try:
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def compute_metrics(graph: GraphFrame) -> None:
    _write_csv(graph.vertices, GRAPH_VERTICES_PATH)
    _write_csv(graph.edges,    GRAPH_EDGES_PATH)
