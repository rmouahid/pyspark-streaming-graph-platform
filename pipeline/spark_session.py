# Initialisation de la SparkSession avec les dépendances GraphFrames.

import os
import sys

from pyspark.sql import SparkSession
from config.settings import SPARK_APP_NAME, GRAPHFRAMES_JAR


def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    # Les workers Python doivent tourner sur le même interpréteur que le driver :
    # sinon PySpark prend le `python3` du PATH et échoue (PYTHON_VERSION_MISMATCH)
    # dès qu'on lance le projet sans activer le virtualenv. PySpark ne lit que la
    # variable d'environnement, avant la création du contexte.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", GRAPHFRAMES_JAR)
        .config("spark.executor.memory", "2g")
        # Réduit le nombre de partitions shuffle (défaut 200, trop élevé en local).
        .config("spark.sql.shuffle.partitions", "4")
        # Le schéma est défini manuellement dans schema.py, on désactive l'inférence.
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Le simulateur horodate en UTC : on lit et on fenêtre en UTC quel que soit
        # le fuseau de la machine.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
