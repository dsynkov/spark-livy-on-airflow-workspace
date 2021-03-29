import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import logging


def get_spark_context(app_name: str) -> SparkSession:
    """
    Helper to manage the `SparkContext` and keep all of our
    configuration params in one place. See below comments for details:
        |_ https://github.com/bitnami/bitnami-docker-spark/issues/18#issuecomment-700628676
        |_ https://github.com/leriel/pyspark-easy-start/blob/master/read_file.py
    """

    conf = SparkConf()

    conf.setAll(
        [
            (
                "spark.master",
                os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            ),
            ("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "local[*]")),
            ("spark.submit.deployMode", "client"),
            ("spark.driver.bindAddress", "0.0.0.0"),
            ("spark.app.name", app_name),
        ]
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()


def init_logger(level=logging.INFO):
    logger = logging.getLogger()
    logging.captureWarnings(True)

    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)

    format_str = "%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    formatter = logging.Formatter(format_str, date_format)
    stream_handler.setFormatter(formatter)

    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(stream_handler)

    return logger
