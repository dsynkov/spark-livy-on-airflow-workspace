import inspect
import os
import textwrap
from typing import Dict, Any
from livyondocker.livy import LivyAPI


def _strip_function_signature(source):
    return "\n".join(source.split("\n")[1:])


def run_spark_example(spark) -> None:

    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StructType

    rows = [
        [1, 100],
        [2, 200],
        [3, 300],
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)

    highest_salary = df.agg({"salary": "max"}).collect()[0]["max(salary)"]

    second_highest_salary = (
        df.filter(f"`salary` < {highest_salary}")
        .orderBy("salary", ascending=False)
        .select("salary")
        .limit(1)
    )

    second_highest_salary.show()


def run_livy_session_example() -> None:

    livy_host = os.environ.get("LIVY_HOST", "http://localhost:8998")

    livy = LivyAPI(livy_host)

    livy.check_session_availability("pyspark")

    source = inspect.getsource(run_spark_example)

    livy.execute_statement(textwrap.dedent(_strip_function_signature(source)))


def run_livy_batch_example(data: Dict[str, Any]) -> None:

    livy_host = os.environ.get("LIVY_HOST", "http://localhost:8998")

    livy = LivyAPI(livy_host)

    livy.execute_batch(data)
