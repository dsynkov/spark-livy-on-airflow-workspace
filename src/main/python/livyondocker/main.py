from livyondocker.examples import run_spark_example
from livyondocker.examples import run_livy_session_example
from livyondocker.examples import run_livy_batch_example
from livyondocker.utils import get_spark_context

if __name__ == "__main__":

    # Traditional Spark executed against Docker container
    spark = get_spark_context("employees")
    run_spark_example(spark)
    spark.stop()

    # Livy using session API
    run_livy_session_example()

    # Livy using batch API
    data = {
        'file': '/target/livyondocker-1.0.0.jar',
        'className': 'com.livyondocker.LivyOnDockerApp',
        'numExecutors': 1,
        'conf': {'spark.shuffle.compress': 'false'},
        'args': ['/data/']
    }

    run_livy_batch_example(data)

