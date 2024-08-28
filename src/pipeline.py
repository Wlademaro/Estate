from pyspark.sql import SparkSession
import logging
import traceback


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)


def create_spark_session(name: str, cluster: str) -> SparkSession:
    """
    Creates and returns a SparkSession with specified configurations.

    Parameters:
    name (str): The name of the Spark application.
    cluster (str): The master URL for the cluster.

    Returns:
    SparkSession: Configured SparkSession object.
    """
    if not all([name, cluster]):
        logging.error("Parameters (name, cluster) "
                      "are not provided or empty.")
        raise ValueError("All parameters (name, cluster) "
                         "must be provided and non-empty.")
    for i in [name, cluster]:
        if not isinstance(i, str):
            logging.error(f"Input variable {i} should be a string")
            raise TypeError(f"Input variable {i} should be a string")
    try:
        spark = SparkSession\
            .builder.appName(name)\
            .master(cluster)\
            .getOrCreate()
        logging.info("Spark session created successfully")
        return spark
    except Exception as e:
        logging.error("An error occurred: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        raise


spark = create_spark_session(name="Real_Estate_App", cluster="local[*]")


if __name__ == "__main__":
    pass
