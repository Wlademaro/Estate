import logging
import traceback
import json

import yaml
import requests
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

from schemas import schema


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
    logging.info("Spark Session is creating")
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
        spark = SparkSession.builder\
            .appName(name)\
            .master(cluster)\
            .getOrCreate()
        logging.info("Spark session created successfully")
        return spark
    except Exception as e:
        logging.error("An error occurred: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        raise


def load_config() -> yaml:
    """
    Loads a YAML configuration file.

    Returns:
        dict: The parsed configuration as a dictionary.
    """
    logging.info("Config file is loading")
    try:
        with open("src/config.yaml", "r") as file:
            config = yaml.safe_load(file)
        logging.info("Config file is loaded")
        return config
    except FileNotFoundError as f:
        logging.error("File was not found: %s", f)
        raise
    except Exception as e:
        logging.error("An error occurred: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        raise


def check_total_pages(config: dict, location: str, period: str) -> int:
    """
    Fetches the total number of pages from the API
    for a given location and time period for builing offsets dataframe.

    Parameters:
    config (dict): Configuration dictionary containing the API URL and headers.
    location (str): The location for which to fetch data.
    period (str): The time period for which to fetch data.

    Returns:
    int: The total number of pages available from the API.
    """
    logging.info("Checking the total number of pages for\
                 location: %s and period: %s", location, period)
    url = config.get("url")
    headers = config.get("headers")
    params = {"location": location, "soldInLast": period}

    try:
        response = requests.post(url=url, headers=headers, json=params)
        response.raise_for_status
        data = response.json()
        total_pages = data.get("totalPages")
        if total_pages is None:
            logging.error("Total pages key not found in the response.")
            raise ValueError("Missing 'total_pages' in the API response.")
        logging.info("Total pages retrieved: %d", total_pages)
        return total_pages
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch total pages: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        raise


def create_offsets_df(spark: SparkSession, total_pages: int) -> DataFrame:
    """
    Creates a Spark DataFrame with page numbers
      to be used as input for fetching data from the API.

    Parameters:
    spark (SparkSession): The active Spark session.
    total_pages (int): The total number of pages available from the API.

    Returns:
    DataFrame: A Spark DataFrame with one column 'page' containing
    page numbers from 1 to total_pages.
    """
    if total_pages <= 0:
        logging.error("Invalid total_pages value: %d.\
                      Must be a positive integer.", total_pages)
        raise ValueError("total_pages must be a positive integer.")
    logging.info("Starting to create offsets DataFrame for %d total pages.",
                 total_pages)
    try:
        df = spark.range(1, total_pages + 1).select(F.col("id").alias("page"))
        logging.info("Offsets DataFrame created with %d rows.", df.count())
        return df
    except Exception as e:
        logging.exception("An unexpected error occurred\
                          while creating the offsets DataFrame: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        raise


@F.udf(returnType=T.StringType())
def fetch_data(page: int, location: str,
               period: str, url: str, key: str, host: str, cnt_type: str) -> str:
    """
    Fetches data from the API for a specific page number, location, and period.

    Parameters:
    page (int): The page number to fetch.
    location (str): The location for which to fetch data.
    period (str): The time period for which to fetch data.
    url (str): The API URL.
    headers (dict): The headers for the API request.

    Returns:
    str: The response text from the API.
    """
    params = {
        "page": page,
        "location": location,
        "soldInLast": period
    }
    headers = {"x-rapidapi-key": key, "x-rapidapi-host": host, "Content-Type": cnt_type}
    response = requests.post(url=url, json=params, headers=headers)
    response.raise_for_status()
    return response.text


def create_dataframe(spark: SparkSession,
                     config: dict, location: str, period: str) -> DataFrame:
    """
    Creates a DataFrame by fetching and parsing data
    from the API using the specified location and period.

    Parameters:
    spark (SparkSession): The active Spark session.
    config (dict): Configuration dictionary containing the API URL,
    headers, and schema.
    location (str): The location for which to fetch data.
    period (str): The time period for which to fetch data.

    Returns:
    DataFrame: A Spark DataFrame containing the parsed API responses.
    """
    logging.info("Creating dataframe with extracted responses from API")
    total_pages = check_total_pages(config, location, period)
    offsets_df = create_offsets_df(spark, total_pages)

    url = config.get("url")
    key = config["headers"]["x-rapidapi-key"]
    host = config["headers"]["x-rapidapi-host"]
    cnt_type = config["headers"]["Content-Type"]

    response_df = offsets_df\
        .withColumn("response",
                    fetch_data("page", F.lit(location),
                               F.lit(period),
                               F.lit(url),
                               F.lit(key),
                               F.lit(host),
                               F.lit(cnt_type)))\
        .withColumn("parsed_response", F.from_json(F.col("response"), schema))

    return response_df


spark = create_spark_session(name="Real_Estate_App", cluster="local[*]")

config = load_config()

df = create_dataframe(spark, config, "Philadelphia, PA", "1w")

df.write.mode("overwrite").json("test.json")

# if __name__ == "__main__":
#     pass
