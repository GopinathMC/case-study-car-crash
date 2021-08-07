import json
import importlib
import argparse
from pyspark.sql import SparkSession
from jobs import logging


def _parse_arguments():
    """ 
    Parse arguments provided by spark-submit command
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    parser.add_argument("--config_folder", required=True)
    return parser.parse_args()


def main():
    """ 
    Main function excecuted by spark-submit command
    """
    args = _parse_arguments()

    config_folder_path = str(args.config_folder)
    with open(config_folder_path+"/config.json", "r") as config_file:
        config = json.load(config_file)

    spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()
    logger = logging.Log4j(spark)

    job_module = importlib.import_module(f"jobs.{args.job}")
    job_module.run_job(spark, config,logger)


if __name__ == "__main__":
    main()

