from data_extracter import extract_data,push_data
from pyspark.sql.functions import col,upper


def transform_data(raw_df,spark):
    """
    Transform the raw data based on the business logic

    :param raw_df: spark dataframe
    :param spark: spark session object
    """

    two_wheeler_crash = raw_df.filter(upper(col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%')).count()
    return (
        spark.createDataFrame([two_wheeler_crash], "integer").toDF("two_wheeler_crash_count")
        )


def run_job(spark,config,log):
    """
    Trigerred by the main function it calls extract,transform and load functions

    :param spark: spark session object
    :param config: config file
    :param log: spark logger object
    """

    log.info('Extracting Units_use csv file')
    df = extract_data(spark,f"{config.get('source_data_path')}/Units_use.csv")
    out_df = transform_data(df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS2",config.get('write_mode'))
    log.info("File pushed successfully")