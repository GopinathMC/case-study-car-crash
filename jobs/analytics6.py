from data_extracter import extract_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,row_number



def transform_data(raw_df,spark):
    """
    Transform the raw data based on the business logic

    :param raw_df: spark dataframe
    :param spark: spark session object
    """

    w = Window.orderBy(col('count').desc())
    drv_zip_alc_df = raw_df.filter("PRSN_ALC_RSLT_ID == 'Positive' and DRVR_ZIP is not null").select('DRVR_ZIP').groupBy('DRVR_ZIP').count()
    drv_alc_zip_top5_df = drv_zip_alc_df.withColumn('rn',row_number().over(w)).filter("rn<=5").select('DRVR_ZIP')
    return (
        drv_alc_zip_top5_df
        )


def run_job(spark,config,log):
    """
    Trigerred by the main function it calls extract,transform and load functions

    :param spark: spark session object
    :param config: config file
    :param log: spark logger object
    """	

    log.info('Extracting source csv files')
    df = extract_data(spark,f"{config.get('source_data_path')}/Primary_Person_use.csv")
    out_df = transform_data(df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS6",config.get('write_mode'))
    log.info("File pushed successfully")