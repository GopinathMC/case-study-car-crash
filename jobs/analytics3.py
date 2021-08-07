from data_extracter import extract_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,rank



def transform_data(raw_df,spark):
    """
    Transform the raw data based on the business logic

    :param raw_df: spark dataframe
    :param spark: spark session object
    """

    w = Window.orderBy(col('count').desc())
    excl_states = ['NA','Unknown','Other']
    most_acc_female_df = raw_df.filter("PRSN_GNDR_ID=='FEMALE'").filter(~col('DRVR_LIC_STATE_ID').isin(excl_states)).select('DRVR_LIC_STATE_ID').groupBy('DRVR_LIC_STATE_ID').count()
    fn_fem_acc_df = most_acc_female_df.withColumn('rn',rank().over(w)).filter(col('rn')==1).select('DRVR_LIC_STATE_ID').withColumnRenamed('DRVR_LIC_STATE_ID','MOST_FEM_ACCIDENT_STATE')
    return (
        fn_fem_acc_df
        )


def run_job(spark,config,log):
    """
    Trigerred by the main function it calls extract,transform and load functions

    :param spark: spark session object
    :param config: config file
    :param log: spark logger object
    """

    log.info('Extracting Primary_Person_use csv file')
    df = extract_data(spark,f"{config.get('source_data_path')}/Primary_Person_use.csv")
    out_df = transform_data(df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS3",config.get('write_mode'))
    log.info("File pushed successfully")