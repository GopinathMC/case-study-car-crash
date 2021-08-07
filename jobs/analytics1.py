from data_extracter import extract_data,push_data


def transform_data(raw_df,spark):
    """
    Transform the raw data based on the business logic

    :param raw_df: spark dataframe
    :param spark: spark session object
    """
    
    male_killed_df = raw_df.filter("PRSN_GNDR_ID =='MALE' and  DEATH_CNT==1").count()
    return (
        spark.createDataFrame([male_killed_df], "integer").toDF("male_killed_count")
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
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS1",config.get('write_mode'))
    log.info("File pushed successfully")




