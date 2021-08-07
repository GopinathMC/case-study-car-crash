from data_extracter import extract_data,extract_join_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,row_number




def transform_data(per_df,unit_df,spark):
    """
    Transform the raw data based on the business logic

    :param per_df: spark dataframe
    :param unit_df: spark dataframe
    :param spark: spark session object
    """

    w = Window.orderBy(col('count').desc())
    injur_df = per_df.filter("TOT_INJRY_CNT==1 or DEATH_CNT==1").select('CRASH_ID')
    left_df = unit_df.filter(col('VEH_MAKE_ID')!='NA').select('CRASH_ID','VEH_MAKE_ID')
    join_df = extract_join_data(left_df,injur_df,'inner','CRASH_ID')
    veh_agg_df = join_df.groupBy('VEH_MAKE_ID').count()	

    veh_most_inju_df = veh_agg_df.withColumn('rn',row_number().over(w)).filter("rn >=5 and rn <=15").select('VEH_MAKE_ID').withColumnRenamed('VEH_MAKE_ID','MOST_INJURED_VEH_MAKE_ID')
    return (
        veh_most_inju_df
        )


def run_job(spark,config,log):
    """
    Trigerred by the main function it calls extract,transform and load functions

    :param spark: spark session object
    :param config: config file
    :param log: spark logger object
    """	

    log.info('Extracting source csv files')
    per_df = extract_data(spark,f"{config.get('source_data_path')}/Primary_Person_use.csv")
    unit_df = extract_data(spark,f"{config.get('source_data_path')}/Units_use.csv")
    out_df = transform_data(per_df,unit_df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS4",config.get('write_mode'))
    log.info("File pushed successfully")