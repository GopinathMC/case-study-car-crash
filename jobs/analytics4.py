from data_extracter import extract_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,row_number,sum
from pyspark.sql.types import IntegerType




def transform_data(unit_df,spark):
    """
    Transform the raw data based on the business logic

    :param per_df: spark dataframe
    :param unit_df: spark dataframe
    :param spark: spark session object
    """

    w = Window.orderBy(col('TOT_INJ_DEATH_CNT').desc())
    sum_df = unit_df.filter(col('VEH_MAKE_ID')!='NA').withColumn('INJR_DEATH_CNT',col('DEATH_CNT')+col('TOT_INJRY_CNT')) \
                    .withColumn('INJR_DEATH_CNT',col('INJR_DEATH_CNT').cast(IntegerType())).select('VEH_MAKE_ID','INJR_DEATH_CNT')
    sum_df_agg = sum_df.groupBy('VEH_MAKE_ID').agg(sum(col('INJR_DEATH_CNT')).alias('TOT_INJ_DEATH_CNT'))
    veh_most_inju_df = sum_df_agg.withColumn('rn',row_number().over(w)).filter("rn >=5 and rn <=15") \
                                  .select('VEH_MAKE_ID').withColumnRenamed('VEH_MAKE_ID','MOST_INJR_DEATH_VEH_MAKE_ID')    
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
    unit_df = extract_data(spark,f"{config.get('source_data_path')}/Units_use.csv")
    out_df = transform_data(unit_df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS4",config.get('write_mode'))
    log.info("File pushed successfully")