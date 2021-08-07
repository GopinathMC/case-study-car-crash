from data_extracter import extract_data,extract_join_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,regexp_extract




def transform_data(charge_df,damage_df,unit_df,spark):
    """
    Transform the raw data based on the business logic

    :param charge_df: charge use spark dataframe
    :param damage_df: damage use spark dataframe
    :param unit_df: units use spark dataframe
    :param spark: spark session object
    """

    no_ins_df = charge_df.filter(col('CHARGE').contains('NO')).filter(col('CHARGE').contains('INSURANCE')).select('CRASH_ID').withColumnRenamed('CRASH_ID','I_CRASH_ID')
    damage_prop_df = damage_df.select('CRASH_ID').withColumnRenamed('CRASH_ID','D_CRASH_ID')

    join_df1 = extract_join_data(unit_df,no_ins_df,'left','CRASH_ID','I_CRASH_ID')
    unit_df_j1 = join_df1.filter("I_CRASH_ID is null").select('CRASH_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID')
    join_df2 = extract_join_data(unit_df_j1,damage_prop_df,'left','CRASH_ID','D_CRASH_ID')
    unit_df_j2 = join_df2.filter("D_CRASH_ID is null").select('CRASH_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID') 
    crash_id_count = unit_df_j2.withColumn('DMAG1_RANGE',regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0)) \
	                        .withColumn('DMAG2_RANGE',regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0)) \
	                        .filter("DMAG1_RANGE > 4 or DMAG2_RANGE > 4") \
	                        .select('CRASH_ID').distinct().count()
    return (
        spark.createDataFrame([crash_id_count], "integer").toDF("SEV_CRASHES_COUNT")
        )


def run_job(spark,config,log):
    """
    Trigerred by the main function it calls extract,transform and load functions

    :param spark: spark session object
    :param config: config file
    :param log: spark logger object
    """	

    log.info('Extracting source csv files')
    charge_df = extract_data(spark,f"{config.get('source_data_path')}/Charges_use.csv")
    unit_df = extract_data(spark,f"{config.get('source_data_path')}/Units_use.csv")
    damage_df = extract_data(spark,f"{config.get('source_data_path')}/Damages_use.csv")
    out_df = transform_data(charge_df,damage_df,unit_df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS7",config.get('write_mode'))
    log.info("File pushed successfully")