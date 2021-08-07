from data_extracter import extract_data,extract_join_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,rank





def transform_data(per_df,unit_df,spark):
    """
    Transform the raw data based on the business logic

    :param per_df: spark dataframe
    :param unit_df: spark dataframe
    :param spark: spark session object
    """

    w = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())
    ethn_df = per_df.filter("PRSN_ETHNICITY_ID != 'NA' and PRSN_ETHNICITY_ID != 'UNKNOWN' and PRSN_ETHNICITY_ID != 'OTHER'").select('CRASH_ID','PRSN_ETHNICITY_ID')
    body_style_df = unit_df.filter("VEH_BODY_STYL_ID != 'NA' and VEH_BODY_STYL_ID != 'UNKNOWN' and VEH_BODY_STYL_ID != 'NOT REPORTED'").filter(~col('VEH_BODY_STYL_ID').like('OTHER%')).select('CRASH_ID','VEH_BODY_STYL_ID')
    join_df = extract_join_data(body_style_df,ethn_df,'inner','CRASH_ID')
    body_style_ethn_df = join_df.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count()
    body_style_ethn_top_df = body_style_ethn_df.withColumn('rn',rank().over(w)).filter("rn == 1").select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')
    return (
        body_style_ethn_top_df
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
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS5",config.get('write_mode'))
    log.info("File pushed successfully")