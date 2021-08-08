from data_extracter import extract_data,extract_join_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,row_number




def transform_data(per_df,charge_df,unit_df,spark):
    """
    Transform the raw data based on the business logic
	
    :param per_df: primary person use spark dataframe
    :param charge_df: charge use spark dataframe
    :param unit_df: units use spark dataframe
    :param spark: spark session object
    """

    w = Window.orderBy(col('count').desc())
    charge_speed = charge_df.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID','CHARGE')#charge corresponding to speed issues
    top_clrs_agg = unit_df.filter("VEH_COLOR_ID != 'NA'").select('VEH_COLOR_ID').groupBy('VEH_COLOR_ID').count()
    top_clrs_df = top_clrs_agg.withColumn('rn',row_number().over(w)).filter("rn <=10").select('VEH_COLOR_ID')#top10 vehicle colors used

    excl_states = ['NA','UN']
    top_states_agg = unit_df.filter(~col('VEH_LIC_STATE_ID').isin(excl_states)).select('VEH_LIC_STATE_ID').groupBy('VEH_LIC_STATE_ID').count()
    top_states_df = top_states_agg.withColumn('rn',row_number().over(w)).filter("rn <=25").select('VEH_LIC_STATE_ID')
    
    unit_df_j1 = extract_join_data(unit_df,top_clrs_df,'inner','VEH_COLOR_ID').select('CRASH_ID','VEH_MAKE_ID','VEH_LIC_STATE_ID')
    unit_df_j2 = extract_join_data(unit_df_j1,charge_speed,'inner','CRASH_ID').select('CRASH_ID','VEH_MAKE_ID','VEH_LIC_STATE_ID')
    unit_df_j3 = extract_join_data(unit_df_j2,top_states_df,'inner','VEH_LIC_STATE_ID').select('CRASH_ID','VEH_MAKE_ID')

    licensed_per_df = per_df.filter("DRVR_LIC_CLS_ID != 'UNLICENSED'").select('CRASH_ID')
    
    fndf = extract_join_data(unit_df_j3,licensed_per_df,'inner','CRASH_ID').select('CRASH_ID','VEH_MAKE_ID')

    fndf_agg = fndf.groupBy('VEH_MAKE_ID').count()
    fn_df = fndf_agg.withColumn('rn',row_number().over(w)).filter("rn <= 5").select('VEH_MAKE_ID')
    return (
        fn_df
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
    charge_df = extract_data(spark,f"{config.get('source_data_path')}/Charges_use.csv")
    unit_df = extract_data(spark,f"{config.get('source_data_path')}/Units_use.csv")
    out_df = transform_data(per_df,charge_df,unit_df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS8_T",config.get('write_mode'))
    log.info("File pushed successfully")