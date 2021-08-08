from data_extracter import extract_data,push_data
from pyspark.sql import Window
from pyspark.sql.functions import col,row_number




def transform_data(per_df,charge_df,unit_df,spark):
    """
    Transform the raw and return top5 vehicle makes where licensed drivers are charged for speeding related offenses, 
    uses top10 vehicle colours,and car licensed with top25 states with highest number of offenses
	
    :param per_df: primary person use spark dataframe
    :param charge_df: charge use spark dataframe
    :param unit_df: units use spark dataframe
    :param spark: spark session object
    :return: spark dataframe
    """

    w = Window.orderBy(col('count').desc())
    charge_speed = charge_df.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID','UNIT_NBR','CHARGE')#charge corresponding to speed issues
    top_clrs_agg = unit_df.filter("VEH_COLOR_ID != 'NA'").select('VEH_COLOR_ID').groupBy('VEH_COLOR_ID').count()
    top_clrs_df = top_clrs_agg.withColumn('rn',row_number().over(w)).filter("rn <=10").select('VEH_COLOR_ID')#top10 vehicle colors used

    excl_states = ['NA','UN']
    top_states_agg = unit_df.filter(~col('VEH_LIC_STATE_ID').isin(excl_states)).select('VEH_LIC_STATE_ID').groupBy('VEH_LIC_STATE_ID').count()
    top_states_df = top_states_agg.withColumn('rn',row_number().over(w)).filter("rn <=25").select('VEH_LIC_STATE_ID')
    
    unit_df_j1 = unit_df.join(top_clrs_df,on=['VEH_COLOR_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
    unit_df_j2 = unit_df_j1.join(charge_speed,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
    unit_df_j3 = unit_df_j2.join(top_states_df,on=['VEH_LIC_STATE_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID')

    licensed_per_df = per_df.filter("DRVR_LIC_CLS_ID != 'UNLICENSED'").select('CRASH_ID','UNIT_NBR')
    fndf = unit_df_j3.join(licensed_per_df,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','VEH_MAKE_ID')
    fndf_agg = fndf.groupBy('VEH_MAKE_ID').count()
    fn_df = fndf_agg.withColumn('rn',row_number().over(w)).filter("rn <= 5").select('VEH_MAKE_ID')
    return fn_df
        


def run_job(spark,config,log):
    """
    Trigerred by the main function it calls extract,transform and load functions

    :param spark: spark session object
    :param config: config file
    :param log: spark logger object
    :return None
    """	

    log.info('Extracting source csv files')

    per_df = extract_data(spark,f"{config.get('source_data_path')}/Primary_Person_use.csv")

    # pulling distinct rows since there is data duplicacy in Units use and Charges data
    charge_df = extract_data(spark,f"{config.get('source_data_path')}/Charges_use.csv").distinct()
    unit_df = extract_data(spark,f"{config.get('source_data_path')}/Units_use.csv").distinct()

    out_df = transform_data(per_df,charge_df,unit_df,spark)
    log.info('All transformations done and writing to output path.......')
    push_data(out_df,f"{config.get('output_data_path')}/ANALYTICS8",config.get('write_mode'))
    log.info("File pushed successfully")
    return None