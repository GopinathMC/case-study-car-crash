

def extract_data(spark,path):
    """
    Read source file from the given path

    :param spark: spark session object
    :param path: source file path string
    :return: spark dataframe
    """
    return (
        spark.read.format('csv').option('header','true').load(path)
        ) 

def extract_join_data(left_df,right_df,_type,col1,col2=None):
    """
    Join two dataframes based on join type and the join columns

    :param left_df: spark dataframe
    :param right_df: spark dataframe
    :param _type: join type
    :param col1: spark column name
    :param col2: spark column name
    """
    if col2 is None:
        join_df = left_df.join(right_df,on=[col1],how=_type)
    else: 
        join_df = left_df.join(right_df,left_df[col1]==right_df[col2],how=_type)
    return join_df


def push_data(fndf,path,_mode):
    """
    Write the spark dataframe to the output path
    
    :param fndf: spark dataframe
    :param path: output path string
    :param _mode: spark write mode	
    """	
    fndf.coalesce(1).write.mode(_mode).format('csv').option('header','true').save(path)

