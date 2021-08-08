

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


def push_data(fndf,path,_mode):
    """
    Write the spark dataframe to the output path
    
    :param fndf: spark dataframe
    :param path: output path string
    :param _mode: spark write mode
    :return: None	
    """	
    fndf.coalesce(1).write.mode(_mode).format('csv').option('header','true').save(path)
    return None

