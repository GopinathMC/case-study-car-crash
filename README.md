# CASE-STUDY-CAR-CRASH
 PySpark Project perform various analytical operations on US accident data
 
## Project Structure
The basic project structure is shown as below

```
root/
  |-- jobs/
  |   |-- __init__.py
  |   |-- data_extracter.py
  |   |-- logging.py
  |   |-- analytics1.py
  |   |-- analytics2.py
  |   |-- analytics3.py
  |   |-- analytics4.py
  |   |-- analytics5.py
  |   |-- analytics6.py
  |   |-- analytics7.py
  |   |-- analytics8.py
  |-- main.py
  |-- config.json
  |-- spark-submit
  |   |-- spark-submit.sh
```

## Project Structure Explanation
### Jobs Module 
```root/jobs/```
1. This module contains the dependency python modules like ```data_extracter.py```,```logging.py``` and ETL modules for the various business analysis
2. ```data_extracter.py``` module contains following methods which will be imported in analytics$.py module to perform ETL
```
         |-- extract_data: get the input file path, create spark dataframe and return it
         |-- extract_join_data: get two spark dataframe,the type of join and joining columns to perform the join operation
         |-- push_data: get the spark dataframe and output file path to push the dataframe as csv file
```
3. ```logging.py``` This module contains a class that wraps the log4j object instantiated by the active SparkContext, enabling Log4j logging for PySpark using.
4. ```analytics$.py``` This module get the spark session object, config file, spark logger object from the main function```root/main.py``` and call necessary dependent modules, methods to perform ETL operation.
```
         |-- run_job: triggered by the main function and call the dependent methods to perform ETL operation.
         |-- transform_data: the actual business logic is written in this function. It converts the raw dataframe to the final desired dataframe
```
5. Following operations are performed by the respective analytics$.py modules


| s.no | module        | operation                                                                                                                                                                               |
|------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | analytics1.py | Get the number of crashes in which number of persons killed are male                                                                                                                    |
| 2    | analytics2.py | Get the number of two wheerlers booked for crashes                                                                                                                                      |
| 3    | analytics3.py | Get the state with highest number of accidents in which female are involved                                                                                                             |
| 4    | analytics4.py | Get the Top5th to 15th VEH_MAKE_ID which contributes large number of injuries and deaths                                                                                                |
| 5    | analytics5.py | For all vehicle body styles, get the top ethnic user group involved in crashes                                                                                                                  |
| 6    | analytics6.py | Get the top5 driver zip code where alcohol is the reason for crashes                                                                                                                    |
| 7    | analytics7.py | Count the distinct crash_id with no damage property, damage level > 4 and car avails insurance                                                                                     |
| 8    | analytics8.py | Get top5 vehicle makes where licensed drivers are charged for speeding related offenses, uses top10 vehicle colours,and car licensed with top25 states with highest number of offenses |

### Main module
```root/main.py```
1. This is the entry point for this spark application which will be called through spark-submit command.
2. It parse the spark-submit additional arguments using argparse python library, and calls the necessary ETL jobs. 
3. Here the spark session object, spark logger object is created, and triggers the respective analytics$.py file based on the --job parameter passed with spark-submit command. i.e spark-submit main.py --job analytics1, this will tells the main.py file to call analytics1.py file to perform necessary operation
4. This is the place where ```config.json``` file is read and pass it to the corresponding analytics$.py modules.

### Configuring input and output data folder
```root/config.json```
The config file looks like below
```json
{
  "app_name": "US_ACCIDENT_REPORT",
  "source_data_path": "/Users/dev/Documents/assignment/case_study/Data",
  "output_data_path": "/Users/dev/Documents/assignment/case_study/Data/OUTPUT",
  "write_mode": "overwrite"
}
```
a) app_name: Defines the spark application name
b) source_data_path: Defines the folder where input data files are stored
c) output_data_path: Defines the folder where output data file will be written. For each analytics$ job, the file will be written like $output_data_path/ANALYTICS$/
i.e. for analytics1.py analysis the output files will be written to $output_data_path/ANALYTICS1/
d) write_mode: Defines the spark write mode of the application. (i.e. overwrite, append)

###spark-submit
This folder contain the spark-submit.sh shell script shown as below, which can be triggered like ```sh spark-submit analytics$```
```shell
#!/bin/bash

analytics_id=$1

DEPEND_DIR=/Users/dev/Documents/assignment/case-study-car-crash/jobs/
SPARK_HOME=/Users/dev/Downloads/spark-3.0.1-bin-hadoop2.7
APP_DIR=/Users/dev/Documents/assignment/case-study-car-crash

cd $SPARK_HOME

bin/spark-submit \
--master local[*] \
--py-files $DEPEND_DIR"data_extracter.py",$DEPEND_DIR$analytics_id".py" \
--files $APP_DIR"/config.json" $APP_DIR"/main.py" \
--job $analytics_id \
--config_folder $APP_DIR
```
1. analytics_id: The input parameter for the spark_submit.sh. This decides which analytics job should run. It can be analytics1,analytics2,analytics3,analytics4,analytics5,analytics6,analytics7,analytics8.
2. DEPEND_DIR: The directory where the dependency module, ETL job modules are placed that will be send to spark application using spark-submit command.
3. SPARK_HOME: Spark home directory
4. APP_DIR: This application installed directory.

#### Spark-Submit command
1. --master: Runs the spark application on local mode
2. --py-files: Python dependency files which will be called by the main function.
3. --files: Passes the config file to the spark application.
4. --job: calls the corresponding analytics$.py module.
5. --config-folder: For parsing the config file by the main.py script

## Installation Steps and Running
1. Clone this repo or download the zip file, and place the case-study-car-crash folder in your local directory.i.e $app_local_dir
2. Go to spark-submit folder and edit the below properties in spark-submit.sh file as below
```
DEPEND_DIR=$app_local_dir/case-study-car-crash/jobs/
SPARK_HOME=$your-spark-installed-dir
APP_DIR=$app_local_dir
```
3. Open config.json file and define the input and output files data path
```json
source_data_path: $your-source-data-folder
output_data_path: $your-target-data-folder
```
4. The final step is to run the application. Move to $app_local_dir/spark-submit/ and call the spark-submit script as below
```
cd $app_local_dir/spark-submit
sh spark-submit.sh analytics1
```
5. The above command triggeres the analytics1.py module and the result data set will be found in $your-target-data-folder/ANALYTICS1/
