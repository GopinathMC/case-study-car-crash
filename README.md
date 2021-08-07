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
