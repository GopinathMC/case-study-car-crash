#!/bin/bash

analytics_id=$1

DEPEND_DIR=/Users/dev/Documents/assignment/case-study-car-crash/jobs/
SPARK_HOME=/Users/dev/Downloads/spark-3.0.1-bin-hadoop2.7
APP_DIR=/Users/dev/Documents/assignment/case-study-car-crash

cd $SPARK_HOME

bin/spark-submit --master local[*] --py-files $DEPEND_DIR"data_extracter.py",$DEPEND_DIR$analytics_id".py" --files $APP_DIR"/config.json" $APP_DIR"/main.py" --job $analytics_id --config_folder $APP_DIR

