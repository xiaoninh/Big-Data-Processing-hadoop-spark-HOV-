# Big Data Management
### This repository contians practices of current technologies for managing and processing Big Data, including High Order Functions, Hadoop, Spark, and  SQL,etc. 


--------------------------------------------------------------------------------------------------------------------------

## Hadoop/pyspark command

### Imformation below is for personal user as reminder, may not be generally applicable

# PySpark
export PYSPARK_DRIVER_PYTHON=\`which jupyter` export PYSPARK_DRIVER_PYTHON_OPTS=notebook export SPARK_HOME=/anaconda3/lib/python3.7/site-packages/pyspark pyspark


# Connect to HPC

## On Campus:
ssh xh1163@dumbo.es.its.nyu.edu
## Off Campus:
ssh xh1163@gw.hpc.nyu.edu

ssh prince.hpc.nyu.edu

ssh dumbo.hpc.nyu.edu

## Initialize
module load spark/2.4.0 python/gnu/3.4.4 gcc/5.3.0

export PYSPARK_PYTHON=\`which python`

module load pygdal/2.2.0.3

## Run

spark-submit --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
--executor-cores 5 --num-executors 10 --py-files ... \
--files hdfs:///tmp/500cities_tracts.geojson,hdfs:///tmp/drug_sched2.txt,hdfs:///tmp/drug_illegal.txt \ final_challenge.py hdfs:///tmp/tweets-100m.csv

spark-submit --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
--executor-cores 5 --num-executors 10 --files hdfs:///tmp/500cities_tracts.geojson,hdfs:///tmp/drug_sched2.txt,hdfs:///tmp/drug_illegal.txt \ final_challenge.py hdfs:///tmp/tweets-100m.csv


## Upload or Download file from Hadoop

(in local) scp file_name xh1163@dumbo.es.its.nyu.edu:~/.

hadoop fs -get hdfs:///tmp/bdm/500cities_tracts.geojson ~/

(in local) scp xh1163@dumbo.hpc.nyu.edu:~/filename /Users/xiaoninghe



.
