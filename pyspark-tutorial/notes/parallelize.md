# PySpark parallelize()

**Source: https://sparkbyexamples.com/pyspark/pyspark-parallelize-create-rdd/**

- PySpark parallelize() is a function in SparkContext and is used to create an RDD from a list collection.
- Resilient Distributed Datasets (RDD) is a fundamental data structure of PySpark, It is an immutable distributed collection of objects. Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.

## Using sc.parallelize on PySpark Shell or REPL
- PySpark shell provides SparkContext variable “sc”, use sc.parallelize() to create an RDD.
  ```python
  rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
  ```

## Using PySpark sparkContext.parallelize() in application
- Snippet:
  ```python
  import pyspark
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
  sparkContext=spark.sparkContext

  rdd=sparkContext.parallelize([1,2,3,4,5])
  rddCollect = rdd.collect()
  print("Number of Partitions: "+str(rdd.getNumPartitions()))
  print("Action: First element: "+str(rdd.first()))
  print(rddCollect)
  ```
- parallelize() function also has another signature which additionally takes integer argument to specifies the number of partitions. Partitions are basic units of parallelism in PySpark.
- Remember, RDDs in PySpark are a collection of partitions.