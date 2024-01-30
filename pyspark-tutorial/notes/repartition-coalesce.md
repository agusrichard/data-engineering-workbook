# PySpark Repartition() vs Coalesce()

**Source: https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-coalesce/**


- Let’s see the difference between PySpark repartition() vs coalesce(), repartition() is used to increase or decrease the RDD/DataFrame partitions whereas the PySpark coalesce() is used to only decrease the number of partitions in an efficient way.
- One important point to note is, PySpark repartition() and coalesce() are very expensive operations as they shuffle the data across many partitions hence try to minimize using these as much as possible.

## PySpark RDD Repartition() vs Coalesce()
- In RDD, you can create parallelism at the time of the creation of an RDD using parallelize(), textFile() and wholeTextFiles().
  ```python
  # Create spark session with local[5]
  rdd = spark.sparkContext.parallelize(range(0,20))
  print("From local[5] : "+str(rdd.getNumPartitions()))

  # Use parallelize with 6 partitions
  rdd1 = spark.sparkContext.parallelize(range(0,25), 6)
  print("parallelize : "+str(rdd1.getNumPartitions()))

  rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
  print("TextFile : "+str(rddFromFile.getNumPartitions()))
  ```
- Result:
  ```python
  rdd1.saveAsTextFile("/tmp/partition")

  #Writes 6 part files, one for each partition
  Partition 1 : 0 1 2
  Partition 2 : 3 4 5
  Partition 3 : 6 7 8 9
  Partition 4 : 10 11 12
  Partition 5 : 13 14 15
  Partition 6 : 16 17 18 19
  ```
  
## RDD repartition()
- repartition() method is used to increase or decrease the partitions. The below example decreases the partitions from 10 to 4 by moving data from all partitions.
  ```python
  # Using repartition
  rdd2 = rdd1.repartition(4)
  print("Repartition size : "+str(rdd2.getNumPartitions()))
  rdd2.saveAsTextFile("/tmp/re-partition")
  ```
- This yields output Repartition size : 4 and the repartition re-distributes the data(as shown below) from all partitions which is a full shuffle leading to a very expensive operation when dealing with billions and trillions of data.
  ```text
  # Output:
  Partition 1 : 1 6 10 15 19
  Partition 2 : 2 3 7 11 16
  Partition 3 : 4 8 12 13 17
  Partition 4 : 0 5 9 14 18
  ```
  
## coalesce()
- RDD coalesce() is used only to reduce the number of partitions. This is an optimized or improved version of repartition() where the movement of the data across the partitions is lower using coalesce.
  ```python
  # Using coalesce()
  rdd3 = rdd1.coalesce(4)
  print("Repartition size : "+str(rdd3.getNumPartitions()))
  rdd3.saveAsTextFile("/tmp/coalesce")
  ```
- If you compare the below output with section 1, you will notice partition 3 has been moved to 2 and Partition 6 has moved to 5, resulting in data movement from just 2 partitions.
  ```text
  Partition 1 : 0 1 2
  Partition 2 : 3 4 5 6 7 8 9
  Partition 4 : 10 11 12 
  Partition 5 : 13 14 15 16 17 18 19
  ```
  
## Complete Example of PySpark RDD repartition and coalesce
- Snippet:
  ```python
  # Complete Example
  import pyspark
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.appName('SparkByExamples.com') \
          .master("local[5]").getOrCreate()

  df = spark.range(0,20)
  print(df.rdd.getNumPartitions())

  spark.conf.set("spark.sql.shuffle.partitions", "500")

  rdd = spark.sparkContext.parallelize(range(0,20))
  print("From local[5]"+str(rdd.getNumPartitions()))

  rdd1 = spark.sparkContext.parallelize(range(0,25), 6)
  print("parallelize : "+str(rdd1.getNumPartitions()))

  """rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
  print("TextFile : "+str(rddFromFile.getNumPartitions())) """

  rdd1.saveAsTextFile("c://tmp/partition2")

  rdd2 = rdd1.repartition(4)
  print("Repartition size : "+str(rdd2.getNumPartitions()))
  rdd2.saveAsTextFile("c://tmp/re-partition2")

  rdd3 = rdd1.coalesce(4)
  print("Repartition size : "+str(rdd3.getNumPartitions()))
  rdd3.saveAsTextFile("c:/tmp/coalesce2")
  ```
  
## PySpark DataFrame repartition() vs coalesce()
- Like RDD, you can’t specify the partition/parallelism while creating DataFrame. DataFrame by default internally uses the methods specified in Section 1 to determine the default partition and splits the data for parallelism.
- The below example creates 5 partitions as specified in master("local[5]") and the data is distributed across all these 5 partitions.
  ```python
  # DataFrame example
  import pyspark
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.appName('SparkByExamples.com') \
          .master("local[5]").getOrCreate()

  df=spark.range(0,20)
  print(df.rdd.getNumPartitions())

  df.write.mode("overwrite").csv("c:/tmp/partition.csv")
  ```
  
## DataFrame repartition()
- Similar to RDD, the PySpark DataFrame repartition() method is used to increase or decrease the partitions. The below example increases the partitions from 5 to 6 by moving data from all partitions.
  ```python
  # DataFrame repartition
  df2 = df.repartition(6)
  print(df2.rdd.getNumPartitions())
  ```
- Even decreasing the partitions also results in moving data from all partitions. hence when you wanted to decrease the partition recommendation is to use coalesce()/

## DataFrame coalesce()
- Spark DataFrame coalesce() is used only to decrease the number of partitions. This is an optimized or improved version of repartition() where the movement of the data across the partitions is fewer using coalesce.
  ```python
  # DataFrame coalesce
  df3 = df.coalesce(2)
  print(df3.rdd.getNumPartitions())
  ```

## Default Shuffle Partition
- Calling groupBy(), union(), join() and similar functions on DataFrame results in shuffling data between multiple executors and even machines and finally repartitions data into 200 partitions by default. PySpark default defines shuffling partition to 200 using spark.sql.shuffle.partitions configuration.
  ```python
  # Default shuffle partition count
  df4 = df.groupBy("id").count()
  print(df4.rdd.getNumPartitions())
  ```