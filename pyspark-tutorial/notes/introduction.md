# PySpark Tutorial For Beginners (Spark 3.5 with Python)

**Source: https://sparkbyexamples.com/pyspark-tutorial/**

## What is Apache Spark?
- Apache Spark is an open-source unified analytics engine used for large-scale data processing, hereafter referred it as Spark.
- Spark can run on single-node machines or multi-node machines(Cluster).
- It was created to address the limitations of MapReduce, by doing in-memory processing.
- Spark reuses data by using an in-memory cache to speed up machine learning algorithms that repeatedly call a function on the same dataset. 
- This lowers the latency making Spark multiple times faster than MapReduce, especially when doing machine learning, and interactive analytics.  Apache Spark can also process real-time streaming. 

## What are the Features of PySpark?
- The following are the main features of PySpark.
  ![PySpark Features](https://sparkbyexamples.com/ezoimgfmt/i0.wp.com/sparkbyexamples.com/wp-content/uploads/2020/08/pyspark-features-1.png?w=1200&ssl=1&ezimgfmt=rs:574x431/rscb1/ng:webp/ngcb1)
  - In-memory computation
  - Distributed processing using parallelize
  - Can be used with many cluster managers (Spark, Yarn, Mesos e.t.c)
  - Fault-tolerant
  - Immutable
  - Lazy evaluation
  - Cache & persistence
  - Inbuild-optimization when using DataFrames
  - Supports ANSI SQL

## Advantages of PySpark
- PySpark is a general-purpose, in-memory, distributed processing engine that allows you to process data efficiently in a distributed fashion.
- Applications running on PySpark are 100x faster than traditional systems.
- You will get great benefits from using PySpark for data ingestion pipelines.
- Using PySpark we can process data from Hadoop HDFS, AWS S3, and many file systems.
- PySpark also is used to process real-time data using Streaming and Kafka.
- Using PySpark streaming you can also stream files from the file system and also stream from the socket.
- PySpark natively has machine learning and graph libraries.

## PySpark Architecture
- Apache Spark works in a master-slave architecture where the master is called the “Driver” and slaves are called “Workers”.
- When you run a Spark application, Spark Driver creates a context that is an entry point to your application, and all operations (transformations and actions) are executed on worker nodes, and the resources are managed by Cluster Manager.
  ![](https://sparkbyexamples.com/ezoimgfmt/i0.wp.com/sparkbyexamples.com/wp-content/uploads/2020/02/spark-cluster-overview.png?w=596&ssl=1&ezimgfmt=ng:webp/ngcb1)

## Cluster Manager Types
- Standalone – a simple cluster manager included with Spark that makes it easy to set up a cluster.
- Apache Mesos – Mesons is a Cluster manager that can also run Hadoop MapReduce and PySpark applications.
- Hadoop YARN – the resource manager in Hadoop 2. This is mostly used as a cluster manager.
- Kubernetes – an open-source system for automating deployment, scaling, and management of containerized applications.
- local – which is not really a cluster manager but still I wanted to mention that we use “local” for master() in order to run Spark on your laptop/computer.

## PySpark Modules & Packages
- PySpark RDD (pyspark.RDD)
- PySpark DataFrame and SQL (pyspark.sql)
- PySpark Streaming (pyspark.streaming)
- PySpark MLib (pyspark.ml, pyspark.mllib)
- PySpark GraphFrames (GraphFrames)
- PySpark Resource (pyspark.resource) It’s new in PySpark 3.0
- PySpark ecosystem:
  ![Spark Ecosystem](https://sparkbyexamples.com/ezoimgfmt/i0.wp.com/sparkbyexamples.com/wp-content/uploads/2020/02/spark-components-1.jpg?w=1018&ssl=1&ezimgfmt=ng:webp/ngcb1)

## PySpark RDD – Resilient Distributed Dataset
- PySpark RDD (Resilient Distributed Dataset) is a fundamental data structure of PySpark that is fault-tolerant, immutable distributed collections of objects, which means once you create an RDD you cannot change it.
- Each dataset in RDD is divided into logical partitions, which can be computed on different nodes of the cluster.

### RDD Creation 
- In order to create an RDD, first, you need to create a SparkSession which is an entry point to the PySpark application
- parkSession can be created using a builder() or newSession() methods of the SparkSession.
- Spark session internally creates a sparkContext variable of SparkContext. You can create multiple SparkSession objects but only one SparkContext per JVM.
- In case you want to create another new SparkContext you should stop the existing Sparkcontext (using stop()) before creating a new one.
- Snippet:
  ```python
  # Import SparkSession
  from pyspark.sql import SparkSession

  # Create SparkSession 
  spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate() 
  ```
  
### using parallelize()
- SparkContext has several functions to use with RDDs. For example, it’s parallelize() method is used to create an RDD from a list.
  ```python
  # Create RDD from parallelize    
  dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
  rdd=spark.sparkContext.parallelize(dataList)
  ```
  
### using textFile()
- RDD can also be created from a text file using textFile() function of the SparkContext.
  ```python
  # Create RDD from external Data source
  rdd2 = spark.sparkContext.textFile("/path/test.txt")
  ```
- Once you have an RDD, you can perform transformation and action operations. Any operation you perform on RDD runs in parallel.  
  
### RDD Operations
- RDD transformations – Transformations are lazy operations. When you run a transformation(for example update), instead of updating a current RDD, these operations return another RDD.
- RDD actions – operations that trigger computation and return RDD values to the driver.

### RDD Transformations
- Transformations on Spark RDD return another RDD and transformations are lazy meaning they don’t execute until you call an action on RDD.
- Some transformations on RDDs are flatMap(), map(), reduceByKey(), filter(), sortByKey() and return a new RDD instead of updating the current.

### RDD Actions
- RDD Action operation returns the values from an RDD to a driver node. In other words, any RDD function that returns non RDD[T] is considered as an action. 
- Some actions on RDDs are count(), collect(), first(), max(), reduce() and more.

## PySpark DataFrame Tutorial for Beginners
- DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as structured data files, tables in Hive, external databases, or existing RDDs. – Databricks
- PySpark DataFrame is mostly similar to Pandas DataFrame with the exception that PySpark DataFrames are distributed in the cluster (meaning the data in data frames are stored in different machines in a cluster) and any operations in PySpark executes in parallel on all machines whereas Panda Dataframe stores and operates on a single machine.
- Just know that data in PySpark DataFrames are stored in different machines in a cluster.

### Is PySpark faster than pandas?
- Due to parallel execution on all cores on multiple machines, PySpark runs operations faster than Pandas
- In other words, Pandas DataFrames run operations on a single node whereas PySpark runs on multiple machines.

### DataFrame creation
- using createDataFrame()
  ```python
  data = [('James','','Smith','1991-04-01','M',3000),
    ('Michael','Rose','','2000-05-19','M',4000),
    ('Robert','','Williams','1978-09-05','M',4000),
    ('Maria','Anne','Jones','1967-12-01','F',4000),
    ('Jen','Mary','Brown','1980-02-17','F',-1)
  ]

  columns = ["firstname","middlename","lastname","dob","gender","salary"]
  df = spark.createDataFrame(data=data, schema = columns)
  ```
- Since DataFrame’s are structure format that contains names and columns, we can get the schema of the DataFrame using df.printSchema()

### DataFrame from external data sources
- Below is an example of how to read a CSV file from a local system.
  ```python
  df = spark.read.csv("/tmp/resources/zipcodes.csv")
  df.printSchema()
  ```
  
## PySpark SQL Tutorial
- PySpark SQL is one of the most used PySpark modules which is used for processing structured columnar data format. Once you have a DataFrame created, you can interact with the data by using SQL syntax.
- In other words, Spark SQL brings native RAW SQL queries on Spark meaning you can run traditional ANSI SQL on Spark Dataframe, in the later section of this PySpark SQL tutorial, you will learn in detail how to use SQL select, where, group by, join, union e.t.c
- In order to use SQL, first, create a temporary table on DataFrame using createOrReplaceTempView() function. Once created, this table can be accessed throughout the SparkSession using sql() and it will be dropped along with your SparkContext termination.
- Use sql() method of the SparkSession object to run the query and this method returns a new DataFrame.
  ```python
  df.createOrReplaceTempView("PERSON_DATA")
  df2 = spark.sql("SELECT * from PERSON_DATA")
  df2.printSchema()
  df2.show()
  
  groupDF = spark.sql("SELECT gender, count(*) from PERSON_DATA group by gender")
  groupDF.show()
  ```
- PySpark Streaming Tutorial for Beginners – Streaming is a scalable, high-throughput, fault-tolerant streaming processing system that supports both batch and streaming workloads. It is used to process real-time data from sources like file system folders, TCP sockets, S3, Kafka, Flume, Twitter, and Amazon Kinesis to name a few. The processed data can be pushed to databases, Kafka, live dashboards e.t.c

### Streaming from TCP Socket
- Use readStream.format("socket") from Spark session object to read data from the socket and provide options host and port where you want to stream data from.
  ```python
  df = spark.readStream
        .format("socket")
        .option("host","localhost")
        .option("port","9090")
        .load()
  ```
- After processing, you can stream the data frame to the console. In real-time, we ideally stream it to either Kafka, database e.t.c
  ```python
  query = count.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
  ```
  
### Streaming from Kafka
- Using Spark Streaming we can read from Kafka topic and write to Kafka topic in TEXT, CSV, AVRO and JSON formats
  ![](https://sparkbyexamples.com/ezoimgfmt/i0.wp.com/sparkbyexamples.com/wp-content/uploads/2019/03/spark-structured-streaming-kafka.png?w=1404&ssl=1&ezimgfmt=ng:webp/ngcb1)
- Snippet:
  ```python
  df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "192.168.1.100:9092")
          .option("subscribe", "json_topic")
          .option("startingOffsets", "earliest") // From starting
          .load()
  ```
- Below Pyspark example, Write a message to another topic in Kafka using writeStream()
  ```python
  df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
     .writeStream
     .format("kafka")
     .outputMode("append")
     .option("kafka.bootstrap.servers", "192.168.1.100:9092")
     .option("topic", "josn_data_topic")
     .start()
     .awaitTermination()
  ```