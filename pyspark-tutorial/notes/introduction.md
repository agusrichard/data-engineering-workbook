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

