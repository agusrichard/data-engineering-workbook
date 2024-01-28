# PySpark Session

**Source: https://sparkbyexamples.com/pyspark/pyspark-what-is-sparksession/**

## Introduction
- Since Spark 2.0 SparkSession has become an entry point to PySpark to work with RDD, and DataFrame.
- Prior to 2.0, SparkContext used to be an entry point.
- SparkSession was introduced in version 2.0, It is an entry point to underlying PySpark functionality in order to programmatically create PySpark RDD, DataFrame. It’s object spark is default available in pyspark-shell and it can be created programmatically using SparkSession.

## SparkSession
- With Spark 2.0 a new class SparkSession (pyspark.sql import SparkSession) has been introduced.
- Since 2.0 SparkSession can be used in replace with SQLContext, HiveContext, and other contexts defined prior to 2.0.
- As mentioned in the beginning SparkSession is an entry point to PySpark and creating a SparkSession instance would be the first statement you would write to program with RDD, DataFrame, and Dataset.
- SparkSession will be created using SparkSession.builder builder patterns.
- You should also know that SparkSession internally creates SparkConfig and SparkContext with the configuration provided with SparkSession.
- SparkSession also includes all the APIs available in different contexts –
  - SparkContext,
  - SQLContext,
  - StreamingContext,
  - HiveContext.
- You can create as many SparkSession as you want in a PySpark application using either SparkSession.builder() or SparkSession.newSession(). Many Spark session objects are required when you wanted to keep PySpark tables (relational entities) logically separated.

## Create SparkSession
- In order to create SparkSession programmatically (in .py file) in PySpark, you need to use the builder pattern method builder() as explained below. getOrCreate() method returns an already existing SparkSession; if not exists, it creates a new SparkSession.
  ```python
  # Create SparkSession from builder
  import pyspark
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.master("local[1]") \
                      .appName('SparkByExamples.com') \
                      .getOrCreate()
  ```
- master() – If you are running it on the cluster you need to use your master name as an argument to master(). usually, it would be either yarn or mesos depends on your cluster setup.
- Use local[x] when running in Standalone mode. x should be an integer value and should be greater than 0; this represents how many partitions it should create when using RDD, DataFrame, and Dataset. Ideally, x value should be the number of CPU cores you have.
- appName() – Used to set your application name.
- getOrCreate() – This returns a SparkSession object if already exists, and creates a new one if not exist.

## Create Another SparkSession
- You can also create a new SparkSession using newSession() method.
- This uses the same app name, master as the existing session. Underlying SparkContext will be the same for both sessions as you can have only one context per PySpark application.
- This always creates a new SparkSession object:
  ```python
  # Create new SparkSession
  spark2 = SparkSession.newSession
  print(spark2)
  ```

## Get Existing SparkSession
- Snippet:
  ```python
  # Get Existing SparkSession
  spark3 = SparkSession.builder.getOrCreate
  print(spark3)
  ```

## Using Spark Config
- Snippet:
  ```python
  # Usage of config()
  spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .config("spark.some.config.option", "config-value") \
        .getOrCreate()
  ```
  
## Create SparkSession with Hive Enable
- Snippet:
  ```python
  # Enabling Hive to use in Spark
  spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .config("spark.sql.warehouse.dir", "<path>/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
  ```
  
## Using PySpark Configs
- Once the SparkSession is created, you can add the spark configs during runtime or get all configs.
  ```python
  # Set Config
  spark.conf.set("spark.executor.memory", "5g")

  # Get a Spark Config
  partitions = spark.conf.get("spark.sql.shuffle.partitions")
  print(partitions)
  ```
  
## Create PySpark DataFrame
- SparkSession also provides several methods to create a Spark DataFrame and DataSet. The below example uses the createDataFrame() method which takes a list of data.
  ```python
  # Create DataFrame
  df = spark.createDataFrame(
      [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
  df.show()

  # Output
  #+-----+-----+
  #|   _1|   _2|
  #+-----+-----+
  #|Scala|25000|
  #|Spark|35000|
  #|  PHP|21000|
  #+-----+-----+
  ```
  
## Working with Spark SQL
- Using SparkSession you can access PySpark/Spark SQL capabilities in PySpark. In order to use SQL features first, you need to create a temporary view in PySpark. Once you have a temporary view you can run any ANSI SQL queries using spark.sql() method.
  ```python
  # Spark SQL
  df.createOrReplaceTempView("sample_table")
  df2 = spark.sql("SELECT _1,_2 FROM sample_table")
  df2.show()
  ```
- PySpark SQL temporary views are session-scoped and will not be available if the session that creates it terminates. If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view using createGlobalTempView()

## Create Hive Table
- saveAsTable() creates Hive managed table. Query the table using spark.sql().
  ```python
  # Create Hive table & query it.  
  spark.table("sample_table").write.saveAsTable("sample_hive_table")
  df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
  df3.show()
  ```
  
## Working with Catalogs
- To get the catalog metadata, PySpark Session exposes catalog variable. Note that these methods spark.catalog.listDatabases and spark.catalog.listTables and returns the DataSet.
```python
# Get metadata from the Catalog
# List databases
dbs = spark.catalog.listDatabases()
print(dbs)

# Output
#[Database(name='default', description='default database', 
#locationUri='file:/Users/admin/.spyder-py3/spark-warehouse')]

# List Tables
tbls = spark.catalog.listTables()
print(tbls)

#Output
#[Table(name='sample_hive_table', database='default', description=None, tableType='MANAGED', #isTemporary=False), Table(name='sample_hive_table1', database='default', description=None, #tableType='MANAGED', isTemporary=False), Table(name='sample_hive_table121', database='default', #description=None, tableType='MANAGED', isTemporary=False), Table(name='sample_table', database=None, #description=None, tableType='TEMPORARY', isTemporary=True)]
```

## FAQs
- It is recommended to end the Spark session after finishing the Spark job in order for the JVMs to close and free the resources.
- If you have spark as a SparkSession object then call spark.sparkContext.isActive. This returns true if it is active otherwise false.


