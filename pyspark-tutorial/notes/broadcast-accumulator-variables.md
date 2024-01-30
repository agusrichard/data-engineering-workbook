# PySpark Broadcast and Accumulator Variables

**Source: https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/**

- In PySpark RDD and DataFrame, Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks.
- Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs.

## Use case
- Let me explain with an example when to use broadcast variables, assume you are getting a two-letter country state code in a file and you wanted to transform it to full state name, (for example CA to California, NY to New York e.t.c) by doing a lookup to reference mapping. In some instances, this data could be large and you may have many such lookups (like zip code e.t.c).
- Instead of distributing this information along with each task over the network (overhead and time consuming), we can use the broadcast variable to cache this lookup info on each machine and tasks use this cached info while executing the transformations.

## How does PySpark Broadcast work?
- When you run a PySpark RDD, DataFrame applications that have the Broadcast variables defined and used, PySpark does the following.
  - PySpark breaks the job into stages that have distributed shuffling and actions are executed with in the stage.
  - Later Stages are also broken into tasks
  - Spark broadcasts the common data (reusable) needed by tasks within each stage.
  - The broadcasted data is cache in serialized format and deserialized before executing each task.
- You should be creating and using broadcast variables for data that shared across multiple stages and tasks.
- Note that broadcast variables are not sent to executors with sc.broadcast(variable) call instead, they will be sent to executors when they are first used.

## How to create Broadcast variable
- Below is a very simple example of how to use broadcast variables on RDD. This example defines commonly used data (states) in a Map variable and distributes the variable using SparkContext.broadcast() and then use these variables on RDD map() transformation.
  ```python
  import pyspark
    from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  states = {"NY":"New York", "CA":"California", "FL":"Florida"}
  broadcastStates = spark.sparkContext.broadcast(states)

  data = [("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    ]

  rdd = spark.sparkContext.parallelize(data)

  def state_convert(code):
      return broadcastStates.value[code]

  result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
  print(result)
  ```
- For DataFrame:
  ```python
  import pyspark
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  states = {"NY":"New York", "CA":"California", "FL":"Florida"}
  broadcastStates = spark.sparkContext.broadcast(states)

  data = [("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    ]

  columns = ["firstname","lastname","country","state"]
  df = spark.createDataFrame(data = data, schema = columns)
  df.printSchema()
  df.show(truncate=False)

  def state_convert(code):
      return broadcastStates.value[code]

  result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
  result.show(truncate=False)
  ```
  
## PySpark Accumulator
- The PySpark Accumulator is a shared variable that is used with RDD and DataFrame to perform sum and counter operations similar to Map-reduce counters. These variables are shared by all executors to update and add information through aggregation or computative operations.
- Accumulators are write-only and initialize once variables where only tasks that are running on workers are allowed to update and updates from the workers get propagated automatically to the driver program. But, only the driver program is allowed to access the Accumulator variable using the value property.
- Using accumulator() from SparkContext class we can create an Accumulator in PySpark programming. Users can also create Accumulators for custom types using AccumulatorParam class of PySpark.
- sparkContext.accumulator() is used to define accumulator variables.
- add() function is used to add/update a value in accumulator
- value property on the accumulator variable is used to retrieve the value from the accumulator.
- We can create Accumulators in PySpark for primitive types int and float. Users can also create Accumulators for custom types using AccumulatorParam class of PySpark.

## Creating Accumulator Variable
- Below is an example of how to create an accumulator variable “accum” of type int and using it to sum all values in an RDD.
  ```python
  from pyspark.sql import SparkSession
  spark=SparkSession.builder.appName("accumulator").getOrCreate()

  accum=spark.sparkContext.accumulator(0)
  rdd=spark.sparkContext.parallelize([1,2,3,4,5])
  rdd.foreach(lambda x:accum.add(x))
  print(accum.value) #Accessed by driver
  ```
- Here, we have created an accumulator variable accum using spark.sparkContext.accumulator(0) with initial value 0. Later, we are iterating each element in an rdd using foreach() action and adding each element of rdd to accum variable. Finally, we are getting accumulator value using accum.value property.
- Example:
  ```python
  accuSum=spark.sparkContext.accumulator(0)
  def countFun(x):
      global accuSum
      accuSum+=x
  rdd.foreach(countFun)
  print(accuSum.value)

  accumCount=spark.sparkContext.accumulator(0)
  rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
  rdd2.foreach(lambda x:accumCount.add(1))
  print(accumCount.value)
  ```
- Another example:
  ```python

  import pyspark
  from pyspark.sql import SparkSession
  spark=SparkSession.builder.appName("accumulator").getOrCreate()

  accum=spark.sparkContext.accumulator(0)
  rdd=spark.sparkContext.parallelize([1,2,3,4,5])
  rdd.foreach(lambda x:accum.add(x))
  print(accum.value)

  accuSum=spark.sparkContext.accumulator(0)
  def countFun(x):
      global accuSum
      accuSum+=x
  rdd.foreach(countFun)
  print(accuSum.value)

  accumCount=spark.sparkContext.accumulator(0)
  rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
  rdd2.foreach(lambda x:accumCount.add(1))
  print(accumCount.value)
  ```