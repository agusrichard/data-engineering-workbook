# PySpark RDD (Resilient Distributed Dataset)

**Source: https://sparkbyexamples.com/pyspark-rdd/**

## What is RDD (Resilient Distributed Dataset)?
- RDD (Resilient Distributed Dataset) is a fundamental building block of PySpark which is fault-tolerant, immutable distributed collections of objects.
- Immutable meaning once you create an RDD you cannot change it. Each record in RDD is divided into logical partitions, which can be computed on different nodes of the cluster. 
- In other words, RDDs are a collection of objects similar to list in Python, with the difference being RDD is computed on several processes scattered across multiple physical servers also called nodes in a cluster while a Python collection lives and process in just one process.
- Additionally, RDDs provide data abstraction of partitioning and distribution of the data designed to run computations in parallel on several nodes, while doing transformations on RDD we don’t have to worry about the parallelism as PySpark by default provides.

## PySpark RDD Benefits
- In-Memory Processing
  - PySpark loads the data from disk and process in memory and keeps the data in memory, this is the main difference between PySpark and Mapreduce (I/O intensive).
- Immutability
  - PySpark RDD’s are immutable in nature meaning, once RDDs are created you cannot modify. When we apply transformations on RDD, PySpark creates a new RDD and maintains the RDD Lineage.
- Fault Tolerance
  - PySpark operates on fault-tolerant data stores on HDFS, S3 e.t.c hence any RDD operation fails, it automatically reloads the data from other partitions.
  - When PySpark applications running on a cluster, PySpark task failures are automatically recovered for a certain number of times (as per the configuration) and finish the application seamlessly.
- Lazy Evaluation
  - PySpark does not evaluate the RDD transformations as they appear/encountered by Driver instead it keeps the all transformations as it encounters(DAG) and evaluates the all transformation when it sees the first RDD action.
- Partitioning
  - When you create RDD from a data, It by default partitions the elements in a RDD. By default it partitions to the number of cores available.

## PySpark RDD Limitations
- PySpark RDDs are not much suitable for applications that make updates to the state store such as storage systems for a web application. For these applications, it is more efficient to use systems that perform traditional update logging and data checkpointing, such as databases.
- The goal of RDD is to provide an efficient programming model for batch analytics and leave these asynchronous applications.

## Creating RDD
- RDD’s are created primarily in two different ways,
  - parallelizing an existing collection and
  - referencing a dataset in an external storage system (HDFS, S3 and many more).
- In realtime application, you will pass master from spark-submit instead of hardcoding on Spark application.
- Snippet:
  ```python
  from pyspark.sql import SparkSession
  spark:SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("SparkByExamples.com")
        .getOrCreate()    
  ```

## Create RDD using sparkContext.parallelize()
- By using parallelize() function of SparkContext (sparkContext.parallelize() ) you can create an RDD.
- This function loads the existing collection from your driver program into parallelizing RDD. 
- This is a basic method to create RDD and is used when you already have data in memory that is either loaded from a file or from a database. and it required all data to be present on the driver program prior to creating RDD.
- Image RDD from list:
  ![](https://sparkbyexamples.com/ezoimgfmt/i0.wp.com/sparkbyexamples.com/wp-content/uploads/2020/08/rdd-creation-1.png?resize=1024%2C635&ssl=1&ezimgfmt=rs:800x496/rscb1/ng:webp/ngcb1)
- Snippet:
  ```python
  #Create RDD from parallelize    
  data = [1,2,3,4,5,6,7,8,9,10,11,12]
  rdd=spark.sparkContext.parallelize(data)
  ```

## Create RDD using sparkContext.textFile()
- Snippet:
  ```python
  #Create RDD from external Data source
  rdd2 = spark.sparkContext.textFile("/path/textFile.txt")
  ```

## Create RDD using sparkContext.wholeTextFiles()
- wholeTextFiles() function returns a PairRDD with the key being the file path and value being file content.
- Snippet:
  ```python
  #Reads entire file into a RDD as single record.
  rdd3 = spark.sparkContext.wholeTextFiles("/path/textFile.txt")
  ```
  
## Create empty RDD using sparkContext.emptyRDD
- Using emptyRDD() method on sparkContext we can create an RDD with no data. This method creates an empty RDD with no partition.
- Snippet:
  ```python
  # Creates empty RDD with no partition    
  rdd = spark.sparkContext.emptyRDD 
  # rddString = spark.sparkContext.emptyRDD[String]
  ```

## Creating empty RDD with partition
- Snippet:
  ```python
  #Create empty RDD with partition
  rdd2 = spark.sparkContext.parallelize([],10) #This creates 10 partitions
  ```
  
## RDD Parallelize
- When we use parallelize() or textFile() or wholeTextFiles() methods of SparkContxt to initiate RDD, it automatically splits the data into partitions based on resource availability. when you run it on a laptop it would create partitions as the same number of cores available on your system.
- getNumPartitions() – This a RDD function which returns a number of partitions our dataset split into.
  ```python
  print("initial partition count:"+str(rdd.getNumPartitions()))
  #Outputs: initial partition count:2
  ```
- Set parallelize manually using ` sparkContext.parallelize([1,2,3,4,56,7,8,9,12,3], 10).`

## Repartition and Coalesce
- Sometimes we may need to repartition the RDD, PySpark provides two ways to repartition; first using repartition() method which shuffles data from all nodes also called full shuffle and second coalesce() method which shuffle data from minimum nodes, for examples if you have data in 4 partitions and doing coalesce(2) moves data from just 2 nodes.  
- Both of the functions take the number of partitions to repartition rdd as shown below.  Note that repartition() method is a very expensive operation as it shuffles data from all nodes in a cluster. 
- repartition() or coalesce() methods also returns a new RDD.
- Snippet:
  ```python
  reparRdd = rdd.repartition(4)
  print("re-partition count:"+str(reparRdd.getNumPartitions()))
  #Outputs: "re-partition count:4
  ```

## PySpark RDD Operations
- RDD transformations – Transformations are lazy operations, instead of updating an RDD, these operations return another RDD.
- RDD actions – operations that trigger computation and return RDD values.

## RDD Transformations with example
- Transformations on PySpark RDD returns another RDD and transformations are lazy meaning they don’t execute until you call an action on RDD.
- Some transformations on RDD’s are flatMap(), map(), reduceByKey(), filter(), sortByKey() and return new RDD instead of updating the current.
- In this PySpark RDD Transformation section of the tutorial, I will explain transformations using the word count example. The below image demonstrates different RDD transformations we going to use.
- flatMap – flatMap() transformation flattens the RDD after applying the function and returns a new RDD. On the below example, first, it splits each record by space in an RDD and finally flattens it. Resulting RDD consists of a single word on each record.
  ```python
  rdd2 = rdd.flatMap(lambda x: x.split(" "))
  ```
- map – map() transformation is used the apply any complex operations like adding a column, updating a column e.t.c, the output of map transformations would always have the same number of records as input.
- In our word count example, we are adding a new column with value 1 for each word, the result of the RDD is PairRDDFunctions which contains key-value pairs, word of type String as Key and 1 of type Int as value.
  ```python
  rdd3 = rdd2.map(lambda x: (x,1))
  ```
- reduceByKey – reduceByKey() merges the values for each key with the function specified. In our example, it reduces the word string by applying the sum function on value. The result of our RDD contains unique words and their count. 
- sortByKey – sortByKey() transformation is used to sort RDD elements on key. In our example, first, we convert RDD[(String,Int]) to RDD[(Int, String]) using map transformation and apply sortByKey which ideally does sort on an integer value. And finally, foreach with println statements returns all words in RDD and their count as key-value pair
  ```python
  rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
  #Print rdd5 result to console
  print(rdd5.collect())
  ```
- filter – filter() transformation is used to filter the records in an RDD. In our example we are filtering all words starts with “a”.
  ```python
  rdd4 = rdd3.filter(lambda x : 'an' in x[1])
  print(rdd4.collect())
  ```
  
## RDD Actions with example
- RDD Action operations return the values from an RDD to a driver program. In other words, any RDD function that returns non-RDD is considered as an action. 
- count() – Returns the number of records in an RDD
  ```python
  # Action - count
  print("Count : "+str(rdd6.count()))
  ```
- first() – Returns the first record.
  ```python
  # Action - first
  firstRec = rdd6.first()
  print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])
  ```
- max() – Returns max record.
  ```python
  # Action - max
  datMax = rdd6.max()
  print("Max Record : "+str(datMax[0]) + ","+ datMax[1])
  ```
- reduce() – Reduces the records to single, we can use this to count or sum.
  ```python
  # Action - reduce
  totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
  print("dataReduce Record : "+str(totalWordCount[0]))
  ```
- take() – Returns the record specified as an argument.
  ```python
  # Action - take
  data3 = rdd6.take(3)
  for f in data3:
      print("data3 Key:"+ str(f[0]) +", Value:"+f[1])
  ```
- collect() – Returns all data from RDD as an array. Be careful when you use this action when you are working with huge RDD with millions and billions of data as you may run out of memory on the driver.
  ```python
  # Action - collect
  data = rdd6.collect()
  for f in data:
      print("Key:"+ str(f[0]) +", Value:"+f[1])
  ```
- saveAsTextFile() – Using saveAsTestFile action, we can write the RDD to a text file.
  ```python
  rdd6.saveAsTextFile("/tmp/wordCount")
  ```
  
## Types of RDD
- PairRDDFunctions or PairRDD – Pair RDD is a key-value pair This is mostly used RDD type,
- ShuffledRDD –
- DoubleRDD –
- SequenceFileRDD –
- HadoopRDD –
- ParallelCollectionRDD – 

## Shuffle Operations
- Shuffling is a mechanism PySpark uses to redistribute the data across different executors and even across machines. PySpark shuffling triggers when we perform certain transformation operations like gropByKey(), reduceByKey(), join() on RDDS
- PySpark Shuffle is an expensive operation since it involves the following
  - Disk I/O
  - Involves data serialization and deserialization
  - Network I/O
- When creating an RDD, PySpark doesn’t necessarily store the data for all keys in a partition since at the time of creation there is no way we can set the key for data set.
- Hence, when we run the reduceByKey() operation to aggregate the data on keys, PySpark does the following. needs to first run tasks to collect all the data from all partitions
- For example, when we perform reduceByKey() operation, PySpark does the following
  - PySpark first runs map tasks on all partitions which groups all values for a single key.
  - The results of the map tasks are kept in memory.
  - When results do not fit in memory, PySpark stores the data into a disk.
  - PySpark shuffles the mapped data across partitions, some times it also stores the shuffled data into a disk for reuse when it needs to recalculate.
  - Run the garbage collection
  - Finally runs reduce tasks on each partition based on key.
- PySpark RDD triggers shuffle and repartition for several operations like repartition() and coalesce(),  groupByKey(),  reduceByKey(), cogroup() and join() but not countByKey() .

## Shuffle partition size & Performance
- Based on your dataset size, a number of cores and memory PySpark shuffling can benefit or harm your jobs. When you dealing with less amount of data, you should typically reduce the shuffle partitions otherwise you will end up with many partitioned files with less number of records in each partition. which results in running many tasks with lesser data to process.
- On other hand, when you have too much of data and having less number of partitions results in fewer longer running tasks and some times you may also get out of memory error.
- Getting the right size of the shuffle partition is always tricky and takes many runs with different values to achieve the optimized number. This is one of the key properties to look for when you have performance issues on PySpark jobs.

## PySpark RDD Persistence Tutorial
- PySpark Cache and Persist are optimization techniques to improve the performance of the RDD jobs that are iterative and interactive.
- Though PySpark provides computation 100 x times faster than traditional Map Reduce jobs, If you have not designed the jobs to reuse the repeating computations you will see degrade in performance when you are dealing with billions or trillions of data. Hence, we need to look at the computations and use optimization techniques as one of the ways to improve performance.
- Using cache() and persist() methods, PySpark provides an optimization mechanism to store the intermediate computation of an RDD so they can be reused in subsequent actions.
- When you persist or cache an RDD, each worker node stores it’s partitioned data in memory or disk and reuses them in other actions on that RDD. And Spark’s persisted data on nodes are fault-tolerant meaning if any partition is lost, it will automatically be recomputed using the original transformations that created it.

## Advantages of Persisting RDD
- Cost efficient – PySpark computations are very expensive hence reusing the computations are used to save cost.
- Time efficient – Reusing the repeated computations saves lots of time.
- Execution time – Saves execution time of the job which allows us to perform more jobs on the same cluster.

## RDD Cache
- PySpark RDD cache() method by default saves RDD computation to storage level `MEMORY_ONLY` meaning it will store the data in the JVM heap as unserialized objects.
- PySpark cache() method in RDD class internally calls persist() method which in turn uses sparkSession.sharedState.cacheManager.cacheQuery to cache the result set of RDD. Let’s look at an example.
  ```python
  cachedRdd = rdd.cache()
  ```
  
## RDD Persist
- PySpark persist() method is used to store the RDD to one of the storage levels MEMORY_ONLY,MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2,MEMORY_AND_DISK_2 and more.
- PySpark persist has two signature first signature doesn’t take any argument which by default saves it to <strong>MEMORY_ONLY</strong> storage level and the second signature which takes StorageLevel as an argument to store it to different storage levels.
  ```python
  import pyspark
  dfPersist = rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)
  dfPersist.show(false)
  ```
  
## RDD Unpersist
- PySpark automatically monitors every persist() and cache() calls you make and it checks usage on each node and drops persisted data if not used or by using least-recently-used (LRU) algorithm.
- You can also manually remove using unpersist() method. unpersist() marks the RDD as non-persistent, and remove all blocks for it from memory and disk.
  ```python
  rddPersist2 = rddPersist.unpersist()
  ```
- npersist(Boolean) with boolean as argument blocks until all blocks are deleted.

## PySpark Shared Variables Tutorial
- When PySpark executes transformation using map() or reduce() operations, It executes the transformations on a remote node by using the variables that are shipped with the tasks and these variables are not sent back to PySpark Driver hence there is no capability to reuse and sharing the variables across tasks.
- PySpark shared variables solve this problem using the below two techniques. PySpark provides two types of shared variables.
  - Broadcast variables (read-only shared variable)
  - Accumulator variables (updatable shared variables)

## Broadcast read-only Variables
- Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks.
- Instead of sending this data along with every task, PySpark distributes broadcast variables to the machine using efficient broadcast algorithms to reduce communication costs.
- When you run a PySpark RDD job that has the Broadcast variables defined and used, PySpark does the following.
  - PySpark breaks the job into stages that have distributed shuffling and actions are executed with in the stage.
  - Later Stages are also broken into tasks
  - PySpark broadcasts the common data (reusable) needed by tasks within each stage.
  - The broadcasted data is cache in serialized format and deserialized before executing each task.
- The PySpark Broadcast is created using the broadcast(v) method of the SparkContext class. This method takes the argument v that you want to broadcast.
  ```python
  broadcastVar = sc.broadcast([0, 1, 2, 3])
  broadcastVar.value
  ```
- Note that broadcast variables are not sent to executors with sc.broadcast(variable) call instead, they will be sent to executors when they are first used.

## Accumulators
- PySpark Accumulators are another type shared variable that are only “added” through an associative and commutative operation and are used to perform counters (Similar to Map-reduce counters) or sum operations.
- PySpark by default supports creating an accumulator of any numeric type and provides the capability to add custom accumulator types. Programmers can create following accumulators
  - named accumulators
  - unnamed accumulators
- Example:
  ```python
  accum = sc.longAccumulator("SumAccumulator")
  sc.parallelize([1, 2, 3]).foreach(lambda x: accum.add(x))
  ```
  
## Creating RDD from DataFrame and vice-versa
- Though we have more advanced API’s over RDD, we would often need to convert DataFrame to RDD or RDD to DataFrame. Below are several examples.
- Snippet:
  ```python
  # Converts RDD to DataFrame
  dfFromRDD1 = rdd.toDF()
  # Converts RDD to DataFrame with column names
  dfFromRDD2 = rdd.toDF("col1","col2")
  # using createDataFrame() - Convert DataFrame to RDD
  df = spark.createDataFrame(rdd).toDF("col1","col2")
  # Convert DataFrame to RDD
  rdd = df.rdd
  ```