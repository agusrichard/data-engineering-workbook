# PySpark Dataframe

**Source: https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/**

## PySpark – Create an Empty DataFrame & RDD

### 1. Create Empty RDD in PySpark
- Snippet:
  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  #Creates Empty RDD
  emptyRDD = spark.sparkContext.emptyRDD()
  print(emptyRDD)

  #Diplays
  #EmptyRDD[188] at emptyRDD
  ```
- Alternatively you can also get empty RDD by using spark.sparkContext.parallelize([]).
  ```python
  #Creates Empty RDD using parallelize
  rdd2= spark.sparkContext.parallelize([])
  print(rdd2)

  #EmptyRDD[205] at emptyRDD at NativeMethodAccessorImpl.java:0
  #ParallelCollectionRDD[206] at readRDDFromFile at PythonRDD.scala:262
  ```
- If you try to perform operations on empty RDD you going to get ValueError("RDD is empty").

### 2. Create Empty DataFrame with Schema (StructType)
- In order to create an empty PySpark DataFrame manually with schema ( column names & data types) first, Create a schema using StructType and StructField .
  ```python
  #Create Schema
  from pyspark.sql.types import StructType,StructField, StringType
  schema = StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)
    ])
  
  #Create empty DataFrame from empty RDD
  df = spark.createDataFrame(emptyRDD,schema)
  df.printSchema()
  ```
  
### 3. Convert Empty RDD to DataFrame
- You can also create empty DataFrame by converting empty RDD to DataFrame using toDF().
  ```python
  #Convert empty RDD to Dataframe
  df1 = emptyRDD.toDF(schema)
  df1.printSchema()
  ```
  
### 4. Create Empty DataFrame with Schema.
- Snippet:
  ```python
  #Create empty DataFrame directly.
  df2 = spark.createDataFrame([], schema)
  df2.printSchema()
  ```
  
### 5. Create Empty DataFrame without Schema (no columns)
- Snippet:
  ```python
  #Create empty DatFrame with no schema (no columns)
  df3 = spark.createDataFrame([], StructType([]))
  df3.printSchema()

  #print below empty schema
  #root
  ```
  

## Convert PySpark RDD to DataFrame

### 1. Create PySpark RDD
- Snippet:
  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
  dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
  rdd = spark.sparkContext.parallelize(dept)
  ```

### 2. Convert PySpark RDD to DataFrame
- Snippet:
  ```python
  df = rdd.toDF()
  df.printSchema()
  df.show(truncate=False)

  deptColumns = ["dept_name","dept_id"]
  df2 = rdd.toDF(deptColumns)
  df2.printSchema()
  df2.show(truncate=False)

  deptDF = spark.createDataFrame(rdd, schema = deptColumns)
  deptDF.printSchema()
  deptDF.show(truncate=False)
  ```
- Snippet:
  ```python
  import pyspark
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
  rdd = spark.sparkContext.parallelize(dept)

  df = rdd.toDF()
  df.printSchema()
  df.show(truncate=False)

  deptColumns = ["dept_name","dept_id"]
  df2 = rdd.toDF(deptColumns)
  df2.printSchema()
  df2.show(truncate=False)

  deptDF = spark.createDataFrame(rdd, schema = deptColumns)
  deptDF.printSchema()
  deptDF.show(truncate=False)

  from pyspark.sql.types import StructType,StructField, StringType
  deptSchema = StructType([       
      StructField('dept_name', StringType(), True),
      StructField('dept_id', StringType(), True)
  ])

  deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
  deptDF1.printSchema()
  deptDF1.show(truncate=False)
  ```
  
## Convert PySpark DataFrame to Pandas

### Prepare PySpark DataFrame
- Prepare:
  ```python
  import pyspark
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  data = [("James","","Smith","36636","M",60000),
          ("Michael","Rose","","40288","M",70000),
          ("Robert","","Williams","42114","",400000),
          ("Maria","Anne","Jones","39192","F",500000),
          ("Jen","Mary","Brown","","F",0)]

  columns = ["first_name","middle_name","last_name","dob","gender","salary"]
  pysparkDF = spark.createDataFrame(data = data, schema = columns)
  pysparkDF.printSchema()
  pysparkDF.show(truncate=False)
  
  pandasDF = pysparkDF.toPandas()
  print(pandasDF)
  ```

### Convert PySpark Dataframe to Pandas DataFrame
- toPandas() results in the collection of all records in the PySpark DataFrame to the driver program and should be done only on a small subset of the data. running on larger dataset’s results in memory error and crashes the application.
  ```python
  pandasDF = pysparkDF.toPandas()
  print(pandasDF)
  ```


### Convert Spark Nested Struct DataFrame to Pandas
- Snippet:
  ```python
  # Nested structure elements
  from pyspark.sql.types import StructType, StructField, StringType,IntegerType
  dataStruct = [(("James","","Smith"),"36636","M","3000"), \
        (("Michael","Rose",""),"40288","M","4000"), \
        (("Robert","","Williams"),"42114","M","4000"), \
        (("Maria","Anne","Jones"),"39192","F","4000"), \
        (("Jen","Mary","Brown"),"","F","-1") \
  ]

  schemaStruct = StructType([
          StructField('name', StructType([
               StructField('firstname', StringType(), True),
               StructField('middlename', StringType(), True),
               StructField('lastname', StringType(), True)
               ])),
            StructField('dob', StringType(), True),
           StructField('gender', StringType(), True),
           StructField('salary', StringType(), True)
           ])
  df = spark.createDataFrame(data=dataStruct, schema = schemaStruct)
  df.printSchema()

  pandasDF2 = df.toPandas()
  print(pandasDF2)
  ```