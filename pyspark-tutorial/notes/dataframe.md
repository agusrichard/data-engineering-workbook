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
  
## PySpark show() – Display DataFrame Contents in Table

### Quick Example of show()
- PySpark DataFrame show() is used to display the contents of the DataFrame in a Table Row and Column Format. By default, it shows only 20 Rows, and the column values are truncated at 20 characters.
  ```python
  # Default - displays 20 rows and 
  # 20 charactes from column value 
  df.show()

  #Display full column contents
  df.show(truncate=False)

  # Display 2 rows and full column contents
  df.show(2,truncate=False) 

  # Display 2 rows & column values 25 characters
  df.show(2,truncate=25) 

  # Display DataFrame rows & columns vertically
  df.show(n=3,truncate=25,vertical=True)
  ```
  
### PySpark show() To Display Contents
- Snippet:
  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
  columns = ["Seqno","Quote"]
  data = [("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy."),
      ("4", "Be cool.")]
  df = spark.createDataFrame(data,columns)
  df.show()

  # Output
  #+-----+--------------------+
  #|Seqno|               Quote|
  #+-----+--------------------+
  #|    1|Be the change tha...|
  #|    2|Everyone thinks o...|
  #|    3|The purpose of ou...|
  #|    4|            Be cool.|
  #+-----+--------------------+
  ```
- Snippet:
  ```python
  #Display full column contents
  df.show(truncate=False)

  #+-----+-----------------------------------------------------------------------------+
  #|Seqno|Quote                                                                        |
  #+-----+-----------------------------------------------------------------------------+
  #|1    |Be the change that you wish to see in the world                              |
  #|2    |Everyone thinks of changing the world, but no one thinks of changing himself.|
  #|3    |The purpose of our lives is to be happy.                                     |
  #|4    |Be cool.                                                                     |
  #+-----+-----------------------------------------------------------------------------+
  
  # Display 2 rows and full column contents
  df.show(2,truncate=False) 

  #+-----+-----------------------------------------------------------------------------+
  #|Seqno|Quote                                                                        |
  #+-----+-----------------------------------------------------------------------------+
  #|1    |Be the change that you wish to see in the world                              |
  #|2    |Everyone thinks of changing the world, but no one thinks of changing himself.|
  #+-----+-----------------------------------------------------------------------------+
  ```

### Display contents vertically
- Snippet:
  ```python
  # Display DataFrame rows & columns vertically
  df.show(n=3,truncate=25,vertical=True)

  #-RECORD 0--------------------------
  # Seqno | 1                         
  # Quote | Be the change that you... 
  #-RECORD 1--------------------------
  # Seqno | 2                         
  # Quote | Everyone thinks of cha... 
  #-RECORD 2--------------------------
  # Seqno | 3                         
  # Quote | The purpose of our liv... 
  ```
  
## PySpark StructType & StructField Explained with Examples

### Intro
- Key points:
  - Defining DataFrame Schemas: StructType is commonly used to define the schema when creating a DataFrame, particularly for structured data with fields of different data types.
  - Nested Structures: You can create complex schemas with nested structures by nesting StructType within other StructType objects, allowing you to represent hierarchical or multi-level data.
  - Enforcing Data Structure: When reading data from various sources, specifying a StructType as the schema ensures that the data is correctly interpreted and structured. This is important when dealing with semi-structured or schema-less data sources.

### StructType – Defines the structure of the DataFrame
- StructType is a collection or list of StructField objects.

### Using PySpark StructType & StructField with DataFrame
- Snippet:
  ```python
  # Imports
  import pyspark
  from pyspark.sql import SparkSession
  from pyspark.sql.types import StructType,StructField, StringType, IntegerType

  spark = SparkSession.builder.master("local[1]") \
                      .appName('SparkByExamples.com') \
                      .getOrCreate()

  data = [("James","","Smith","36636","M",3000),
      ("Michael","Rose","","40288","M",4000),
      ("Robert","","Williams","42114","M",4000),
      ("Maria","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","","F",-1)
    ]

  schema = StructType([ \
      StructField("firstname",StringType(),True), \
      StructField("middlename",StringType(),True), \
      StructField("lastname",StringType(),True), \
      StructField("id", StringType(), True), \
      StructField("gender", StringType(), True), \
      StructField("salary", IntegerType(), True) \
    ])
   
  df = spark.createDataFrame(data=data,schema=schema)
  df.printSchema()
  df.show(truncate=False)
  
  # Output
  root
   |-- firstname: string (nullable = true)
   |-- middlename: string (nullable = true)
   |-- lastname: string (nullable = true)
   |-- id: string (nullable = true)
   |-- gender: string (nullable = true)
   |-- salary: integer (nullable = true)

  +---------+----------+--------+-----+------+------+
  |firstname|middlename|lastname|id   |gender|salary|
  +---------+----------+--------+-----+------+------+
  |James    |          |Smith   |36636|M     |3000  |
  |Michael  |Rose      |        |40288|M     |4000  |
  |Robert   |          |Williams|42114|M     |4000  |
  |Maria    |Anne      |Jones   |39192|F     |4000  |
  |Jen      |Mary      |Brown   |     |F     |-1    |
  +---------+----------+--------+-----+------+------+
  ```
  
### Defining Nested StructType object struct
- Snippet:
  ```python
  # Defining schema using nested StructType
  structureData = [
      (("James","","Smith"),"36636","M",3100),
      (("Michael","Rose",""),"40288","M",4300),
      (("Robert","","Williams"),"42114","M",1400),
      (("Maria","Anne","Jones"),"39192","F",5500),
      (("Jen","Mary","Brown"),"","F",-1)
    ]
  structureSchema = StructType([
          StructField('name', StructType([
               StructField('firstname', StringType(), True),
               StructField('middlename', StringType(), True),
               StructField('lastname', StringType(), True)
               ])),
           StructField('id', StringType(), True),
           StructField('gender', StringType(), True),
           StructField('salary', IntegerType(), True)
           ])

  df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
  df2.printSchema()
  df2.show(truncate=False)

  # Output
  root
   |-- name: struct (nullable = true)
   |    |-- firstname: string (nullable = true)
   |    |-- middlename: string (nullable = true)
   |    |-- lastname: string (nullable = true)
   |-- id: string (nullable = true)
   |-- gender: string (nullable = true)
   |-- salary: integer (nullable = true)

  +--------------------+-----+------+------+
  |name                |id   |gender|salary|
  +--------------------+-----+------+------+
  |[James, , Smith]    |36636|M     |3100  |
  |[Michael, Rose, ]   |40288|M     |4300  |
  |[Robert, , Williams]|42114|M     |1400  |
  |[Maria, Anne, Jones]|39192|F     |5500  |
  |[Jen, Mary, Brown]  |     |F     |-1    |
  +--------------------+-----+------+------+
  ```
  
### Adding & Changing struct of the DataFrame
- Snippet:
  ```python
  # Updating existing structtype using struct
  from pyspark.sql.functions import col,struct,when
  updatedDF = df2.withColumn("OtherInfo", 
      struct(col("id").alias("identifier"),
      col("gender").alias("gender"),
      col("salary").alias("salary"),
      when(col("salary").cast(IntegerType()) < 2000,"Low")
        .when(col("salary").cast(IntegerType()) < 4000,"Medium")
        .otherwise("High").alias("Salary_Grade")
    )).drop("id","gender","salary")

  updatedDF.printSchema()
  updatedDF.show(truncate=False)

  # Output
  root
   |-- name: struct (nullable = true)
   |    |-- firstname: string (nullable = true)
   |    |-- middlename: string (nullable = true)
   |    |-- lastname: string (nullable = true)
   |-- OtherInfo: struct (nullable = false)
   |    |-- identifier: string (nullable = true)
   |    |-- gender: string (nullable = true)
   |    |-- salary: integer (nullable = true)
   |    |-- Salary_Grade: string (nullable = false)
  ```
  
### Using SQL ArrayType and MapType
- Snippet:
  ```python
  # Using SQL ArrayType and MapType
  arrayStructureSchema = StructType([
      StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
         StructField('hobbies', ArrayType(StringType()), True),
         StructField('properties', MapType(StringType(),StringType()), True)
      ])

  # Output
  root
   |-- name: struct (nullable = true)
   |    |-- firstname: string (nullable = true)
   |    |-- middlename: string (nullable = true)
   |    |-- lastname: string (nullable = true)
   |-- hobbies: array (nullable = true)
   |    |-- element: string (containsNull = true)
   |-- properties: map (nullable = true)
   |    |-- key: string
   |    |-- value: string (valueContainsNull = true)
  ```
  
### Creating StructType object struct from JSON file
- Snippet:
  ```python
  # Using json() to load StructType
  print(df2.schema.json())

  # Loading json schema to create DataFrame
  import json
  schemaFromJson = StructType.fromJson(json.loads(schema.json))
  df3 = spark.createDataFrame(
          spark.sparkContext.parallelize(structureData),schemaFromJson)
  df3.printSchema()
  ```
  
### Creating StructType object from DDL
- Snippet:
  ```python
  # Create StructType from DDL String
  ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING,
   `middle`: STRING>,`age` INT,`gender` STRING"
  ddlSchema = StructType.fromDDL(ddlSchemaStr)
  ddlSchema.printTreeString()
  ```

### Checking if a Column Exists in a DataFrame
- Snippet:
  ```python
  # Checking Column exists using contains()
  print(df.schema.fieldNames.contains("firstname"))
  print(df.schema.contains(StructField("firstname",StringType,true)))
  ```
  
### Complete Example of PySpark StructType & StructField
- Complete code:
  ```python
  import pyspark
  from pyspark.sql import SparkSession
  from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
  from pyspark.sql.functions import col,struct,when

  spark = SparkSession.builder.master("local[1]") \
                      .appName('SparkByExamples.com') \
                      .getOrCreate()

  data = [("James","","Smith","36636","M",3000),
      ("Michael","Rose","","40288","M",4000),
      ("Robert","","Williams","42114","M",4000),
      ("Maria","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","","F",-1)
    ]

  schema = StructType([ 
      StructField("firstname",StringType(),True), 
      StructField("middlename",StringType(),True), 
      StructField("lastname",StringType(),True), 
      StructField("id", StringType(), True), 
      StructField("gender", StringType(), True), 
      StructField("salary", IntegerType(), True) 
    ])
   
  df = spark.createDataFrame(data=data,schema=schema)
  df.printSchema()
  df.show(truncate=False)

  structureData = [
      (("James","","Smith"),"36636","M",3100),
      (("Michael","Rose",""),"40288","M",4300),
      (("Robert","","Williams"),"42114","M",1400),
      (("Maria","Anne","Jones"),"39192","F",5500),
      (("Jen","Mary","Brown"),"","F",-1)
    ]
  structureSchema = StructType([
          StructField('name', StructType([
               StructField('firstname', StringType(), True),
               StructField('middlename', StringType(), True),
               StructField('lastname', StringType(), True)
               ])),
           StructField('id', StringType(), True),
           StructField('gender', StringType(), True),
           StructField('salary', IntegerType(), True)
           ])

  df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
  df2.printSchema()
  df2.show(truncate=False)


  updatedDF = df2.withColumn("OtherInfo", 
      struct(col("id").alias("identifier"),
      col("gender").alias("gender"),
      col("salary").alias("salary"),
      when(col("salary").cast(IntegerType()) < 2000,"Low")
        .when(col("salary").cast(IntegerType()) < 4000,"Medium")
        .otherwise("High").alias("Salary_Grade")
    )).drop("id","gender","salary")

  updatedDF.printSchema()
  updatedDF.show(truncate=False)


  """ Array & Map"""


  arrayStructureSchema = StructType([
      StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
         StructField('hobbies', ArrayType(StringType()), True),
         StructField('properties', MapType(StringType(),StringType()), True)
      ])
  ```
  
## PySpark Column Class | Operators & Functions

### Intro
- PySpark Column class represents a single Column in a DataFrame.
- It provides functions that are most used to manipulate DataFrame Columns & Rows.
- Some of these Column functions evaluate a Boolean expression that can be used with filter() transformation to filter the DataFrame Rows.
- Provides functions to get a value from a list column by index, map value by key & index, and finally struct nested column.
- PySpark also provides additional functions pyspark.sql.functions that take Column object and return a Column type.

### Create Column Class Object
- Snippet:
  ```python
  from pyspark.sql.functions import lit
  colObj = lit("sparkbyexamples.com")

  data=[("James",23),("Ann",40)]
  df=spark.createDataFrame(data).toDF("name.fname","gender")
  df.printSchema()
  #root
  # |-- name.fname: string (nullable = true)
  # |-- gender: long (nullable = true)

  # Using DataFrame object (df)
  df.select(df.gender).show()
  df.select(df["gender"]).show()
  #Accessing column name with dot (with backticks)
  df.select(df["`name.fname`"]).show()

  #Using SQL col() function
  from pyspark.sql.functions import col
  df.select(col("gender")).show()
  #Accessing column name with dot (with backticks)
  df.select(col("`name.fname`")).show()

  #Create DataFrame with struct using Row class
  from pyspark.sql import Row
  data=[Row(name="James",prop=Row(hair="black",eye="blue")),
        Row(name="Ann",prop=Row(hair="grey",eye="black"))]
  df=spark.createDataFrame(data)
  df.printSchema()
  #root
  # |-- name: string (nullable = true)
  # |-- prop: struct (nullable = true)
  # |    |-- hair: string (nullable = true)
  # |    |-- eye: string (nullable = true)

  #Access struct column
  df.select(df.prop.hair).show()
  df.select(df["prop.hair"]).show()
  df.select(col("prop.hair")).show()

  #Access all columns from struct
  df.select(col("prop.*")).show()
  ```

### PySpark Column Operators
- Snippet:
  ```python
  data=[(100,2,1),(200,3,4),(300,4,4)]
  df=spark.createDataFrame(data).toDF("col1","col2","col3")

  #Arthmetic operations
  df.select(df.col1 + df.col2).show()
  df.select(df.col1 - df.col2).show() 
  df.select(df.col1 * df.col2).show()
  df.select(df.col1 / df.col2).show()
  df.select(df.col1 % df.col2).show()

  df.select(df.col2 > df.col3).show()
  df.select(df.col2 < df.col3).show()
  df.select(df.col2 == df.col3).show()
  ```

### PySpark Column Functions
- alias() – Set’s name to Column
  ```python
  #alias
  from pyspark.sql.functions import expr
  df.select(df.fname.alias("first_name"), \
            df.lname.alias("last_name")
     ).show()

  #Another example
  df.select(expr(" fname ||','|| lname").alias("fullName") \
     ).show()
  ```
- asc() & desc() – Sort the DataFrame columns by Ascending or Descending order.
  ```python
  #asc, desc to sort ascending and descending order repsectively.
  df.sort(df.fname.asc()).show()
  df.sort(df.fname.desc()).show()
  ```
- getField() – To get the value by key from MapType column and by stuct child name from StructType column
  ```python
  #Create DataFrame with struct, array & map
  from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType
  data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
        (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
        (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
        (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

  schema = StructType([
          StructField('name', StructType([
              StructField('fname', StringType(), True),
              StructField('lname', StringType(), True)])),
          StructField('languages', ArrayType(StringType()),True),
          StructField('properties', MapType(StringType(),StringType()),True)
       ])
  df=spark.createDataFrame(data,schema)
  df.printSchema()

  #Display's to console
  root
   |-- name: struct (nullable = true)
   |    |-- fname: string (nullable = true)
   |    |-- lname: string (nullable = true)
   |-- languages: array (nullable = true)
   |    |-- element: string (containsNull = true)
   |-- properties: map (nullable = true)
   |    |-- key: string
   |    |-- value: string (valueContainsNull = true)

  #getField from MapType
  df.select(df.properties.getField("hair")).show()

  #getField from Struct
  df.select(df.name.getField("fname")).show()
  ```
  

## PySpark Select Columns From DataFrame

### Intro
- PySpark select() is a transformation function hence it returns a new DataFrame with the selected columns.

### Select Single & Multiple Columns From PySpark
- Snippet:
  ```python
  df.select("firstname","lastname").show()
  df.select(df.firstname,df.lastname).show()
  df.select(df["firstname"],df["lastname"]).show()

  #By using col() function
  from pyspark.sql.functions import col
  df.select(col("firstname"),col("lastname")).show()

  #Select columns by regular expression
  df.select(df.colRegex("`^.*name*`")).show()
  ```

### Select All Columns From List
- Snippet:
  ```python
  # Select All columns from List
  df.select(*columns).show()

  # Select All columns
  df.select([col for col in df.columns]).show()
  df.select("*").show()
  ```
  
### Select Columns by Index
- Snippet:
  ```python
  #Selects first 3 columns and top 3 rows
  df.select(df.columns[:3]).show(3)

  #Selects columns 2 to 4  and top 3 rows
  df.select(df.columns[2:4]).show(3)
  ```
  
### Select Nested Struct Columns from PySpark
- Snippet:
  ```python
  data = [
          (("James",None,"Smith"),"OH","M"),
          (("Anna","Rose",""),"NY","F"),
          (("Julia","","Williams"),"OH","F"),
          (("Maria","Anne","Jones"),"NY","M"),
          (("Jen","Mary","Brown"),"NY","M"),
          (("Mike","Mary","Williams"),"OH","M")
          ]

  from pyspark.sql.types import StructType,StructField, StringType        
  schema = StructType([
      StructField('name', StructType([
           StructField('firstname', StringType(), True),
           StructField('middlename', StringType(), True),
           StructField('lastname', StringType(), True)
           ])),
       StructField('state', StringType(), True),
       StructField('gender', StringType(), True)
       ])
  df2 = spark.createDataFrame(data = data, schema = schema)
  df2.printSchema()
  df2.show(truncate=False) # shows all columns

  df2.select("name.firstname","name.lastname").show(truncate=False)
  +---------+--------+
  |firstname|lastname|
  +---------+--------+
  |James    |Smith   |
  |Anna     |        |
  |Julia    |Williams|
  |Maria    |Jones   |
  |Jen      |Brown   |
  |Mike     |Williams|
  +---------+--------+

  df2.select("name.*").show(truncate=False)
  +---------+----------+--------+
  |firstname|middlename|lastname|
  +---------+----------+--------+
  |James    |null      |Smith   |
  |Anna     |Rose      |        |
  |Julia    |          |Williams|
  |Maria    |Anne      |Jones   |
  |Jen      |Mary      |Brown   |
  |Mike     |Mary      |Williams|
  +---------+----------+--------+
  ```

### Complete example:
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)

df.select("firstname").show()

df.select("firstname","lastname").show()

#Using Dataframe object name
df.select(df.firstname,df.lastname).show()

# Using col function
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()

data = [(("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])

df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns

df2.select("name").show(truncate=False)
df2.select("name.firstname","name.lastname").show(truncate=False)
df2.select("name.*").show(truncate=False)
```

## PySpark Collect() – Retrieve data from DataFrame
- PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset (from all nodes) to the driver node. We should use the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets results in OutOfMemory error.
- Snippet:
  ```python
  dataCollect = deptDF.collect()
  print(dataCollect)

  [Row(dept_name='Finance', dept_id=10), 
  Row(dept_name='Marketing', dept_id=20), 
  Row(dept_name='Sales', dept_id=30), 
  Row(dept_name='IT', dept_id=40)]
  ```
- Usually, collect() is used to retrieve the action output when you have very small result set and calling collect() on an RDD/DataFrame with a bigger result set causes out of memory as it returns the entire dataset (from all workers) to the driver hence we should avoid calling collect() on a larger dataset.
- select() is a transformation that returns a new DataFrame and holds the columns that are selected whereas collect() is an action that returns the entire data set in an Array to the driver.
- Complete example:
  ```python
  import pyspark
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  dept = [("Finance",10), \
      ("Marketing",20), \
      ("Sales",30), \
      ("IT",40) \
    ]
  deptColumns = ["dept_name","dept_id"]
  deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
  deptDF.printSchema()
  deptDF.show(truncate=False)

  dataCollect = deptDF.collect()

  print(dataCollect)

  dataCollect2 = deptDF.select("dept_name").collect()
  print(dataCollect2)

  for row in dataCollect:
      print(row['dept_name'] + "," +str(row['dept_id']))
  ```
  
## PySpark withColumn() Usage with Examples
- PySpark withColumn() is a transformation function of DataFrame which is used to change the value, convert the datatype of an existing column, create a new column, and many more.
- In order to create a new column, pass the column name you wanted to the first argument of withColumn() transformation function. Make sure this new column not already present on DataFrame, if it presents it updates the value of that column.
- Complete example:
  ```python
  import pyspark
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import col, lit
  from pyspark.sql.types import StructType, StructField, StringType,IntegerType

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  data = [('James','','Smith','1991-04-01','M',3000),
    ('Michael','Rose','','2000-05-19','M',4000),
    ('Robert','','Williams','1978-09-05','M',4000),
    ('Maria','Anne','Jones','1967-12-01','F',4000),
    ('Jen','Mary','Brown','1980-02-17','F',-1)
  ]

  columns = ["firstname","middlename","lastname","dob","gender","salary"]
  df = spark.createDataFrame(data=data, schema = columns)
  df.printSchema()
  df.show(truncate=False)

  df2 = df.withColumn("salary",col("salary").cast("Integer"))
  df2.printSchema()
  df2.show(truncate=False)

  df3 = df.withColumn("salary",col("salary")*100)
  df3.printSchema()
  df3.show(truncate=False) 

  df4 = df.withColumn("CopiedColumn",col("salary")* -1)
  df4.printSchema()

  df5 = df.withColumn("Country", lit("USA"))
  df5.printSchema()

  df6 = df.withColumn("Country", lit("USA")) \
     .withColumn("anotherColumn",lit("anotherValue"))
  df6.printSchema()

  df.withColumnRenamed("gender","sex") \
    .show(truncate=False) 
    
  df4.drop("CopiedColumn") \
  .show(truncate=False)
  ```

## PySpark withColumnRenamed to Rename Column on DataFrame
- PySpark has a withColumnRenamed() function on DataFrame to change a column name. This is the most straight forward approach; this function takes two parameters; the first is your existing column name and the second is the new column name you wish for.
- Note that withColumnRenamed function returns a new DataFrame and doesn’t modify the current DataFrame.
- Complete code:
  ```python
  import pyspark
  from pyspark.sql import SparkSession
  from pyspark.sql.types import StructType,StructField, StringType, IntegerType
  from pyspark.sql.functions import *

  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
    (('Michael','Rose',''),'2000-05-19','M',4000),
    (('Robert','','Williams'),'1978-09-05','M',4000),
    (('Maria','Anne','Jones'),'1967-12-01','F',4000),
    (('Jen','Mary','Brown'),'1980-02-17','F',-1)
  ]

  schema = StructType([
          StructField('name', StructType([
               StructField('firstname', StringType(), True),
               StructField('middlename', StringType(), True),
               StructField('lastname', StringType(), True)
               ])),
           StructField('dob', StringType(), True),
           StructField('gender', StringType(), True),
           StructField('salary', IntegerType(), True)
           ])

  df = spark.createDataFrame(data = dataDF, schema = schema)
  df.printSchema()

  # Example 1
  df.withColumnRenamed("dob","DateOfBirth").printSchema()
  # Example 2   
  df2 = df.withColumnRenamed("dob","DateOfBirth") \
      .withColumnRenamed("salary","salary_amount")
  df2.printSchema()

  # Example 3 
  schema2 = StructType([
      StructField("fname",StringType()),
      StructField("middlename",StringType()),
      StructField("lname",StringType())])
      
  df.select(col("name").cast(schema2),
    col("dob"),
    col("gender"),
    col("salary")) \
      .printSchema()    

  # Example 4 
  df.select(col("name.firstname").alias("fname"),
    col("name.middlename").alias("mname"),
    col("name.lastname").alias("lname"),
    col("dob"),col("gender"),col("salary")) \
    .printSchema()
    
  # Example 5
  df4 = df.withColumn("fname",col("name.firstname")) \
        .withColumn("mname",col("name.middlename")) \
        .withColumn("lname",col("name.lastname")) \
        .drop("name")
  df4.printSchema()

  #Example 7
  newColumns = ["newCol1","newCol2","newCol3","newCol4"]
  df.toDF(*newColumns).printSchema()

  # Example 6
  '''
  not working
  old_columns = Seq("dob","gender","salary","fname","mname","lname")
  new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
  columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
  df5 = df4.select(columnsList:_*)
  df5.printSchema()
  '''
  ```
  
## PySpark Where Filter Function | Multiple Conditions
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

arrayStructureData = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
        ]
        
arrayStructureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('languages', ArrayType(StringType()), True),
         StructField('state', StringType(), True),
         StructField('gender', StringType(), True)
         ])


df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)
df.printSchema()
df.show(truncate=False)

df.filter(df.state == "OH") \
    .show(truncate=False)

df.filter(col("state") == "OH") \
    .show(truncate=False)    
    
df.filter("gender  == 'M'") \
    .show(truncate=False)    

df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)        

df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)        

df.filter(df.name.lastname == "Williams") \
    .show(truncate=False) 
```

### PySpark Distinct to Drop Duplicate Rows
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

#Distinct
distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

#Drop duplicates
df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)

#Drop duplicates on selected columns
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)
}
```

## PySpark orderBy() and sort() explained
```python
# Imports

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc,desc

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data = simpleData, schema = columns)

df.printSchema()
df.show(truncate=False)

df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)

df.orderBy("department","state").show(truncate=False)
df.orderBy(col("department"),col("state")).show(truncate=False)

df.sort(df.department.asc(),df.state.asc()).show(truncate=False)
df.sort(col("department").asc(),col("state").asc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

df.createOrReplaceTempView("EMP")
spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)
```

## PySpark Groupby Explained with Example
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count().show(truncate=False)


df.groupBy("department","state") \
    .sum("salary","bonus") \
   .show(truncate=False)

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus") \
     ) \
    .show(truncate=False)
    
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)
```

## PySpark Join Types | Join Two DataFrames
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
  
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)
    
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)
   
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)
   
empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)

empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")
   
joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)
```

## PySpark Union and UnionAll Explained
```python
# Imports
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

df2.printSchema()
df2.show(truncate=False)

unionDF = df.union(df2)
unionDF.show(truncate=False)
disDF = df.union(df2).distinct()
disDF.show(truncate=False)

unionAllDF = df.unionAll(df2)
unionAllDF.show(truncate=False)
```

## PySpark unionByName()
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create DataFrame df1 with columns name, and id
data = [("James",34), ("Michael",56), \
        ("Robert",30), ("Maria",24) ]

df1 = spark.createDataFrame(data = data, schema=["name","id"])
df1.printSchema()

# Create DataFrame df2 with columns name and id
data2=[(34,"James"),(45,"Maria"), \
       (45,"Jen"),(34,"Jeff")]

df2 = spark.createDataFrame(data = data2, schema = ["id","name"])
df2.printSchema()

# Using unionByName()
df3 = df1.unionByName(df2)
df3.printSchema()
df3.show()

# Using allowMissingColumns
df1 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])
df3 = df1.unionByName(df2, allowMissingColumns=True)
df3.printSchema()
df3.show()
```

## PySpark UDF (User Defined Function)
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

""" Converting function to UDF """
convertUDF = udf(lambda z: convertCase(z))

df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
.show(truncate=False)

def upperCase(str):
    return str.upper()

upperCaseUDF = udf(lambda z:upperCase(z),StringType())    

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
.show(truncate=False)

""" Using UDF on SQL """
spark.udf.register("convertUDF", convertCase,StringType())
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE") \
     .show(truncate=False)
     
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE " + \
          "where Name is not null and convertUDF(Name) like '%John%'") \
     .show(truncate=False)  
     
""" null check """

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders"),
    ('4',None)]

df2 = spark.createDataFrame(data=data,schema=columns)
df2.show(truncate=False)
df2.createOrReplaceTempView("NAME_TABLE2")
    
spark.udf.register("_nullsafeUDF", lambda str: convertCase(str) if not str is None else "" , StringType())

spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2") \
     .show(truncate=False)

spark.sql("select Seqno, _nullsafeUDF(Name) as Name from NAME_TABLE2 " + \
          " where Name is not null and _nullsafeUDF(Name) like '%John%'") \
     .show(truncate=False)  
```

## PySpark transform() Function with Example
```python
# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()

# Prepare Data
simpleData = (("Java",4000,5), \
    ("Python", 4600,10),  \
    ("Scala", 4100,15),   \
    ("Scala", 4500,15),   \
    ("PHP", 3000,20),  \
  )
columns= ["CourseName", "fee", "discount"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# Custom transformation 1
from pyspark.sql.functions import upper
def to_upper_str_columns(df):
    return df.withColumn("CourseName",upper(df.CourseName))

# Custom transformation 2
def reduce_price(df,reduceBy):
    return df.withColumn("new_fee",df.fee - reduceBy)

# Custom transformation 3
def apply_discount(df):
    return df.withColumn("discounted_fee",  \
             df.new_fee - (df.new_fee * df.discount) / 100)

# transform() usage
df2 = df.transform(to_upper_str_columns) \
        .transform(reduce_price,1000) \
        .transform(apply_discount) 
                
df2.show()

# Create DataFrame with Array
data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"]),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"]),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"])
]
df = spark.createDataFrame(data=data,schema=["Name","Languages1","Languages2"])
df.printSchema()
df.show()

# using transform() SQL function
from pyspark.sql.functions import upper
from pyspark.sql.functions import transform
df.select(transform("Languages1", lambda x: upper(x)).alias("languages1")) \
  .show()
```

## PySpark apply Function to Column
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

# Apply function using withColumn
from pyspark.sql.functions import upper
df.withColumn("Upper_Name", upper(df.Name)) \
  .show()

# Apply function using select  
df.select("Seqno","Name", upper(df.Name)) \
  .show()

# Apply function using sql()
df.createOrReplaceTempView("TAB")
spark.sql("select Seqno, Name, UPPER(Name) from TAB") \
     .show()  

# Create custom function
def upperCase(str):
    return str.upper()

# Convert function to udf
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
upperCaseUDF = udf(lambda x:upperCase(x),StringType())   

# Custom UDF with withColumn()
df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)

# Custom UDF with select()  
df.select(col("Seqno"), \
    upperCaseUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)

# Custom UDF with sql()
spark.udf.register("upperCaseUDF", upperCaseUDF)
df.createOrReplaceTempView("TAB")
spark.sql("select Seqno, Name, upperCaseUDF(Name) from TAB") \
     .show() 
```

## PySpark map() Transformation
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project",
"Gutenberg’s",
"Alice’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)
    
data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

rdd2=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2],x[3]*2)
    )  
df2=rdd2.toDF(["name","gender","new_salary"]   )
df2.show()

#Referring Column Names
rdd2=df.rdd.map(lambda x: 
    (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2)
    ) 

#Referring Column Names
rdd2=df.rdd.map(lambda x: 
    (x.firstname+","+x.lastname,x.gender,x.salary*2)
    ) 

def func1(x):
    firstName=x.firstname
    lastName=x.lastname
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd2=df.rdd.map(lambda x: func1(x))
```

## PySpark flatMap() Transformation
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)

#Flatmap    
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)
```
```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()
```

## PySpark foreach() Usage with Examples
```python
# Import
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
                    .getOrCreate()

# Prepare Data
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

# Create DataFrame
df = spark.createDataFrame(data=data,schema=columns)
df.show()

# foreach() Example
def f(df):
    print(df.Seqno)
df.foreach(f)

# foreach() with RDD example
accum=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value) #Accessed by driver
```

## PySpark Random Sample with Example
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

df=spark.range(100)
print(df.sample(0.06).collect())

print(df.sample(0.1,123).collect())
# Output: 36,37,41,43,56,66,69,75,83

print(df.sample(0.1,123).collect())
# Output: 36,37,41,43,56,66,69,75,83

print(df.sample(0.1,456).collect())
# Output: 19,21,42,48,49,50,75,80

print(df.sample(True,0.3,123).collect()) # with Duplicates
# Output: 0,5,9,11,14,14,16,17,21,29,33,41,42,52,52,54,58,65,65,71,76,79,85,96
print(df.sample(0.3,123).collect()) #  No duplicates
# Output: 0,4,17,19,24,25,26,36,37,41,43,44,53,56,66,68,69,70,71,75,76,78,83,84,88,94,96,97,98
```

## PySpark fillna() & fill() – Replace NULL/None Values
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

filePath="resources/small_zipcode.csv"
df = spark.read.options(header='true', inferSchema='true') \
          .csv(filePath)

df.printSchema()
df.show(truncate=False)


df.fillna(value=0).show()
df.fillna(value=0,subset=["population"]).show()
df.na.fill(value=0).show()
df.na.fill(value=0,subset=["population"]).show()


df.fillna(value="").show()
df.na.fill(value="").show()

df.fillna("unknown",["city"]) \
    .fillna("",["type"]).show()

df.fillna({"city": "unknown", "type": ""}) \
    .show()

df.na.fill("unknown",["city"]) \
    .na.fill("",["type"]).show()

df.na.fill({"city": "unknown", "type": ""}) \
    .show()
```

## PySpark Pivot and Unpivot DataFrame
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)

pivotDF = df.groupBy("Product","Country") \
      .sum("Amount") \
      .groupBy("Product") \
      .pivot("Country") \
      .sum("sum(Amount)")
pivotDF.printSchema()
pivotDF.show(truncate=False)

""" unpivot """
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)) \
    .where("Total is not null")
unPivotDF.show(truncate=False)
```