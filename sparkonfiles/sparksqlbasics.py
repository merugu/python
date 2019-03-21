"""
A simple example demonstrating basic Spark SQL features.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/basic.py
"""
from __future__ import print_function

# $example on:init_session$
from pyspark.sql import SparkSession
# $example off:init_session$

# $example on:schema_inferring$
from pyspark.sql import Row
# $example off:schema_inferring$

# $example on:programmatic_schema$
# Import data types
from pyspark.sql.types import *
# $example off:programmatic_schema$


def basic_df_example(spark):
    # $example on:create_df$
    # spark is an existing SparkSession
    df = spark.read.json("/Users/amitheshmerugu/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.json")
    # Displays the content of the DataFrame to stdout
    #df.show()
    # $example on:untyped_ops$
    # spark, df are from the previous example
    # Print the schema in a tree format
    #df.printSchema()
    #df.select(df['age']+1, df['name']).show()
    #df.filter(df['age'] < 30).show()
    #df.groupBy('age').count().show()

    df.createOrReplaceTempView("people")
    sqlDF = spark.sql('select * from people')
    sqlDF.show()

    # Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()

    # Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

def schema_inference_example(spark):
    sc = spark.sparkContext
    # Load a text file and convert each line to a Row.
    lines = sc.textFile("/Users/amitheshmerugu/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.txt")
    parts = lines.map(lambda line: line.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    schema_people = spark.createDataFrame(people)
    schema_people.createOrReplaceTempView("people")
    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)
    # Name: Justin
    # $example off:schema_inferring$

def programmatic_schema_example(spark):
    # $example on:programmatic_schema$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("/Users/amitheshmerugu/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+
    # $example off:programmatic_schema$


if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$
    basic_df_example(spark)
    schema_inference_example(spark)
    programmatic_schema_example(spark)