from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

require_minimum_pandas_version()
require_minimum_pyarrow_version()


def dataframe_with_arrow_example(spark):
    import numpy as np
    import pandas as pd
    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # Generate a Pandas DataFrame
    pdf = pd.DataFrame(np.random.rand(100, 3))

    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    df = spark.createDataFrame(pdf)
    result_pdf = df.select("*").toPandas()
    print("Pandas DataFrame result statistics:\n%s\n" % str(result_pdf.describe()))

def scalar_pandas_udf_example(spark):
    from pyspark.sql.functions import col, pandas_udf
    from pyspark.sql.types import LongType
    import pandas as pd
    # Declare the function and create the UDF
    def multiply_func(a, b):
        return a * b
    multiply = pandas_udf(multiply_func, returnType=LongType())
    # The function for a pandas_udf should be able to execute with local Pandas data
    x = pd.Series([1, 2, 3])
    print(multiply_func(x, x))
    # Create a Spark DataFrame, 'spark' is an existing SparkSession
    df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))
    # Execute function as a Spark vectorized UDF
    df.select(multiply(col("x"), col("x"))).show()


def grouped_agg_pandas_udf_example(spark):
    # $example on:grouped_agg_pandas_udf$
    from pyspark.sql.functions import pandas_udf, PandasUDFType
    from pyspark.sql import Window

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v):
        return v.mean()

    df.groupby("id").agg(mean_udf(df['v'])).show()
    # +---+-----------+
    # | id|mean_udf(v)|
    # +---+-----------+
    # |  1|        1.5|
    # |  2|        6.0|
    # +---+-----------+

    w = Window \
        .partitionBy('id') \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()
    # +---+----+------+
    # | id|   v|mean_v|
    # +---+----+------+
    # |  1| 1.0|   1.5|
    # |  1| 2.0|   1.5|
    # |  2| 3.0|   6.0|
    # |  2| 5.0|   6.0|
    # |  2|10.0|   6.0|
    # +---+----+------+
    # $example off:grouped_agg_pandas_udf$



if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()
    print("Running Pandas to/from conversion example")
    dataframe_with_arrow_example(spark)
    print("Running pandas_udf scalar example")
    scalar_pandas_udf_example(spark)
    print("Running pandas_udf grouped map example")
    grouped_agg_pandas_udf_example(spark)

    spark.stop()



