from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.utils import AnalysisException
from pathlib import Path

def ingest_data(tbl_name, execution_date):
    year, month, day = execution_date.split("-")

    spark = SparkSession.builder.appName("SQL to Data Lake").getOrCreate()

    jdbc_url = "jdbc:mysql://localhost:3306/DATN?user=kitkat&password=123"

    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", tbl_name) \
        .option("fetchSize", "100") \
        .load()

    path_to_parquet = f"hdfs://192.168.190.128:9000/datalake/{tbl_name}"

    try:
        try:
            previous_max_id = spark.read.parquet(path_to_parquet) \
                    .agg({"id": "max"}) \
                    .collect()[0][0]
        except AnalysisException as e:
            print(f"AnalysisException: {e}")
            previous_max_id = 0

        new_data = df.filter(col("id") > previous_max_id)

        if new_data.count() > 0:
                new_data.withColumn("year", lit(year)) \
                .withColumn("month", lit(month)) \
                .withColumn("day", lit(day)) \
                .write.partitionBy("year", "month", "day").mode("append").parquet(path_to_parquet)
                
    except AnalysisException as e:
        print(f"AnalysisException: {e}")
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Need 2 args: --tblName [table_name] --executionDate [execution_date]")
        sys.exit(1)
    tbl_name = ""
    execution_date = ""
    args = iter(sys.argv[1:])
    for arg in args:
        if arg == "--tblName":
            tbl_name = next(args, None)
        elif arg == "--executionDate":
            execution_date = next(args, None)

    if not tbl_name or not execution_date:
        print("Missing arguments!")
        sys.exit(1)
    ingest_data(tbl_name, execution_date)
