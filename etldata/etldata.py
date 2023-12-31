from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys

def main():
    execution_date = ""
    args = iter(sys.argv[1:])
    for arg in args:
        if arg == "--executionDate":
            execution_date = next(args, None)

    if not execution_date:
        print("Missing execution date argument!")
        sys.exit(1)

    spark = SparkSession.builder.appName("ETL for Data Warehousing").getOrCreate()

    year, month, day = execution_date.split("-")

    # Đọc dữ liệu từ các bảng trong HDFS với điều kiện lọc theo partition
    customers_df = spark.read.parquet(f"hdfs://192.168.190.128:9000/datalake/Customers/year={year}/month={month}/day={day}")
    car_models_df = spark.read.parquet(f"hdfs://192.168.190.128:9000/datalake/CarModels/year={year}/month={month}/day={day}")
    transactions_df = spark.read.parquet(f"hdfs://192.168.190.128:9000/datalake/Transactions/year={year}/month={month}/day={day}")
    reviews_df = spark.read.parquet(f"hdfs://192.168.190.128:9000/datalake/Reviews/year={year}/month={month}/day={day}")

    # Xây dựng kho dữ liệu về Giao dịch (Transaction Data Warehouse)
    transaction_data_warehouse = transactions_df.join(customers_df, transactions_df["customer_id"] == customers_df["id"], "inner") \
        .join(car_models_df, transactions_df["model_id"] == car_models_df["id"], "inner") \
        .withColumn("transaction_year", lit(year)) \
        .withColumn("transaction_month", lit(month)) \
        .withColumn("transaction_day", lit(day)) \
        .select(transactions_df["customer_id"], customers_df["customer_name"], transactions_df["model_id"], car_models_df["model_name"], transactions_df["id"].alias("transaction_id"), transactions_df["amount"], transactions_df["payment_method"], lit(year).alias("transaction_year"), lit(month).alias("transaction_month"), lit(day).alias("transaction_day"))

    # Lưu dữ liệu vào Transaction Data Warehouse
    transaction_data_warehouse.write.partitionBy("transaction_year", "transaction_month", "transaction_day") \
        .mode("append") \
        .parquet("hdfs://192.168.190.128:9000/warehouse/transaction_data_warehouse")

    # Xây dựng kho dữ liệu về Đánh giá và Phản hồi (Review & Feedback Data Warehouse)
    reviews_data_warehouse = reviews_df.join(customers_df, reviews_df["customer_id"] == customers_df["id"], "inner") \
        .join(car_models_df, reviews_df["model_id"] == car_models_df["id"], "inner") \
        .withColumn("review_year", lit(year)) \
        .withColumn("review_month", lit(month)) \
        .withColumn("review_day", lit(day)) \
        .select(reviews_df["customer_id"], customers_df["customer_name"], reviews_df["model_id"], car_models_df["model_name"], reviews_df["id"].alias("review_id"), reviews_df["rating"], reviews_df["comment"], lit(year).alias("review_year"), lit(month).alias("review_month"), lit(day).alias("review_day"))

    # Lưu dữ liệu vào Review & Feedback Data Warehouse
    reviews_data_warehouse.write.partitionBy("review_year", "review_month", "review_day") \
        .mode("append") \
        .parquet("hdfs://192.168.190.128:9000/warehouse/reviews_data_warehouse")

if __name__ == "__main__":
    main()
