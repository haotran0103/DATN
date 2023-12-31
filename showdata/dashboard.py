import streamlit as st
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, split, explode
from wordcloud import WordCloud
import plotly.express as px

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("DataWarehouseDashboard").getOrCreate()

# Đọc dữ liệu từ HDFS
transaction_data_warehouse = spark.read.parquet("hdfs://192.168.190.128:9000/warehouse/transaction_data_warehouse")
reviews_data_warehouse = spark.read.parquet("hdfs://192.168.190.128:9000/warehouse/reviews_data_warehouse")

# Tiêu đề của trang web
st.title("Data Warehouse Dashboard")

# Lấy ngày mới nhất từ transaction_data_warehouse và reviews_data_warehouse
latest_partition = transaction_data_warehouse.selectExpr("max(transaction_year) as max_year", "max(transaction_month) as max_month", "max(transaction_day) as max_day").collect()[0]
max_year, max_month, max_day = latest_partition["max_year"], latest_partition["max_month"], latest_partition["max_day"]
latest_transaction_data = transaction_data_warehouse.filter((col("transaction_year") == max_year) & (col("transaction_month") == max_month) & (col("transaction_day") == max_day))

latest_partition = reviews_data_warehouse.selectExpr("max(review_year) as max_year", "max(review_month) as max_month", "max(review_day) as max_day").collect()[0]
max_year, max_month, max_day = latest_partition["max_year"], latest_partition["max_month"], latest_partition["max_day"]
latest_reviews_data = reviews_data_warehouse.filter((col("review_year") == max_year) & (col("review_month") == max_month) & (col("review_day") == max_day))

# Lựa chọn Data Warehouse
selected_data_warehouse = st.sidebar.selectbox("Chọn Data Warehouse:", ["Transaction Data Warehouse", "Reviews Data Warehouse"])

# Phân tích Transaction Data Warehouse
if selected_data_warehouse == "Transaction Data Warehouse":
    
    # Hiển thị bảng dữ liệu giao dịch
    st.subheader("Transaction Data Warehouse")
    st.write(latest_transaction_data.toPandas())

    # Tính toán và hiển thị tổng doanh thu và tổng số lượng giao dịch
    total_amount = latest_transaction_data.agg({"amount": "sum"}).collect()[0][0]
    total_transactions = latest_transaction_data.count()
    
    st.subheader("Tổng Doanh Thu và Số Lượng Giao Dịch")
    col1, col2 = st.columns(2)
    with col1:
        st.write(
                f'<div style="text-align:center; box-shadow: 5px 5px 10px #888888; padding: 20px;">'
                f'<h3>Tổng doanh thu</h3>'
                f'<h2>{total_amount:,.0f}</h2>'
                f'</div>',
                unsafe_allow_html=True
            )
    with col2:
        st.write(
            f'<div style="text-align:center; box-shadow: 5px 5px 10px #888888; padding: 20px;">'
            f'<h3>Tổng số lượng giao dịch</h3>'
            f'<h2>{total_transactions:,.0f}</h2>'
            f'</div>',
            unsafe_allow_html=True
        )

    # Top 5 sản phẩm có tổng amount cao nhất và thấp nhất
    top_products_highest_amount = latest_transaction_data.groupBy("model_name").agg({"amount": "sum"}).orderBy("sum(amount)", ascending=False).limit(5).toPandas()
    top_products_lowest_amount = latest_transaction_data.groupBy("model_name").agg({"amount": "sum"}).orderBy("sum(amount)").limit(5).toPandas()

    # Biểu đồ 1: Top 5 sản phẩm có tổng amount cao nhất
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15, 6))
    axes[0].bar(top_products_highest_amount["model_name"], top_products_highest_amount["sum(amount)"])
    axes[0].set_title("Top 5 sản phẩm có tổng amount cao nhất")
    axes[0].set_xlabel("Product Name")
    axes[0].set_ylabel("Total Amount")
    axes[0].tick_params(axis='x', rotation=45)

    # Biểu đồ 2: Top 5 sản phẩm có tổng amount thấp nhất
    axes[1].bar(top_products_lowest_amount["model_name"], top_products_lowest_amount["sum(amount)"])
    axes[1].set_title("Top 5 sản phẩm có tổng amount thấp nhất")
    axes[1].set_xlabel("Product Name")
    axes[1].set_ylabel("Total Amount")
    axes[1].tick_params(axis='x', rotation=45)

    st.pyplot(fig)

    # Biểu đồ tổng quan về doanh thu theo sản phẩm
    st.header("Revenue Overview by Product")
    fig_revenue = px.pie(latest_transaction_data, names='model_name', values='amount', title='Revenue by Product')
    st.plotly_chart(fig_revenue)

    # Biểu đồ phân phối phương thức thanh toán
    st.subheader('Payment Method Distribution')
    payment_method_counts = latest_transaction_data.groupBy("payment_method").count().toPandas()
    fig, ax = plt.subplots()
    ax.pie(payment_method_counts['count'], labels=payment_method_counts['payment_method'], autopct='%1.1f%%')
    ax.set_ylabel('')
    st.pyplot(fig)

    # Biểu đồ phân phối giao dịch theo loại sản phẩm (Top 10)
    transactions_per_product = latest_transaction_data.groupBy("model_name").count().orderBy("count", ascending=False).limit(10).toPandas()
    plt.figure(figsize=(10, 6))
    plt.bar(transactions_per_product["model_name"], transactions_per_product["count"])
    plt.xlabel('Product Name')
    plt.ylabel('Số Lượng Giao Dịch')
    plt.title('Phân Phối Giao Dịch Theo Sản Phẩm (Top 10)')
    plt.xticks(rotation=45)
    st.pyplot(plt)

else:
    st.subheader("Reviews Data Warehouse")
    st.write(latest_reviews_data.toPandas())

    # Biểu đồ phần trăm đánh giá
    st.subheader("Biểu đồ phần trăm đánh giá")
    rating_percentages = latest_reviews_data.groupBy("rating").count()
    labels = rating_percentages.select("rating").rdd.flatMap(lambda x: x).collect()
    sizes = rating_percentages.select("count").rdd.flatMap(lambda x: x).collect()

    # Biểu đồ phân tích phần trăm đánh giá
    fig = plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.axis('equal')
    st.pyplot(fig)

    # Xác định top sản phẩm được đánh giá cao nhất và thấp nhất
    top_products_highest_ratings = latest_reviews_data.groupBy("model_name") \
        .agg({"rating": "avg", "*": "count"}) \
        .orderBy(["avg(rating)", "count(1)"], ascending=[0, 0]) \
        .limit(5).toPandas()

    top_products_lowest_ratings = latest_reviews_data.groupBy("model_name") \
        .agg({"rating": "avg", "*": "count"}) \
        .orderBy(["avg(rating)", "count(1)"], ascending=[1, 0]) \
        .limit(5).toPandas()

    # Biểu đồ 1: Top 5 sản phẩm được đánh giá cao nhất
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15, 6))
    axes[0].bar(top_products_highest_ratings["model_name"], top_products_highest_ratings["avg(rating)"])
    axes[0].set_title("Top 5 sản phẩm được đánh giá cao nhất")
    axes[0].set_xlabel("Product Name")
    axes[0].set_ylabel("Average Rating")
    axes[0].tick_params(axis='x', rotation=45)

    # Biểu đồ 2: Top 5 sản phẩm được đánh giá thấp nhất
    axes[1].bar(top_products_lowest_ratings["model_name"], top_products_lowest_ratings["avg(rating)"])
    axes[1].set_title("Top 5 sản phẩm được đánh giá thấp nhất")
    axes[1].set_xlabel("Product Name")
    axes[1].set_ylabel("Average Rating")
    axes[1].tick_params(axis='x', rotation=45)

    st.pyplot(fig)

    # Word Cloud từ phản hồi
    words_in_reviews = latest_reviews_data.withColumn("words", split(latest_reviews_data["comment"], "\s+")).select(explode("words").alias("word"))
    word_frequencies = words_in_reviews.groupBy("word").count().orderBy("count", ascending=False).limit(50).toPandas()

    wordcloud = WordCloud(width=800, height=400, background_color='white', prefer_horizontal=1.0).generate_from_frequencies(dict(zip(word_frequencies['word'], word_frequencies['count'])))

    plt.figure(figsize=(10, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Word Cloud - Top 50 Từ Phổ Biến Trong Phản Hồi', fontdict={'fontsize': 16})
    st.pyplot(plt)
