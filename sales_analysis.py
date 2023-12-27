from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DataType, DateType

spark = SparkSession.builder.appName("SalesDataAnalysis").master("local[*]").getOrCreate()
sales_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("Customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("source_order", IntegerType(), True)
])
menu_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

sales_df = spark.read.format("csv").option("inferschema", "true"). \
    schema(sales_schema).load("/Users/soumyakantarath/Desktop/DS_Monk/PySparkPractice/resources/sales.csv.txt")

sales_df.show(truncate=False)

menu_df = spark.read.format("csv").option("inferschema", "true").schema(menu_schema). \
    load("/Users/soumyakantarath/Desktop/DS_Monk/PySparkPractice/resources/menu.csv.txt")
menu_df.show()
