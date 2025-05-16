from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import count, col

spark = SparkSession.builder \
    .appName("Remover Duplicatas") \
    .config("spark.executor.memory", "10G") \
    .config("spark.driver.memory", "4G") \
    .config("spark.executor.extraJavaOptions", "-XX:MaxHeapSize=10G") \
    .getOrCreate()

_schema = StructType([
    StructField("bus_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("city_code", IntegerType(), True),
    StructField("adapted", BooleanType(), True), 
    StructField("agency", StringType(), True),
    StructField("line_url", StringType(), True),
    StructField("line_fare", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bus_speed", DoubleType(), True),
    StructField("is_eletric", BooleanType(), True),
    StructField("bus_type", StringType(), True),
    StructField("bus_direction", IntegerType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Reading
df = spark.read.csv("path/*.csv", header=False, schema=_schema)

df = df.repartition(50)

df_sem_duplicatas = df.dropDuplicates(["bus_id", "updated_at"])

contagem_antes = df.count()

contagem_depois = df_sem_duplicatas.count()

percentual_duplicatas = ((contagem_antes - contagem_depois) / contagem_antes) * 100

# Writing
with open("out.txt", "w") as f:
    f.write(f"Percentual dos dados duplicados: {percentual_duplicatas}%\n")
    
spark.stop()