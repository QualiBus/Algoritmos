from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

spark = SparkSession.builder \
    .appName("Analise Velocidades") \
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

speed_df = df.filter(col("bus_speed").isNotNull() & (col("bus_speed") >= 0))

total = speed_df.count()
above_110 = speed_df.filter(col("bus_speed") > 110).count()
above_140 = speed_df.filter(col("bus_speed") > 140).count()

percent_110 = (above_110 / total) * 100 if total > 0 else 0
percent_140 = (above_140 / total) * 100 if total > 0 else 0

report = f"""
=== Análise de Velocidades ===
Total de registros de velocidade: {total:,}
Registros acima de 110 km/h: {above_110:,} ({percent_110:.2f}%)
Registros acima de 140 km/h: {above_140:,} ({percent_140:.2f}%)
"""

# Writing
with open("out.txt", "w") as f:
    f.write(report)

spark.stop()