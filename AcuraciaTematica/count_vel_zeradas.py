from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("ZeroSpeedAnalysis").getOrCreate()

_schema = StructType([
    StructField("line_code", StringType(), True),
    StructField("has_zero_speed", IntegerType(), True),
    StructField("percent_zero_speed", DoubleType(), True)
])

# Reading

df = spark.read.csv("path_write/velocidades_nulas/*.csv", header=True, schema=_schema)

filtered_df = df.filter("has_zero_speed = 1")

avg_percent = filtered_df.agg(F.avg("percent_zero_speed").alias("media")).first()["media"]

count_over_40 = filtered_df.filter("percent_zero_speed > 40").count()

filtered_count = filtered_df.count()
percentage_over_40 = (count_over_40 / filtered_count * 100) if filtered_count > 0 else 0

resultado = f"""Análise de Velocidades Zeradas:

Média de porcentagem zerada: {avg_percent}%

Linhas com mais de 40% de zeros: {count_over_40}

Porcentagem de linhas acima de 40%: {percentage_over_40}%"""

# Writing
with open("cont_vel_zeradas.txt", "w") as arquivo:
    arquivo.write(resultado)

spark.stop()