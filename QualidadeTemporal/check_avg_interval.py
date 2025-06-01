from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, date_format, avg, unix_timestamp, udf, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

spark = SparkSession.builder \
    .appName("check intervalos") \
    .config("spark.executor.memory", "10G") \
    .config("spark.driver.memory", "4G") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

def map_type(type_str):
    mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType()
    }
    return mapping[type_str.strip().lower()]

meus_campos = {
    "bus_id": "id",
    "updated_at": "atualizado_em",
}

# Nome_real → nome_interno
mapa_colunas = {v: k for k, v in meus_campos.items()}

# Schema.txt
# Modifique a parte "absolute_path" no código abaixo para o caminho absoluto até o schema.txt
schema_fields = []
with open("absolute_path/schema.txt", "r") as f:
    for linha in f:
        nome, tipo = linha.strip().split(":")
        schema_fields.append(StructField(nome.strip(), map_type(tipo), True))
schema_final = StructType(schema_fields)

# Reading
# Modifique a parte "absolute_path" no código abaixo para o caminho absoluto até o path/*.csv
df = spark.read.csv("absolute_path/path/*.csv", header=True, schema=schema_final)

for nome_real in df.columns:
    if nome_real in mapa_colunas:
        df = df.withColumnRenamed(nome_real, mapa_colunas[nome_real])

window_spec = Window.partitionBy("bus_id").orderBy("updated_at")
df = df.withColumn("time_diff", 
                  unix_timestamp(col("updated_at")) - 
                  unix_timestamp(lag("updated_at").over(window_spec)))

df = df.filter(col("time_diff").isNotNull())


avg_per_bus = df.groupBy("bus_id").agg(avg("time_diff").alias("avg_seconds"))


overall_avg = df.agg(avg("time_diff").alias("overall_avg_seconds"))

def seconds_to_hhmmss(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

seconds_to_hhmmss_udf = udf(seconds_to_hhmmss, StringType())


avg_per_bus = avg_per_bus.withColumn("avg_interval", 
                                    seconds_to_hhmmss_udf(col("avg_seconds")))\
                                    .select("bus_id", "avg_interval")

overall_avg = overall_avg.withColumn("overall_avg", 
                                    seconds_to_hhmmss_udf(col("overall_avg_seconds")))\
                                    .select("overall_avg")

# Writing 
(avg_per_bus.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("delimiter", ",")
 .csv("per_bus_id_avg"))

(overall_avg.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("delimiter", ",")
 .csv("overall_avg"))

spark.stop()