from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, regexp_extract, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
import json

spark = SparkSession.builder \
    .appName("Identificar Linhas com Velocidades Zeradas") \
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
    "line_code": "codigo_linha",
    "bus_speed": "velocidade",
}

# Inverte para: nome_real → nome_interno
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


# Cálculo das velocidades zeradas por linha
df_grouped = df.groupBy("line_code").agg(
    when(count(when(col("bus_speed") == 0, True)) > 0, 1).otherwise(0).alias("has_zero_speed"),
    (count(when(col("bus_speed") == 0, True)) / count("*") * 100).alias("percent_zero_speed")
)

df_grouped.write.csv("path_write/velocidades_nulas", header=True, mode="overwrite")


spark.stop()