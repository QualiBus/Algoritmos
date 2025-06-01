from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

spark = SparkSession.builder \
    .appName("Analise Velocidades") \
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
    "bus_speed": "velocidade",
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
with open("dominio_velocidades.txt", "w") as f:
    f.write(report)

spark.stop()