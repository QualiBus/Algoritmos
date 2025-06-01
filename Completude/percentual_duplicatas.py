from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import count, col

spark = SparkSession.builder \
    .appName("Remover Duplicatas") \
    .config("spark.executor.memory", "10G") \
    .config("spark.driver.memory", "4G") \
    .config("spark.executor.extraJavaOptions", "-XX:MaxHeapSize=10G") \
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

df_sem_duplicatas = df.dropDuplicates(["bus_id", "updated_at"])

contagem_antes = df.count()

contagem_depois = df_sem_duplicatas.count()

percentual_duplicatas = ((contagem_antes - contagem_depois) / contagem_antes) * 100

# Writing
with open("percent_duplicatas.txt", "w") as f:
    f.write(f"Percentual dos dados duplicados: {percentual_duplicatas}%\n")
    
spark.stop()