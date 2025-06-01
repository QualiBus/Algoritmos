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
schema_fields = []
with open("schema.txt", "r") as f:
    for linha in f:
        nome, tipo = linha.strip().split(":")
        schema_fields.append(StructField(nome.strip(), map_type(tipo), True))
schema_final = StructType(schema_fields)

# Reading
df = spark.read.csv("path/*.csv", header=False, schema=schema_final)

df = df.repartition(50)

df_sem_duplicatas = df.dropDuplicates(["bus_id", "updated_at"])

contagem_antes = df.count()

contagem_depois = df_sem_duplicatas.count()

percentual_duplicatas = ((contagem_antes - contagem_depois) / contagem_antes) * 100

# Writing
with open("out.txt", "w") as f:
    f.write(f"Percentual dos dados duplicados: {percentual_duplicatas}%\n")
    
spark.stop()