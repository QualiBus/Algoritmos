from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, regexp_extract, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# Configuração do Spark
spark = SparkSession.builder \
    .appName("Identificar Linhas com Velocidades Nulas") \
    .config("spark.executor.memory", "10G") \
    .config("spark.driver.memory", "4G") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Definição do schema
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
    StructField("bus_speed", DoubleType(), True),  # Coluna que pode ter valores nulos
    StructField("is_eletric", BooleanType(), True),
    StructField("bus_type", StringType(), True),
    StructField("bus_direction", IntegerType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Leitura dos dados sem considerar o cabeçalho e aplicando o schema manual
df = spark.read.csv("0.301/*.csv", header=False, schema=_schema)

# Extrair o line_code do caminho do arquivo e converter para DoubleType
df = df.withColumn("file_path", input_file_name()) \
       .withColumn("line_code", regexp_extract(col("file_path"), r"line_code=([\d.]+)", 1).cast(DoubleType())) \
       .drop("file_path")  # Remover a coluna file_path, pois não é mais necessária

# Verificar se há velocidades nulas para cada line_code
df_grouped = df.groupBy("line_code").agg(
    when(count(when(col("bus_speed").isNull(), True)) > 0, 1).otherwise(0).alias("has_null_speed"),
    (count(when(col("bus_speed").isNull(), True)) / count("*") * 100).alias("percent_null_speed")
)

# Salvar o resultado em um arquivo CSV
df_grouped.write.csv("0.301/velocidades_nulas_0.301", header=True, mode="overwrite")

# Calcular a porcentagem geral de velocidades nulas
total_null_speed_percent = df.select(
    (count(when(col("bus_speed").isNull(), True)) / count("*") * 100
).alias("percent_null_speed")
).collect()[0]["percent_null_speed"]

# Gerar o arquivo TXT com a porcentagem geral de velocidades nulas
with open("0.301/percent_null_speed.txt", "w") as f:
    f.write(f"Percentual Geral de Velocidades Nulas: {total_null_speed_percent:.2f}%\n")

# Encerrar a sessão do Spark
spark.stop()