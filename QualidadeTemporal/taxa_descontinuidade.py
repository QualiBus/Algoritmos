from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, when, stddev, mean, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Taxa Descontinuidade Temporal") \
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

# Carregar dados (mesmo schema anterior)
df = spark.read.csv("/home/diego/dataset-df/dataset/df_per_line_code/line_code=*/*.csv", header=False, schema=_schema)

# Calcular intervalos temporais por bus_id
window_spec = Window.partitionBy("bus_id").orderBy("updated_at")
df = df.withColumn("prev_time", lag("updated_at").over(window_spec))

# Calcular diferença temporal em segundos
df = df.withColumn("time_diff", 
                  unix_timestamp(col("updated_at")) - 
                  unix_timestamp(col("prev_time")))

# Filtrar primeira linha de cada bus_id (null)
df = df.filter(col("time_diff").isNotNull())

# Calcular estatísticas por veículo
stats = df.groupBy("bus_id").agg(
    mean("time_diff").alias("avg_interval"),
    stddev("time_diff").alias("stddev_interval")
)

# Definir limiar para buraco (ex: média + 3 desvios)
LIMIAR_MULTIPLIER = 3  # Ajustável conforme necessidade
stats = stats.withColumn("limiar_buraco", 
                        col("avg_interval") + (LIMIAR_MULTIPLIER * col("stddev_interval")))

# Juntar estatísticas com dados originais
df = df.join(stats, "bus_id")

# Identificar buracos e calcular métricas
df = df.withColumn("buraco", 
                  when(col("time_diff") > col("limiar_buraco"), 1).otherwise(0))

# Calcular métricas por veículo
metricas_por_bus = df.groupBy("bus_id").agg(
    count("*").alias("total_registros"),
    _sum("buraco").alias("total_buracos"),
    _sum(when(col("buraco") == 1, col("time_diff")).otherwise(0)).alias("tempo_total_buracos")
)

# Calcular métricas globais
metricas_globais = df.agg(
    count("*").alias("total_registros_analisados"),
    _sum("buraco").alias("total_buracos_global"),
    _sum(when(col("buraco") == 1, col("time_diff")).otherwise(0)).alias("tempo_total_buracos_global")
)

# Calcular taxas
metricas_por_bus = metricas_por_bus.withColumn(
    "taxa_buracos", 
    col("total_buracos")/col("total_registros")
)

metricas_globais = metricas_globais.withColumn(
    "taxa_buracos_global",
    col("total_buracos_global")/col("total_registros_analisados")
)

# Exibir resultados
print("=== Métricas por Veículo ===")
metricas_por_bus.show()

print("\n=== Métricas Globais ===")
metricas_globais.show()

# Salvar resultados
metricas_por_bus.coalesce(1).write.csv("TaxaDescontinuidade/per_bus", header=True)
metricas_globais.coalesce(1).write.csv("TaxaDescontinuidade/global", header=True)

spark.stop()