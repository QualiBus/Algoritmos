from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("NullSpeedAnalysis").getOrCreate()

_schema = StructType([
    StructField("line_code", StringType(), True),
    StructField("has_null_speed", IntegerType(), True),
    StructField("percent_null_speed", DoubleType(), True)
])

# Reading
df = spark.read.csv("path/*.csv", header=True, schema=_schema)


df = df.withColumn("categoria",
    F.when(F.col("percent_null_speed") == 100.0, "totalmente_nula")
     .when(F.col("percent_null_speed") == 0.0, "nao_nula")
     .otherwise("parcialmente_nula")
)


contagem_categorias = df.groupBy("categoria").count().collect()


contagens = {
    "totalmente_nula": 0,
    "nao_nula": 0,
    "parcialmente_nula": 0
}


for linha in contagem_categorias:
    contagens[linha["categoria"]] = linha["count"]


total = sum(contagens.values())
porcentagem_totalmente = (contagens["totalmente_nula"] / total) * 100 if total > 0 else 0
porcentagem_nao = (contagens["nao_nula"] / total) * 100 if total > 0 else 0
porcentagem_parcial = (contagens["parcialmente_nula"] / total) * 100 if total > 0 else 0


resultado = f"""Total de linhas: {total}
Velocidades 100% nulas: {contagens["totalmente_nula"]} ({porcentagem_totalmente:.2f}%)
Velocidades 0% nulas: {contagens["nao_nula"]} ({porcentagem_nao:.2f}%)
Velocidades parcialmente nulas: {contagens["parcialmente_nula"]} ({porcentagem_parcial:.2f}%)"""

# Writing
with open("out.txt", "w") as arquivo:
    arquivo.write(resultado)

spark.stop()