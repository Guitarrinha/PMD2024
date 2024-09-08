import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round

# Configuração do Spark com o conector MongoDB
spark = SparkSession.builder \
    .appName("MongoSparkConnectorIntro") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/Matriculas.racaetnia") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/Matriculas.racaetnia?readPreference=primaryPreferred") \
    .getOrCreate()

# Leitura dos dados do MongoDB
df = spark.read.format("mongodb").load()

aggregated_df = df.groupBy("State Name").agg(
    sum("American Indian/Alaska Native Students").alias("Total American Indian/Alaska Native"),
    sum("Asian or Asian/Pacific Islander Students").alias("Total Asian or Asian/Pacific Islander"),
    sum("Hispanic Students").alias("Total Hispanic"),
    sum("Black or African American Students").alias("Total Black or African American"),
    sum("White Students").alias("Total White"),
    sum("Nat Hawaiian or Other Pacific Isl Students").alias("Total Nat Hawaiian or Other Pacific Isl"),
    sum("Two or More Races Students").alias("Total Two or More Races"),
    sum("Total Students All Grades").alias("Total Students")
)

# Calcular a porcentagem para cada grupo racial/étnico
percentage_df = aggregated_df.select(
    col("State Name"),
    round((col("Total American Indian/Alaska Native") / col("Total Students")) * 100, 2).alias("Percentage American Indian/Alaska Native"),
    round((col("Total Asian or Asian/Pacific Islander") / col("Total Students")) * 100, 2).alias("Percentage Asian or Asian/Pacific Islander"),
    round((col("Total Hispanic") / col("Total Students")) * 100, 2).alias("Percentage Hispanic"),
    round((col("Total Black or African American") / col("Total Students")) * 100, 2).alias("Percentage Black or African American"),
    round((col("Total White") / col("Total Students")) * 100, 2).alias("Percentage White"),
    round((col("Total Nat Hawaiian or Other Pacific Isl") / col("Total Students")) * 100, 2).alias("Percentage Nat Hawaiian or Other Pacific Isl"),
    round((col("Total Two or More Races") / col("Total Students")) * 100, 2).alias("Percentage Two or More Races")
)

# Converter para Pandas DataFrame
pdf = percentage_df.toPandas()

# Configuração do estilo dos gráficos
sns.set(style="whitegrid")

# Plotar um gráfico de barras empilhadas
pdf.set_index("State Name", inplace=True)
pdf.plot(kind="bar", stacked=True, figsize=(12, 8))

# Adicionar títulos e legendas
plt.title("Distribuição de Matrículas por Raça/Etnia em Cada Estado (%)")
plt.xlabel("Estado")
plt.ylabel("Porcentagem (%)")
plt.legend(loc="upper right", bbox_to_anchor=(1.15, 1))

# Mostrar o gráfico
plt.show()
