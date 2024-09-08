import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round, lower
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# Configuração do Spark com o conector MongoDB
spark = SparkSession.builder \
    .appName("MongoSparkConnector") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/Matriculas.racaetnia") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/Matriculas.racaetnia?readPreference=primaryPreferred") \
    .getOrCreate()

# Leitura dos dados do MongoDB
df = spark.read.format("mongodb").load()

# Tratamento dos dados --> Remoção de items com state name nulo e colunas não utilizadas
df = df.filter(df["State Name"].isNotNull())

# Normaliza o nome dos estados para minúsculas
df = df.withColumn("State Name", lower(col("State Name")))

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
# Consulta 1: Identificar a distribuição total de matrículas por raça/etnia em cada estado
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

# Especificar o caminho onde deseja salvar o arquivo CSV
# Coletar os dados do DataFrame PySpark como um DataFrame Pandas
df_pd = percentage_df.toPandas()

# Salvar o DataFrame Pandas como um arquivo CSV
df_pd.to_csv("consulta1.csv", index=False)
percentage_df.show()

# Consulta 2: Qual região dos EUA está concentrada a maior quantidade de alunos asiáticos
# Mapeamento de estados para suas respectivas regiões
regions = {
    'Northeast': ['connecticut', 'maine', 'massachusetts', 'new hampshire', 'rhode island', 'vermont', 
                  'new jersey', 'new york', 'pennsylvania'],
    'Midwest': ['illinois', 'indiana', 'michigan', 'ohio', 'wisconsin', 'iowa', 'kansas', 'minnesota', 
                'missouri', 'nebraska', 'north dakota', 'south dakota'],
    'South': ['delaware', 'florida', 'georgia', 'maryland', 'north carolina', 'south carolina', 'virginia', 
              'district of columbia', 'west virginia', 'alabama', 'kentucky', 'mississippi', 'tennessee', 
              'arkansas', 'louisiana', 'oklahoma', 'texas', 'puerto rico', 'virgin islands', 'bureau of indian education'],
    'West': ['arizona', 'colorado', 'idaho', 'montana', 'nevada', 'new mexico', 'utah', 'wyoming', 
             'alaska', 'california', 'hawaii', 'oregon', 'washington', 'guam', 'northern marianas', 'american samoa'],
}

# Cria uma nova coluna 'Region' com base no mapeamento
def get_region(state):
    if state is None:
        return None
    state_lower = state.lower()
    for region, states in regions.items():
        if state_lower in states:
            return region
    print(f"Estado não encontrado: {state}")
    return None

# Criação da UDF (User Defined Function) para mapear estados para regiões
spark.udf.register("get_region", get_region, StringType())

# Aplica a função para criar uma coluna de região
df = df.withColumn("Region", F.expr("get_region(`State Name`)"))

# Agrupa por região e soma o número de alunos asiáticos
df_region_asian = df.groupBy("Region").agg(F.sum("Asian or Asian/Pacific Islander Students").alias("Total Asian Students"))

# Ordena para encontrar a região com o maior número de alunos asiáticos
df_region_asian = df_region_asian.orderBy(F.desc("Total Asian Students"))

# Mostra o resultado
# Coletar os dados do DataFrame PySpark como um DataFrame Pandas
df_pd = df_region_asian.toPandas()

# Salvar o DataFrame Pandas como um arquivo CSV
df_pd.to_csv("consulta2.csv", index=False)
df_region_asian.show(1)