from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 1. Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Projeto Big Data - Epidemiologia Nacional") \
    .getOrCreate()

# 2. Carregar o CSV com dados nacionais
csv_path = "caso_full.csv"  # Substitua pelo caminho real
df_spark = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

# 3. Selecionar e agrupar os dados por data
df_spark_clean = df_spark.select("date", "new_confirmed", "new_deaths")
df_grouped_spark = df_spark_clean.groupBy("date").sum("new_confirmed", "new_deaths")

# 4. Converter para Pandas para visualização e análise
df_grouped_pandas = df_grouped_spark.toPandas()
df_grouped_pandas.columns = ["date", "new_confirmed", "new_deaths"]
df_grouped_pandas["date"] = pd.to_datetime(df_grouped_pandas["date"])
df_grouped_pandas.sort_values("date", inplace=True)

# 5. Calcular médias móveis de 7 dias
df_grouped_pandas["mm_casos"] = df_grouped_pandas["new_confirmed"].rolling(7).mean()
df_grouped_pandas["mm_obitos"] = df_grouped_pandas["new_deaths"].rolling(7).mean()

# 6. Plotar gráfico
plt.figure(figsize=(14, 6))
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["new_confirmed"], alpha=0.3, label="Novos Casos (diário)", color="blue")
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["mm_casos"], label="Média Móvel Casos (7 dias)", color="blue")
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["new_deaths"], alpha=0.3, label="Novos Óbitos (diário)", color="red")
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["mm_obitos"], label="Média Móvel Óbitos (7 dias)", color="red")
plt.title("Evolução de Casos e Óbitos no Brasil")
plt.xlabel("Data")
plt.ylabel("Número de Casos/Óbitos")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("evolucao_casos_obitos_completa.png")
plt.show()

'''
# Versão com (Hadoop)
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 1. Criar a sessão Spark com suporte ao HDFS
spark = SparkSession.builder \
    .appName("Projeto Big Data - Epidemiologia Nacional") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# 2. Carregar o CSV com dados nacionais a partir do HDFS
csv_path = "hdfs://localhost:9000/user/seu_usuario/caso_full.csv"  # Substitua pelo caminho real no HDFS
df_spark = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

# 3. Selecionar e agrupar os dados por data
df_spark_clean = df_spark.select("date", "new_confirmed", "new_deaths")
df_grouped_spark = df_spark_clean.groupBy("date").sum("new_confirmed", "new_deaths")

# 4. Converter para Pandas para visualização e análise
df_grouped_pandas = df_grouped_spark.toPandas()
df_grouped_pandas.columns = ["date", "new_confirmed", "new_deaths"]
df_grouped_pandas["date"] = pd.to_datetime(df_grouped_pandas["date"])
df_grouped_pandas.sort_values("date", inplace=True)

# 5. Calcular médias móveis de 7 dias
df_grouped_pandas["mm_casos"] = df_grouped_pandas["new_confirmed"].rolling(7).mean()
df_grouped_pandas["mm_obitos"] = df_grouped_pandas["new_deaths"].rolling(7).mean()

# 6. Plotar gráfico
plt.figure(figsize=(14, 6))
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["new_confirmed"], alpha=0.3, label="Novos Casos (diário)", color="blue")
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["mm_casos"], label="Média Móvel Casos (7 dias)", color="blue")
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["new_deaths"], alpha=0.3, label="Novos Óbitos (diário)", color="red")
plt.plot(df_grouped_pandas["date"], df_grouped_pandas["mm_obitos"], label="Média Móvel Óbitos (7 dias)", color="red")
plt.title("Evolução de Casos e Óbitos no Brasil")
plt.xlabel("Data")
plt.ylabel("Número de Casos/Óbitos")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("evolucao_casos_obitos_completa.png")
plt.show()
'''
