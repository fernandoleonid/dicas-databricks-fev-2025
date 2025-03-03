# Análise de Dados de Vendas de Jogos

Este projeto contém a análise de dados sobre vendas de jogos em três países: Alemanha, Japão e Estados Unidos. O objetivo do projeto é realizar a leitura, limpeza e análise dos dados para entender as tendências de vendas, como as vendas totais por console e por ano.

## Estrutura dos Arquivos

- **Germany.csv**: Dados sobre vendas de jogos na Alemanha.
- **Japan.csv**: Dados sobre vendas de jogos no Japão.
- **USA.xlsx**: Dados sobre vendas de jogos nos Estados Unidos.

Esses arquivos foram lidos e combinados em um único DataFrame para análise posterior.

## Etapas do Processo

### 1. **Leitura dos Dados**
Os dados foram lidos a partir de arquivos CSV (para Alemanha e Japão) e Excel (para os Estados Unidos) utilizando o Spark. Para o Excel, a biblioteca `spark-excel` foi utilizada para fazer a leitura de arquivos `.xlsx`.

### 2. **Limpeza dos Dados**
Após a leitura dos dados, uma coluna adicional foi adicionada para identificar a origem dos dados (país). Também foi realizada a conversão da coluna "Jogo" para números inteiros e os dados foram ordenados pela coluna "Jogo". Em seguida, os dados foram filtrados para incluir apenas os jogos com vendas superiores a 1 milhão de unidades.

### 3. **Análise dos Dados**
A análise foi realizada com base nas seguintes métricas:
- **Vendas por Console**: O total de unidades vendidas por console foi calculado.
- **Vendas por Ano**: O total de unidades vendidas por ano foi analisado.

### 4. **Exportação dos Dados**
Os dados limpos e analisados foram exportados em diferentes formatos:
- CSV: Os dados filtrados (somente jogos com mais de 1 milhão de unidades vendidas) foram salvos em um arquivo CSV.
- JSON: O DataFrame final foi transformado em um arquivo JSON, que também foi armazenado no DBFS.

## Acessando os Arquivos

Você pode acessar os arquivos exportados diretamente do Databricks:

- **Arquivo CSV com os dados filtrados**:  
  [Jogos Limpos (CSV)](https://community.cloud.databricks.com/files/tables/jogos/jogos_limpos.csv)
  
- **Arquivo JSON com os dados limpos**:  
  [Jogos Limpos (JSON)](https://community.cloud.databricks.com/files/tables/jogos/jogos_limpos2.json)

## Código

O código utilizado para ler, limpar, analisar e exportar os dados é descrito abaixo:

```python
# Leitura dos arquivos
file_path_germany = "dbfs:/FileStore/tables/jogos/Germany.csv"
file_path_japan = "dbfs:/FileStore/tables/jogos/Japan.csv"
file_path_usa = "dbfs:/FileStore/tables/jogos/USA.xlsx"

# Criar uma sessão do Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# Ler os arquivos CSV
df_germany = spark.read.csv(file_path_germany, header=True, inferSchema=True)
df_japan = spark.read.csv(file_path_japan, header=True, inferSchema=True)

# Ler os arquivos Excel
df_usa = spark.read.format("com.crealytics.spark.excel") \
                   .option("header", "true")\
                   .option("inferSchema", "true")\
                   .load(file_path_usa)

# Exibir os primeiros registros
df_germany.show()
df_japan.show()
df_usa.show()

# Adicionar coluna 'País' para identificar a origem dos dados
from pyspark.sql.functions import lit
df_germany = df_germany.withColumn("País", lit("Germany"))
df_japan = df_japan.withColumn("País", lit("Japan"))
df_usa = df_usa.withColumn("País", lit("USA"))

# Unir os DataFrames
df_final = df_germany.union(df_japan).union(df_usa)

# Limpeza e ordenação dos dados
from pyspark.sql.functions import regexp_extract, col
df_final = df_final.withColumn("Jogo", regexp_extract(col("Jogo"), r"(\d+)", 1).cast("int"))
df_final = df_final.orderBy("Jogo")

# Filtragem dos dados
df_final = df_final.filter(col("Unidades Vendidas (Milhões)") >= 1)

# Salvar o DataFrame filtrado como CSV
df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv("dbfs:/FileStore/tables/jogos/jogos_limpos.csv")

# Análise: Vendas por Console e Ano
from pyspark.sql.functions import sum, round
df_total_por_console = df_final.groupBy("Console").agg(
    round(sum("Unidades Vendidas (Milhões)"), 2).alias("Total Unidades Vendidas")
)

df_total_por_ano = df_final.groupBy("Ano").agg(
    round(sum("Unidades Vendidas (Milhões)"), 2).alias("Total Unidades Vendidas")
)

# Exportar para JSON
json_data = df_final.toJSON().collect()
json_string = '{"jogos": [' + ",".join(json_data) + ']}'
dbutils.fs.put("dbfs:/FileStore/tables/jogos/jogos_limpos2.json", json_string, overwrite=True)
