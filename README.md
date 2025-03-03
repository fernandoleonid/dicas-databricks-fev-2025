
# Dicas Databricks - Análise de Dados de Vendas de Jogos

Este projeto contém a análise de dados sobre vendas de jogos em três países: Alemanha, Japão e Estados Unidos. O objetivo do projeto é realizar a leitura, limpeza e análise dos dados para entender as tendências de vendas, como as vendas totais por console e por ano.

## Estrutura dos Arquivos

- [**Germany.csv**](data/Germany.csv): Dados sobre vendas de jogos na Alemanha.
- [**Japan.csv**](data/Japan.csv): Dados sobre vendas de jogos no Japão.
- [**USA.xlsx**](data/USA.xlsx): Dados sobre vendas de jogos nos Estados Unidos.

Esses arquivos foram lidos e combinados em um único DataFrame para análise posterior.

Obs.: Para carregar os arquivos, no Azure databricks, foi utilizado a opção "Upload de ficheiros para o DBFS"

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

## Notebooks

### 1. **Notebook 01 - Ler Dados**

Este notebook é responsável por ler os dados dos arquivos CSV e Excel, adicionar uma coluna para identificar o país de origem dos dados, e unir todos os dados em um único DataFrame.

```python
# Exibir os arquivos na pasta especificada
display(dbutils.fs.ls("dbfs:/FileStore/tables/jogos"))

# Caminhos dos arquivos de dados, no caso adicionei 3 arquivos
file_path_germany = "dbfs:/FileStore/tables/jogos/Germany.csv"
file_path_japan = "dbfs:/FileStore/tables/jogos/Japan.csv"
file_path_usa = "dbfs:/FileStore/tables/jogos/USA.xlsx"

# Criar uma sessão do Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# Ler os arquivos CSV DataFrames
df_germany = spark.read.csv(file_path_germany, header=True, inferSchema=True)
df_japan = spark.read.csv(file_path_japan, header=True, inferSchema=True)

# Ler os arquivos Excel para DataFrames
df_usa = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(file_path_usa)

# Exibir os primeiros registros de cada DataFrame
df_germany.show()
df_japan.show()
df_usa.show()

# Adicionar uma coluna "País" para identificar a origem dos dados
from pyspark.sql.functions import lit
df_germany = df_germany.withColumn("País", lit("Germany"))
df_japan = df_japan.withColumn("País", lit("Japan"))
df_usa = df_usa.withColumn("País", lit("USA"))

# Unir todos os DataFrames em um único DataFrame
df_final = df_germany.union(df_japan).union(df_usa)

# Extrair apenas os números da coluna "Jogo" e converter para inteiro
from pyspark.sql.functions import regexp_extract, col
df_final = df_final.withColumn("Jogo", regexp_extract(col("Jogo"), r"(\d+)", 1).cast("int"))

# Ordenar os dados pela coluna "Jogo"
df_final = df_final.orderBy("Jogo")
df_final.show(10)

# Salvar o DataFrame consolidado como CSV
df_final.write.mode("overwrite").option("header", "true").csv("dbfs:/FileStore/tables/jogos/final.csv")
```

### 2. **Notebook 02 - Limpeza**

Este notebook realiza a limpeza dos dados, filtrando os jogos que venderam mais de 1 milhão de unidades e salvando os dados em um arquivo CSV.

```python
df_lido = spark.read.option("header", "true").csv("dbfs:/FileStore/tables/jogos/final.csv")
df_lido.show()

# Filtrar apenas os jogos que venderam mais de 1 milhão de unidades
df_lido = df_lido.filter(col("Unidades Vendidas (Milhões)") >= 1)
df_lido.show()

# Salvar o DataFrame filtrado como CSV
df_lido.coalesce(1).write.mode("overwrite").option("header", "true").csv("dbfs:/FileStore/tables/jogos/jogos_limpos.csv")
df_limpo = spark.read.option("header", "true").csv("dbfs:/FileStore/tables/jogos/jogos_limpos.csv")
df_limpo.show()
```

### 3. **Notebook 03 - Análise**

Este notebook realiza a análise dos dados, calculando o total de unidades vendidas por console e por ano.

```python
df_limpo = spark.read.option("header", "true").csv("dbfs:/FileStore/tables/jogos/jogos_limpos.csv")
df_limpo.show(5)

# Agrupar os dados pelo console e calcular o total de unidades vendidas
from pyspark.sql.functions import sum, round
df_total_por_console = df_limpo.groupBy("Console").agg(
    round(sum("Unidades Vendidas (Milhões)"), 2).alias("Total Unidades Vendidas")
)

# Ordenar do maior para o menor total de vendas
df_total_por_console = df_total_por_console.orderBy("Total Unidades Vendidas", ascending=False)
df_total_por_console.show()

# Agrupar os dados por ano e calcular o total de unidades vendidas
df_total_por_ano = df_limpo.groupBy("Ano").agg(
    round(sum("Unidades Vendidas (Milhões)"), 2).alias("Total Unidades Vendidas")
)

# Ordenar os anos do mais recente para o mais antigo
df_total_por_ano = df_total_por_ano.orderBy("Ano", ascending=False)
df_total_por_ano.show()
```

### 4. **Notebook 04 - Exportar**

Este notebook exporta os dados limpos para JSON e os salva no DBFS, além de gerar uma URL para acesso.

```python
# Converter o DataFrame para JSON e salvar no DBFS
import json
df_limpo = spark.read.option("header", "true").csv("dbfs:/FileStore/tables/jogos/jogos_limpos.csv")

# Transformar o DataFrame em uma lista de strings JSON
json_data = df_limpo.toJSON().collect()

# Criar a string JSON final
json_string = '{"jogos": [' + ",".join(json_data) + ']}'

# Salvar o JSON no DBFS
dbutils.fs.put("dbfs:/FileStore/tables/jogos/jogos_limpos2.json", json_string, overwrite=True)

# Gerar a URL do arquivo JSON

# Opção 1 - Utilizando o azure databricks 
instance_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl")
file_path = f"https://{instance_url}/files/tables/jogos/jogos_limpos2.json"

#Opção 2 - Utilizando o databricls community
instance_url = "https://community.cloud.databricks.com"
file_path = f"{instance_url}/files/tables/jogos/jogos_limpos2.json"

print("URL do arquivo:", file_path)
```

## Tecnologias Utilizadas

- **Apache Spark**: Utilizado para processamento e análise de dados em grande escala.
- **Databricks**: Plataforma para trabalhar com Spark de forma simplificada.
- **PySpark**: Interface Python para o Apache Spark.

## Contribuições

Se você quiser contribuir com este projeto, fique à vontade para fazer um fork, enviar pull requests ou abrir issues com sugestões.

## Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para mais detalhes.
