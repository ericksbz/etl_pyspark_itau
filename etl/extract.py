from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("ETL Itaú - Extract").getOrCreate()

def read_clientes(caminho):
    return spark.read.option("header", True).csv(caminho)

def read_vendas(caminho):
    def parse_linha(linha):
        return (
            int(linha[0:5]),
            int(linha[5:10]),
            int(linha[10:15]),
            float(linha[15:23]) / 100,
            linha[23:31]
        )

    linhas = spark.read.text(caminho).rdd.map(lambda r: parse_linha(r[0]))
    
    schema = StructType([
        StructField("venda_id", IntegerType(), True),
        StructField("cliente_id", IntegerType(), True),
        StructField("produto_id", IntegerType(), True),
        StructField("valor", DoubleType(), True),
        StructField("data_venda", StringType(), True),
    ])
    
    return spark.createDataFrame(linhas, schema)