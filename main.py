from etl.extract import read_clientes, read_vendas
from etl.transform import transformar_dados
from etl.load import salvar_resultados
from pyspark.sql import SparkSession
import sys

def main():
    try:
        spark = SparkSession.builder \
            .appName("Desafio PySpark Itaú") \
            .getOrCreate()
    except Exception as e:
        print(f"[ERRO] Falha ao iniciar SparkSession: {e}")
        sys.exit(1)

    caminho_clientes = "data/clientes.csv"
    caminho_vendas = "data/vendas.txt"

    try:
        df_clientes = read_clientes(caminho_clientes)
        df_vendas = read_vendas(caminho_vendas)
    except Exception as e:
        print(f"[ERRO] Falha ao ler arquivos de entrada: {e}")
        spark.stop()
        sys.exit(1)

    try:
        resumo_clientes, resumo_produtos = transformar_dados(df_clientes, df_vendas)
    except Exception as e:
        print(f"[ERRO] Falha durante a transformação dos dados: {e}")
        spark.stop()
        sys.exit(1)

    try:
        salvar_resultados(resumo_clientes, "output/resumo_clientes.csv")
        salvar_resultados(resumo_produtos, "output/balanco_produtos.csv")
    except Exception as e:
        print(f"[ERRO] Falha ao salvar os resultados: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("[INFO] Processo finalizado com sucesso.")

if __name__ == "__main__":
    main()