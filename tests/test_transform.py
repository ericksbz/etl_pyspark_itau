import pytest
from pyspark.sql import SparkSession
from etl.transform import transformar_dados

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_data(spark):
    df_clientes = spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob"),
    ], ["cliente_id", "nome"])

    df_vendas = spark.createDataFrame([
        (1, 100.0, 10),
        (1, 200.0, 20),
        (2, 300.0, 10),
    ], ["cliente_id", "valor", "produto_id"])

    return df_clientes, df_vendas

def test_transformar_dados_basic(spark, sample_data):
    df_clientes, df_vendas = sample_data
    resumo_clientes, resumo_produtos = transformar_dados(df_clientes, df_vendas)

    clientes = {row['cliente_id']: row for row in resumo_clientes.collect()}
    assert clientes[1]['total_vendas'] == 300.0
    assert clientes[1]['quantidade_vendas'] == 2
    assert pytest.approx(clientes[1]['ticket_medio']) == 150.0

    assert clientes[2]['total_vendas'] == 300.0
    assert clientes[2]['quantidade_vendas'] == 1
    assert pytest.approx(clientes[2]['ticket_medio']) == 300.0

    produtos = {row['produto_id']: row for row in resumo_produtos.collect()}
    assert produtos[10]['total_vendas_produto'] == 400.0
    assert produtos[10]['quantidade_vendas_produto'] == 2
    assert pytest.approx(produtos[10]['ticket_medio_produto']) == 200.0

    assert produtos[20]['total_vendas_produto'] == 200.0
    assert produtos[20]['quantidade_vendas_produto'] == 1
    assert pytest.approx(produtos[20]['ticket_medio_produto']) == 200.0

def test_transformar_dados_empty(spark):
    df_clientes = spark.createDataFrame([], "cliente_id INT, nome STRING")
    df_vendas = spark.createDataFrame([], "cliente_id INT, valor DOUBLE, produto_id INT")
    resumo_clientes, resumo_produtos = transformar_dados(df_clientes, df_vendas)
    assert resumo_clientes.count() == 0
    assert resumo_produtos.count() == 0

def test_transformar_dados_cliente_sem_venda(spark):
    df_clientes = spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
    ], ["cliente_id", "nome"])
    df_vendas = spark.createDataFrame([
        (1, 100.0, 10),
        (2, 200.0, 20),
    ], ["cliente_id", "valor", "produto_id"])
    resumo_clientes, _ = transformar_dados(df_clientes, df_vendas)
    clientes_ids = set(row['cliente_id'] for row in resumo_clientes.collect())
    assert 3 not in clientes_ids  # Cliente sem venda não aparece no resumo

def test_transformar_dados_produto_sem_venda(spark):
    df_clientes = spark.createDataFrame([
        (1, "Alice"),
    ], ["cliente_id", "nome"])
    df_vendas = spark.createDataFrame([
        (1, 100.0, 10),
    ], ["cliente_id", "valor", "produto_id"])
    _, resumo_produtos = transformar_dados(df_clientes, df_vendas)
    produtos_ids = set(row['produto_id'] for row in resumo_produtos.collect())
    assert 10 in produtos_ids
    assert 20 not in produtos_ids  # Produto sem venda não aparece no resumo