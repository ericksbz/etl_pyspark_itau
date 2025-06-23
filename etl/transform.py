def transformar_dados(df_clientes, df_vendas):
    joined = df_vendas.join(df_clientes, on="cliente_id")

    resumo_clientes = joined.groupBy("cliente_id", "nome") \
        .agg(
            {"valor": "sum", "*": "count"}
        ) \
        .withColumnRenamed("sum(valor)", "total_vendas") \
        .withColumnRenamed("count(1)", "quantidade_vendas")

    resumo_clientes = resumo_clientes.withColumn(
        "ticket_medio", 
        (resumo_clientes["total_vendas"] / resumo_clientes["quantidade_vendas"])
    )

    resumo_produtos = df_vendas.groupBy("produto_id") \
        .agg(
            {"valor": "sum", "*": "count"}
        ) \
        .withColumnRenamed("sum(valor)", "total_vendas_produto") \
        .withColumnRenamed("count(1)", "quantidade_vendas_produto")

    resumo_produtos = resumo_produtos.withColumn(
        "ticket_medio_produto", 
        (resumo_produtos["total_vendas_produto"] / resumo_produtos["quantidade_vendas_produto"])
    )

    return resumo_clientes, resumo_produtos