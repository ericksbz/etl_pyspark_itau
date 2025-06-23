def salvar_resultados(df, caminho_saida):
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(caminho_saida)