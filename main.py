from etl.extract import read_clientes, read_vendas
from etl.transform import transformar_dados
from etl.load import salvar_resultados

def main():
    caminho_clientes = "data/clientes.csv"
    caminho_vendas = "data/vendas.txt"

    df_clientes = read_clientes(caminho_clientes)
    df_vendas = read_vendas(caminho_vendas)

    resumo_clientes, resumo_produtos = transformar_dados(df_clientes, df_vendas)

    salvar_resultados(resumo_clientes, "output/resumo_clientes.csv")
    salvar_resultados(resumo_produtos, "output/balanco_produtos.csv")

if __name__ == "__main__":
    main()