# etl_pyspark_itau

Este repositório contém a solução desenvolvida para o teste técnico do Itaú Unibanco, referente à vaga de Analista de Dados Pleno. O projeto consiste na criação de um processo ETL utilizando PySpark, executado em ambiente local.
💻 Ambiente de Desenvolvimento

O projeto foi desenvolvido em uma máquina virtual (Oracle VirtualBox) com Ubuntu 24.04.2 e utilizando a IDE Visual Studio Code.
⚙️ Requisitos e Configuração do Ambiente

Para garantir o correto funcionamento do projeto, recomenda-se o uso das seguintes versões:

    JDK: 17

    Python: 3.10

    PySpark: instalado via pip

    pytest: para execução dos testes

⚠️ Importante: Certifique-se de que o JDK e o PySpark estão em versões compatíveis. Versões divergentes podem causar falhas na execução.

🚀 Passo a Passo para Execução

1 - Criar ambiente virtual:
python3 -m venv venv

2 - Ativar o ambiente virtual (Linux/Mac):
source venv/bin/activate

3 - Instalar dependências:
pip install pytest

4 - Executar os testes com o pytest:
python3 -m pytest tests/

💡 Dica: Sempre ative o ambiente virtual antes de rodar o projeto ou os testes.

☕ Instalação do JDK 17 no Ubuntu

Caso ainda não tenha o JDK instalado, utilize os comandos abaixo no terminal:
sudo apt update
sudo apt install openjdk-17-jdk

Para verificar se a instalação foi bem-sucedida:
java -version

▶️ Executando o Projeto

Após configurar o ambiente:

 - Abra o projeto no VS Code.

 - Acesse o arquivo main.py.

 - Clique em "Executar Arquivo Python" (ou utilize o atalho da IDE).

Os resultados serão salvos na pasta output. Dentro de cada subpasta haverá um arquivo .csv contendo os dados das tabelas processadas.

Exemplo do resultado esperado no output:
Para resumo do cliente:
cliente_id,nome,total_vendas,quantidade_vendas,ticket_medio
10003,Carlos Lima,30500.2,1,30500.2
10005,Lucas Rocha,50000.2,1,50000.2
10002,Maria Souza,20000.2,1,20000.2
10004,Ana Paula,10000.2,1,10000.2
10001,João Silva,12345.2,1,12345.2

Para balanço de produtos:
produto_id,total_vendas_produto,quantidade_vendas_produto,ticket_medio_produto
10002,20000.2,1,20000.2
10001,22345.4,2,11172.7
10003,30500.2,1,30500.2
10004,50000.2,1,50000.2

