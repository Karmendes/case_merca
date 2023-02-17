# Modelo de cross - sell

## Objetivo

O objetivo final é criar um dataframe onde tenhamos os apenas os 5 produtos mais vendidos conjuntamente com outros produtos. O código para isso está no arquivo cross_sell.ipynb

### Passo 1
Como os dados vieram em CSV, permiti - me transforma - los cada um para parquet, individualmente devido ao volume dos dados. Os dados estavam no diretório **data/raw** em csv e terminaram no caminho **data/processed** em parquet.

### Passo 2
O segundo passo consiste em criar pares para cada produto vendido em um determinado mês. Depois fazer uma contagem de quantas vezes a venda simultânea ocorreu. Fiz isso para cada mês individualmente para meu computador pessoal conseguir processar. Com isso, salvo esses dados no caminho **data/gold**.

### Passo 3
Nessa etapa, leio os valores agregados de todos os pares de produtos de todos os meses, agrupo mais uma vez para o ano inteiro agora. Depois faço um ranking por produto de quais pares tiveram maior contagem. Por último, faço um filtro dos 5 maiores no ranking.


### Disclaimer
É um pouco difícil reproduzir isso em máquinas limitadas. Todo o processo demora mais de 4 horas.
