# -*- coding: utf-8 -*-
"""
Script de teste e demonstração para a biblioteca Pandas.

Este script executa os seguintes passos:
1. Cria um DataFrame do Pandas a partir de um dicionário Python.
2. Exibe informações básicas e estatísticas sobre o DataFrame.
3. Demonstra como selecionar, filtrar e adicionar novas colunas.
4. Realiza uma operação de agrupamento (group by) e agregação.
"""

# 1. Importação da Biblioteca
# Importamos a biblioteca pandas e, por convenção da comunidade Python,
# damos a ela o apelido (alias) de 'pd'. Você verá 'pd' em quase todos
# os códigos que usam Pandas.
import pandas as pd

def main():
    """Função principal que executa a demonstração do Pandas."""
    
    print("Iniciando o teste com a biblioteca Pandas...")

    # 2. Criação do DataFrame
    # Um DataFrame é a estrutura de dados principal do Pandas, similar a uma
    # planilha ou uma tabela de banco de dados, com linhas e colunas nomeadas.
    # Aqui, criamos um a partir de um dicionário, onde as chaves são os nomes
    # das colunas e os valores são as listas de dados para cada coluna.
    dados = {
        'Produto': ['Café', 'Açúcar', 'Leite', 'Pão', 'Manteiga', 'Suco'],
        'Categoria': ['Bebida', 'Mercearia', 'Laticínio', 'Padaria', 'Laticínio', 'Bebida'],
        'Preco': [15.50, 4.20, 5.80, 7.00, 12.00, 9.90],
        'Estoque': [150, 300, 200, 90, 120, 80]
    }
    df = pd.DataFrame(dados)

    # 3. Inspeção Básica do DataFrame
    print("\n--- DataFrame Original ---")
    # Imprimir o DataFrame diretamente mostra seu conteúdo de forma tabular.
    print(df)

    print("\n--- Informações do DataFrame (.info()) ---")
    # O método .info() é ótimo para uma visão geral: mostra o número de linhas,
    # as colunas, a contagem de valores não-nulos e o tipo de dado (Dtype) de cada coluna.
    df.info()

    print("\n--- Estatísticas Descritivas (.describe()) ---")
    # O método .describe() calcula estatísticas básicas (contagem, média, desvio padrão, etc.)
    # para todas as colunas numéricas.
    print(df.describe())

    # 4. Manipulação de Dados
    
    # Selecionando uma única coluna (retorna uma Series do Pandas)
    print("\n--- Selecionando a coluna 'Produto' ---")
    produtos = df['Produto']
    print(produtos)

    # Filtrando linhas com base em uma condição
    # Aqui, selecionamos apenas os produtos da categoria 'Laticínio'.
    print("\n--- Filtrando apenas produtos da categoria 'Laticínio' ---")
    laticinios = df[df['Categoria'] == 'Laticínio']
    print(laticinios)

    # Criando uma nova coluna
    # Podemos criar novas colunas a partir de operações em colunas existentes.
    # A operação é "vetorizada", ou seja, é aplicada a todos os valores da coluna de uma só vez.
    print("\n--- Adicionando uma nova coluna 'Valor_Total_Estoque' ---")
    df['Valor_Total_Estoque'] = df['Preco'] * df['Estoque']
    print(df)

    # 5. Agrupamento e Agregação (Group By)
    # Esta é uma das funcionalidades mais poderosas do Pandas.
    # Agrupamos o DataFrame pela coluna 'Categoria' e depois calculamos o preço
    # médio para cada categoria.
    print("\n--- Preço médio por Categoria (.groupby()) ---")
    preco_medio_por_categoria = df.groupby('Categoria')['Preco'].mean().reset_index()
    print(preco_medio_por_categoria)

    print("\n\nTeste do Pandas concluído com sucesso!")


if __name__ == '__main__':
    main()
