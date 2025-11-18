# -*- coding: utf-8 -*-
"""
Script de teste de conexão com o banco de dados PostgreSQL.

Este script utiliza a biblioteca psycopg2 para:
1. Estabelecer uma conexão com o banco de dados PostgreSQL em execução no container 'db'.
2. Executar uma consulta SQL simples para obter a versão do PostgreSQL.
3. Imprimir a versão do banco de dados para confirmar que a conexão foi bem-sucedida.
4. Fechar a conexão de forma segura.
"""

# 1. Importação da Biblioteca
# Importamos a biblioteca psycopg2, que é o driver que permite a comunicação
# do Python com o PostgreSQL.
import psycopg2

def main():
    """Função principal que executa o teste de conexão."""

    print("Tentando se conectar ao banco de dados PostgreSQL...")

    # 2. Definição dos Parâmetros de Conexão
    # Estes valores DEVEM ser os mesmos que definimos na seção 'environment'
    # do serviço 'db' no seu arquivo 'docker-compose.yml'.
    #
    # IMPORTANTE: O 'host' é 'db'. Este é o nome do serviço do PostgreSQL
    # no docker-compose.yml. O Docker Compose cria uma rede interna onde
    # os serviços podem se encontrar usando seus nomes como se fossem um DNS.
    # Não use 'localhost' ou '127.0.0.1' aqui!
    conn_params = {
        "host": "db",
        "database": "mydb",
        "user": "myuser",
        "password": "mypassword"
    }

    conn = None  # Inicializa a variável de conexão como None

    try:
        # 3. Estabelecendo a Conexão
        # Usamos um bloco try...except para capturar qualquer erro que possa ocorrer
        # durante a tentativa de conexão (ex: senha errada, host inacessível).
        conn = psycopg2.connect(**conn_params)

        # 4. Criando um Cursor
        # O cursor é um objeto que permite executar comandos SQL no banco de dados.
        cur = conn.cursor()

        # 5. Executando uma Consulta de Teste
        # A consulta 'SELECT version()' é uma forma segura e padrão de verificar
        # se a conexão está ativa e funcionando. Ela simplesmente pede ao banco
        # para retornar sua própria versão.
        print("Conexão estabelecida com sucesso! Verificando a versão do PostgreSQL...")
        cur.execute('SELECT version()')

        # 6. Buscando o Resultado
        # .fetchone() recupera a próxima linha do resultado da consulta.
        # Como nossa consulta retorna apenas uma linha, é o suficiente.
        db_version = cur.fetchone()
        print("\n--- Versão do PostgreSQL ---")
        print(db_version[0])

        # 7. Fechando o Cursor
        # É uma boa prática fechar o cursor quando você termina de usá-lo.
        cur.close()
        
        print("\nTeste de conexão com o PostgreSQL concluído com sucesso!")

    except psycopg2.Error as e:
        # Se qualquer erro relacionado ao psycopg2 ocorrer no bloco 'try',
        # ele será capturado aqui e uma mensagem de erro será exibida.
        print("\nOcorreu um erro ao conectar ou interagir com o PostgreSQL:", e)

    finally:
        # 8. Fechando a Conexão
        # O bloco 'finally' é executado SEMPRE, tenha o 'try' funcionado ou não.
        # Isso garante que a conexão com o banco de dados seja sempre fechada,
        # evitando deixar conexões abertas desnecessariamente.
        if conn is not None:
            conn.close()
            print("\nConexão com o banco de dados foi fechada.")

if __name__ == '__main__':
    main()
