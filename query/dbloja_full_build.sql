/*****************************************************************************************
 * SCRIPT DE GERAÇÃO DE DADOS EM MASSA 
 *****************************************************************************************
 *
 * OBJETIVO:
 * Popular o schema 'db_loja' com um grande volume de dados que simula um ambiente de
 * e-commerce em produção. O objetivo não é apenas volume, mas sim realismo
 * estatístico, criando um conjunto de dados assimétrico, com "ruído" e distribuições
 * do tipo "cauda longa" (Power Law), ideal para testes de performance (queries,
 * índices) e análise de BI.
 *
 * ESTRATÉGIAS DE SIMULAÇÃO E TÉCNICAS UTILIZADAS:
 *
 * 1.  **Geração de Totais Dinâmicos e Imprevisíveis:**
 * * **Estratégia:** Evitar números "redondos" (ex: 5000, 10000). Um sistema real
 * nunca tem contagens exatas.
 * * **Técnica:** O script é encapsulado em um bloco `DO $$ ... END $$` (plpgsql).
 * Isso nos permite declarar variáveis (ex: `v_total_clientes`). No início,
 * essas variáveis recebem valores aleatórios dentro de um intervalo
 * (ex: `4800 + (random() * 400)::int`). Todos os comandos `generate_series()`
 * subsequentes usam essas variáveis, fazendo com que cada execução do
 * script produza um banco de dados com contagens ligeiramente diferentes.
 *
 * 2.  **Simulação de Curva ABC (Lei de Potência / "Power Law"):**
 * * **Estratégia:** Simular o padrão onde "poucos clientes geram a maior parte
 * da receita" e "poucos produtos representam a maior parte das vendas".
 * * **Técnica:** Em vez de uma distribuição uniforme (`random()`), usamos a
 * fórmula `TRUNC(random() * random() * (N-K) + 1)::int`. Multiplicar
 * `random()` por si mesmo "achata" a distribuição de probabilidade,
 * tornando os IDs mais baixos (ex: clientes 1-1000) exponencialmente
 * mais prováveis de serem selecionados do que os IDs altos.
 *
 * 3.  **Distribuição de Probabilidade Manual (Não-Uniforme):**
 * * **Estratégia:** Simular a realidade onde a maioria dos pedidos tem 1-2 itens
 * e a maioria dos itens é comprada em quantidade 1. Uma distribuição
 * uniforme (`random() * 5`) é irrealista.
 * * **Técnica:** Usamos "baldes" (buckets) de probabilidade com `CASE`.
 * (ex: `CASE WHEN random() < 0.45 THEN 1 ... WHEN random() < 0.75 THEN 2 ... END`).
 * Isso nos dá controle total: 45% de chance de 1 item, 30% de 2, etc.
 * Esta técnica é usada DUAS VEZES:
 * 1. Para definir o **número de itens (linhas)** em um pedido.
 * 2. Para definir a **quantidade (unidades)** de cada item individual.
 *
 * 4.  **Garantia de Entidades 'Vazias' (Exclusão de Intervalo):**
 * * **Estratégia:** Simular dados "inertes" que existem no banco mas não
 * participam das transações (clientes que se cadastraram e nunca
 * compraram, produtos de "fim de linha" que não vendem mais, etc.).
 * * **Técnica:** A lógica de geração é intencionalmente limitada:
 * * **Clientes:** O `N` na fórmula da Curva ABC é `(v_total_clientes - 2)`.
 * Isso garante que o último cliente (`v_id_cliente_sem_pedido`)
 * nunca seja selecionado.
 * * **Produtos:** O `N` é `(v_total_produtos - 100)`. Isso garante que os
 * últimos 100 produtos nunca sejam selecionados para venda.
 * * **Categorias:** Geramos `N` categorias e inserimos manualmente uma
 * `N+1` ('Categoria Vazia'). A geração de produtos só usa IDs de `1` a `N`.
 *
 * 5.  **Injeção de 'Caos' e Realismo Transacional:**
 * * **Estratégia:** Simular a variabilidade e imperfeições de dados reais.
 * * **Técnica:**
 * * **Nulos:** `CASE WHEN random() > 0.X THEN [VALOR] ELSE NULL END` é usado
 * para inserir `NULL` de forma controlada em campos não-obrigatórios
 * (ex: 30% de telefones nulos, 20% de descrições de produto nulas).
 * * **Datas:** Os pedidos são espalhados (`NOW() - (random() * 500) * '1 day'`).
 * A aleatoriedade pura garante que alguns dias terão muitas vendas e
 * outros (estatisticamente) terão zero, simulando fins de semana/feriados.
 * * **Lógica de Pedido:** Para simular um fluxo de sistema real, o
 * `pedido_cabecalho` é inserido com `valor_total = 0`. Somente após
 * `pedido_itens` ser populado, um `UPDATE` final calcula e preenche
 * o `valor_total` correto, garantindo a integridade.
 *
 *****************************************************************************************/


--
-- SEÇÃO 1: LIMPEZA DO AMBIENTE
--
-- [ETAPA 1/5] Limpando ambiente anterior (DROP SCHEMA)...
DROP TABLE IF EXISTS db_loja.pedido_itens, db_loja.pedido_cabecalho, db_loja.produto, db_loja.categoria_produto, db_loja.cliente CASCADE;
DROP SCHEMA IF EXISTS db_loja CASCADE;


--
-- SEÇÃO 2: CRIAÇÃO DO SCHEMA
--
-- [ETAPA 2/5] Criando schema db_loja...
CREATE SCHEMA IF NOT EXISTS db_loja;


--
-- SEÇÃO 3: ESTRUTURA DAS TABELAS (CREATE TABLE)
--
-- [ETAPA 3/5] Criando estrutura de tabelas (CREATE TABLE)...

-- Tabela: categorias_produtos
CREATE TABLE db_loja.categoria_produto (
    id INT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE,
    descricao TEXT
);

-- Tabela: produtos
CREATE TABLE db_loja.produto (
    id INT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    descricao TEXT,
    preco NUMERIC(10, 2) NOT NULL,
    estoque INT NOT NULL DEFAULT 0,
    id_categoria INT,
    -- Coluna Watermark: populada no INSERT (DEFAULT) e atualizada no UPDATE (TRIGGER)
    last_modified_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_categoria
        FOREIGN KEY(id_categoria)
        REFERENCES db_loja.categoria_produto(id)
);

-- Tabela: clientes
CREATE TABLE db_loja.cliente (
    id INT PRIMARY KEY,
    nome VARCHAR(150) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    telefone VARCHAR(20),
    insert_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_delete BOOLEAN DEFAULT FALSE NOT NULL
);

-- Tabela: pedido_cabecalho
CREATE TABLE db_loja.pedido_cabecalho (
    id INT PRIMARY KEY,
    id_cliente INT NOT NULL,
    data_pedido TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    valor_total NUMERIC(10, 2) NOT NULL,
    CONSTRAINT fk_cliente
        FOREIGN KEY(id_cliente)
        REFERENCES db_loja.cliente(id)
);

-- Tabela: pedido_itens
CREATE TABLE db_loja.pedido_itens (
    id BIGINT PRIMARY KEY,
    id_pedido INT NOT NULL,
    id_produto INT NOT NULL,
    quantidade INT NOT NULL,
    preco_unitario NUMERIC(10, 2) NOT NULL,
    CONSTRAINT fk_pedido
        FOREIGN KEY(id_pedido)
        REFERENCES db_loja.pedido_cabecalho(id) ON DELETE CASCADE,
    CONSTRAINT fk_produto
        FOREIGN KEY(id_produto)
        REFERENCES db_loja.produto(id)
);


--
-- SEÇÃO 4: AUTOMAÇÃO DE TIMESTAMP (FUNÇÃO E TRIGGER)
--
-- [ETAPA 4/5] Criando função e trigger da watermark (last_modified_date)...

-- *** FUNÇÃO RENOMEADA ***
CREATE OR REPLACE FUNCTION db_loja.upd_last_modified_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- Atualiza a coluna 'last_modified_date' para o horário atual
    NEW.last_modified_date = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- *** TRIGGER RENOMEADO E APONTANDO PARA A NOVA FUNÇÃO ***
CREATE TRIGGER trg_produtos_last_modified
BEFORE UPDATE ON db_loja.produto
FOR EACH ROW
EXECUTE FUNCTION db_loja.upd_last_modified_trigger();


--
-- SEÇÃO 5: GERAÇÃO DE DADOS EM MASSA (COM TOTAIS DINÂMICOS)
--
-- [ETAPA 5/5] Iniciando geração de dados em massa dinâmica...
DO $$
DECLARE
    -- Declaração das variáveis de contagem total
    v_total_categorias INT;
    v_total_clientes INT;
    v_total_produtos INT;
    v_total_pedidos INT;
    
    -- Variáveis de controle para as regras de negócio
    v_id_categoria_vazia INT;
    v_id_cliente_sem_pedido INT;
    v_range_produto_nao_vendido INT := 100;
BEGIN
    
    -- 5.0: Atribuir valores aleatórios às contagens totais
    v_total_categorias        := 45 + (random() * 10)::int; -- Entre 45 e 55
    v_total_clientes          := 4800 + (random() * 400)::int; -- Entre 4800 e 5200
    v_total_produtos          := 9500 + (random() * 1000)::int; -- Entre 9500 e 10500
    v_total_pedidos           := 98000 + (random() * 4000)::int; -- Entre 98k e 102k
    
    -- Definir IDs de controle com base nos totais dinâmicos
    v_id_categoria_vazia    := v_total_categorias + 1;
    v_id_cliente_sem_pedido := v_total_clientes;

    RAISE NOTICE '[ETAPA 5/5] Iniciando geração de dados em massa dinâmica...';
    RAISE NOTICE '... Script irá gerar:';
    RAISE NOTICE '...   % categorias (+1 vazia)', v_total_categorias;
    RAISE NOTICE '...   % clientes (último sem pedido)', v_total_clientes;
    RAISE NOTICE '...   % produtos (últimos 100 não vendidos)', v_total_produtos;
    RAISE NOTICE '...   % pedidos', v_total_pedidos;

    ---
    --- 5.1. CATEGORIAS (Dinâmico)
    ---
    RAISE NOTICE '... Gerando categorias...';
    INSERT INTO db_loja.categoria_produto (id, nome, descricao)
    SELECT
        i AS id,
        'Categoria ' || i AS nome,
        'Descrição detalhada da Categoria ' || i AS descricao
    FROM generate_series(1, v_total_categorias) s(i);

    INSERT INTO db_loja.categoria_produto (id, nome, descricao)
    VALUES (v_id_categoria_vazia, 'Categoria Vazia', 'Esta categoria não terá produtos associados.');

    ---
    --- 5.2. CLIENTES (Dinâmico)
    ---
    RAISE NOTICE '... Gerando clientes...';
    INSERT INTO db_loja.cliente (id, nome, email, telefone, insert_date)
    SELECT
        i AS id,
        'Cliente ' || i AS nome,
        'cliente.' || i || '@example.com' AS email,
        CASE
            WHEN random() > 0.3 THEN '(11) 9' || LPAD((random()*89999999 + 10000000)::int::text, 8, '0')
            ELSE NULL
        END AS telefone,
        NOW() - (random() * 730) * '1 day'::interval AS insert_date
    FROM generate_series(1, v_total_clientes) s(i);

    ---
    --- 5.3. PRODUTOS (Dinâmico)
    ---
    RAISE NOTICE '... Gerando produtos...';
    -- O INSERT omite 'last_modified_date', permitindo o DEFAULT
    INSERT INTO db_loja.produto (id, nome, descricao, preco, estoque, id_categoria)
    SELECT
        i AS id,
        'Produto ' || i AS nome,
        CASE
            WHEN random() > 0.2 THEN 'Descrição longa e detalhada do Produto ' || i
            ELSE NULL
        END AS descricao,
        TRUNC((random() * 1990 + 9.99)::numeric, 2) AS preco,
        (random() * 500)::int AS estoque,
        (random() * (v_total_categorias - 1) + 1)::int AS id_categoria
    FROM generate_series(1, v_total_produtos) s(i);

    ---
    --- 5.4. PEDIDOS (CABEÇALHO) (Dinâmico)
    ---
    RAISE NOTICE '... Gerando cabeçalhos de pedido...';
    INSERT INTO db_loja.pedido_cabecalho (id, id_cliente, data_pedido, valor_total)
    SELECT
        i AS id,
        TRUNC(random() * random() * (v_id_cliente_sem_pedido - 2) + 1)::int AS id_cliente,
        date_trunc('day', NOW() - (random() * 500) * '1 day'::interval) AS data_pedido,
        0 AS valor_total
    FROM generate_series(1, v_total_pedidos) s(i);

    ---
    --- 5.5. ITENS DE PEDIDO (Lógica de Distribuição Realista e Dinâmica)
    ---
    RAISE NOTICE '... Gerando itens de pedido (distribuição realista)...';

    WITH PedidosComQtde AS (
        SELECT
            id AS id_pedido,
            (CASE
                WHEN r.val < 0.45 THEN 1
                WHEN r.val < 0.75 THEN 2
                WHEN r.val < 0.88 THEN 3
                WHEN r.val < 0.95 THEN 4
                WHEN r.val < 0.99 THEN 5
                ELSE (random() * 5 + 6)::int
            END) AS num_itens_neste_pedido
        FROM db_loja.pedido_cabecalho
        CROSS JOIN LATERAL (SELECT random() AS val) r
    ),
    ItensGerados AS (
        SELECT
            p.id_pedido,
            TRUNC(random() * random() * (v_total_produtos - v_range_produto_nao_vendido - 1) + 1)::int AS id_produto,
            (CASE
                WHEN r_qtde.val < 0.80 THEN 1
                WHEN r_qtde.val < 0.95 THEN 2
                WHEN r_qtde.val < 0.99 THEN 3
                ELSE (random() * 2 + 4)::int
            END) AS quantidade
        FROM
            PedidosComQtde p
        CROSS JOIN LATERAL
            generate_series(1, p.num_itens_neste_pedido) s(num_item)
        CROSS JOIN LATERAL (SELECT random() AS val) r_qtde
    )
    INSERT INTO db_loja.pedido_itens (id, id_pedido, id_produto, quantidade, preco_unitario)
    SELECT
        row_number() OVER () AS id,
        ig.id_pedido,
        ig.id_produto,
        ig.quantidade,
        prod. preco AS preco_unitario
    FROM
        ItensGerados ig
    JOIN
        db_loja.produto prod ON ig.id_produto = prod.id;

    ---
    --- 5.6. ATUALIZAÇÃO DO VALOR TOTAL DOS PEDIDOS
    ---
    RAISE NOTICE '... Atualizando o valor total dos cabeçalhos de pedido...';

    WITH Totais AS (
        SELECT
            id_pedido,
            SUM(quantidade * preco_unitario) AS total_calculado
        FROM
            db_loja.pedido_itens
        GROUP BY
            id_pedido
    )
    UPDATE db_loja.pedido_cabecalho pc
    SET
        valor_total = t.total_calculado
    FROM
        Totais t
    WHERE
        pc.id = t.id_pedido;

    RAISE NOTICE '*** GERAÇÃO DE DADOS EM MASSA DINÂMICA CONCLUÍDA! ***';

END $$ LANGUAGE plpgsql;