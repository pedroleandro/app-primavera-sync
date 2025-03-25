CREATE DATABASE app_primavera
CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE TABLE vendas_clientes (
    numeroPedido INT PRIMARY KEY,
    dataPedido DATE NOT NULL,
    dataPedidoFaturado DATE NOT NULL,
    codigoCobranca VARCHAR(10),
    codigoFilial INT,
    filial VARCHAR(100),
    codigoCliente INT,
    nomeCliente VARCHAR(255),
    nomeFantasiaCliente VARCHAR(255),
    valorFaturadoPedido DECIMAL(10, 2),
    valorDespesas DECIMAL(10, 2),
    valorLiquidoPedido DECIMAL(10, 2)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE vendas_produtos (
    numeroPedido INT,
    dataPedido DATE NOT NULL,
    codigoCliente INT,
    nomeCliente VARCHAR(255),
    codigoProduto INT,
    produto VARCHAR(255),
    quantidade INT,
    precoVenda DECIMAL(10, 2),
    subTotalPedido DECIMAL(10, 2),
    codigoFilial INT,
    filial VARCHAR(255),
    UNIQUE (numeroPedido, codigoProduto)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE backup_vendas_clientes AS SELECT * FROM vendas_clientes WHERE 1=0;
CREATE TABLE backup_vendas_produtos AS SELECT * FROM vendas_produtos WHERE 1=0;

CREATE TABLE faturamento (
    codigoFilial INT NOT NULL,
    filial VARCHAR(255) NOT NULL,
    faturamento DECIMAL(15,2) NOT NULL,
    dataInicial DATE NOT NULL,
    dataFinal DATE NOT NULL,
    UNIQUE (codigoFilial, dataInicial, dataFinal)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
