# Delta Lake House - PDE

Repositório criado para o projeto de **Processamento de Dados em Escala (PDE)** desenvolvido na Universidade Federal de São Carlos. O objetivo do projeto foi implementar e comparar diferentes arquiteturas de processamento e armazenamento de dados em grande escala, com foco na arquitetura **Lakehouse** usando o modelo de **Arquitetura Medalhão**.

## Estrutura do Projeto

O projeto foi dividido em três fases principais, com cada fase representando um nível da **Arquitetura Medalhão**:

1. **Bronze**: Armazenamento de dados brutos e preparação para processamento inicial.
2. **Silver**: Limpeza e integração dos dados, preparando-os para análises mais complexas.
3. **Gold**: Agregação e processamento de dados para análises e aplicações específicas, como BI e aprendizado de máquina.

### Tecnologias Utilizadas

As principais tecnologias empregadas para garantir o processamento eficiente e armazenamento em larga escala foram:

- **Apache Spark**: Processamento paralelo de dados em memória para análises de alto desempenho.
- **Delta Lake**: Criação de tabelas otimizadas para leitura e manipulação de grandes volumes de dados.
- **Python**: Linguagem principal para a implementação dos scripts e manipulação dos dados.
- **Data Lakehouse Model**: Estrutura híbrida de armazenamento que combina as vantagens de Data Lakes e Data Warehouses.

### Organização do Código

O repositório contém scripts organizados por nível de processamento:

- **`bronze.py`**: Processa e armazena os dados brutos em formato Delta Lake.
- **`silver.py`**: Limpa, integra e organiza os dados para análises intermediárias.
- **`gold.py`**: Realiza agregações finais para relatórios e modelos analíticos avançados.

Além disso, o arquivo `config.py` configura o ambiente Spark, utilizando um setup em modo pseudo-distribuído.

### Base de Dados Utilizada

A base de dados utilizada foi a **Vehicle Energy Dataset**, que contém cerca de 2.7 GB de dados sobre o consumo de energia e combustível de veículos. Os dados estão divididos em dados dinâmicos (leituras de sensores dos veículos) e dados estáticos (características dos veículos).

## Resultados

A implementação da Arquitetura Medalhão mostrou-se eficaz no tratamento e preparação dos dados para análises avançadas, permitindo flexibilidade e escalabilidade. O uso das camadas Bronze, Silver e Gold facilita o armazenamento, processamento e análise de dados de forma otimizada.

## Colaboradores

- Ana Ellen Deodato Pereira da Silva
- Augusto dos Santos Gomes Vaz
- Sara Ferreira Bento da Silva
- Vinicius Gonçalves Perillo
