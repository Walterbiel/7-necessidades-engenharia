# Guia de Estudos: Engenharia de Dados

Um roteiro objetivo do que estudar para atuar com Engenharia de Dados, organizado em 8 etapas. A ordem importa: cada etapa usa o que foi construído na anterior.

## Sumário

1. [Fundamentos da Engenharia de Dados](#1-fundamentos-da-engenharia-de-dados)
2. [Armazenamento e Bancos de Dados](#2-armazenamento-e-bancos-de-dados)
3. [Ingestão, Pipelines e Orquestração](#3-ingestão-pipelines-e-orquestração)
4. [Arquitetura de Dados](#4-arquitetura-de-dados)
5. [Qualidade e Governança](#5-qualidade-e-governança)
6. [Performance, Custo e Trade-off](#6-performance-custo-e-trade-off)
7. [APIs](#7-apis)
8. [CI/CD, Ambientes e DataOps](#8-cicd-ambientes-e-dataops)
9. [Como estudar este roteiro](#como-estudar-este-roteiro)
10. [Próximo passo](#próximo-passo)

---

## 1. Fundamentos da Engenharia de Dados

Entenda o papel do engenheiro de dados: garantir que o dado certo chegue ao lugar certo, no formato certo e no tempo certo.

**O que estudar**
- Ciclo de vida do dado: geração, ingestão, armazenamento, transformação e consumo
- Diferença entre engenheiro, analista e cientista de dados
- Python e SQL como ferramentas base

**Exemplo 1: ciclo de vida na prática**

Uma venda registrada no sistema da loja (geração) é copiada todo dia para o lake (ingestão), tratada (transformação) e vira um indicador no dashboard (consumo).

**Exemplo 2: quem faz o quê**

| Papel              | Pergunta que responde                         | Entrega típica             |
|--------------------|-----------------------------------------------|----------------------------|
| Engenheiro de dados| O dado está disponível, confiável e no prazo? | Pipelines e tabelas tratadas |
| Analista de dados  | O que aconteceu e por quê?                    | Dashboards e relatórios    |
| Cientista de dados | O que vai acontecer?                          | Modelos preditivos         |

**Exemplo 3: Python e SQL resolvendo o mesmo problema**

```sql
SELECT uf, SUM(valor) AS total
FROM vendas
GROUP BY uf;
```

```python
import pandas as pd

df = pd.read_csv("vendas.csv")
total = df.groupby("uf")["valor"].sum()
```

## 2. Armazenamento e Bancos de Dados

Saber onde e como o dado fica guardado define tudo o que vem depois.

**O que estudar**
- Bancos relacionais, CRUD e propriedades ACID
- Bancos NoSQL (documento, chave-valor, colunar, grafo)
- Data Lake e formatos de arquivo (CSV, JSON, Parquet, Avro)
- OLTP vs OLAP e as formas normais

**Exemplo 1: CRUD em SQL**

```sql
INSERT INTO clientes (id, nome, uf) VALUES (1, 'Ana', 'BA');   -- Create
SELECT * FROM clientes WHERE id = 1;                           -- Read
UPDATE clientes SET uf = 'SP' WHERE id = 1;                    -- Update
DELETE FROM clientes WHERE id = 1;                             -- Delete
```

**Exemplo 2: transação ACID**

Garante que uma transferência não debite sem creditar.

```sql
BEGIN TRANSACTION;
UPDATE contas SET saldo = saldo - 100 WHERE id = 1;
UPDATE contas SET saldo = saldo + 100 WHERE id = 2;
COMMIT;
```

**Exemplo 3: o mesmo dado em relacional e em documento (NoSQL)**

```json
{
  "id_pedido": 501,
  "cliente": { "id": 1, "nome": "Ana" },
  "itens": [
    { "produto": "Notebook", "qtd": 1 },
    { "produto": "Mouse", "qtd": 2 }
  ]
}
```

No relacional, isso viraria três tabelas (pedidos, clientes e itens) ligadas por chaves.

**Exemplo 4: escolha do formato**

| Formato | Quando usar                                         |
|---------|-----------------------------------------------------|
| CSV     | Troca simples de arquivos, volumes pequenos         |
| JSON    | Dados semiestruturados, retorno de APIs             |
| Parquet | Análise em grande volume, leitura por colunas       |
| Avro    | Streaming e evolução de schema                      |

## 3. Ingestão, Pipelines e Orquestração

Mover dados com confiabilidade, no momento certo e com dependências controladas.

**O que estudar**
- Pipelines batch: ETL vs ELT, carga full vs incremental
- Orquestração com Airflow (DAGs, agendamento, retries)
- Streaming e tempo real (Kafka, eventos)
- Arquitetura Lambda vs Kappa

**Exemplo 1: carga incremental**

Lê só o que mudou desde a última execução.

```sql
SELECT * FROM pedidos
WHERE data_atualizacao > :ultima_execucao;
```

**Exemplo 2: DAG simples no Airflow**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extrair(): print("extraindo")
def transformar(): print("transformando")
def carregar(): print("carregando")

with DAG("pipeline_vendas", start_date=datetime(2026, 1, 1),
         schedule="@daily", catchup=False) as dag:
    t1 = PythonOperator(task_id="extrair", python_callable=extrair)
    t2 = PythonOperator(task_id="transformar", python_callable=transformar)
    t3 = PythonOperator(task_id="carregar", python_callable=carregar)

    t1 >> t2 >> t3
```

**Exemplo 3: batch ou streaming?**

| Cenário                                   | Abordagem  |
|-------------------------------------------|------------|
| Relatório gerencial atualizado toda manhã | Batch      |
| Detecção de fraude em cartão              | Streaming  |
| Fechamento contábil mensal                | Batch      |
| Rastreamento de entregas em tempo real    | Streaming  |

## 4. Arquitetura de Dados

Como as peças se encaixam para atender o negócio com escala e confiabilidade.

**O que estudar**
- Data Warehouse e modelagem dimensional (fato e dimensão)
- Delta Lake e Data Lakehouse
- Arquitetura medalhão (Bronze, Silver, Gold)
- Backup e replicação
- SCD Tipo I e Tipo II

**Exemplo 1: modelo estrela**

```
            dim_cliente
                 |
dim_produto -- fato_vendas -- dim_tempo
                 |
             dim_loja
```

A fato guarda os eventos e as métricas (quantidade, valor). As dimensões guardam o contexto (quem, o quê, quando, onde).

**Exemplo 2: camadas medalhão**

| Camada | Conteúdo                                  | Exemplo                          |
|--------|-------------------------------------------|----------------------------------|
| Bronze | Dado bruto, como veio da origem           | JSON da API sem tratamento       |
| Silver | Dado limpo, tipado e sem duplicidade      | Tabela de vendas padronizada     |
| Gold   | Dado agregado e pronto para o negócio     | Faturamento mensal por região    |

**Exemplo 3: MERGE no Delta Lake (upsert)**

```sql
MERGE INTO silver.clientes AS destino
USING bronze.clientes_novos AS origem
ON destino.id = origem.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Exemplo 4: SCD Tipo II**

Preserva o histórico quando um cliente muda de cidade.

| id_cliente | cidade    | inicio     | fim        | atual |
|------------|-----------|------------|------------|-------|
| 10         | Salvador  | 2024-01-01 | 2026-03-31 | N     |
| 10         | São Paulo | 2026-04-01 | NULL       | S     |

No SCD Tipo I, a cidade seria apenas sobrescrita e o histórico se perderia.

## 5. Qualidade e Governança

Dado sem confiança não gera decisão. Aqui você aprende a garantir e comprovar a qualidade.

**O que estudar**
- Metadados e dicionário de dados
- Validações e constraints em SQL
- Validação de schema com Pydantic
- Catálogo de dados e linhagem
- Monitoramento de pipelines e alertas

**Exemplo 1: constraints no banco**

```sql
CREATE TABLE vendas (
    id_venda   INT PRIMARY KEY,
    valor      DECIMAL(10,2) NOT NULL CHECK (valor > 0),
    uf         CHAR(2) NOT NULL,
    id_cliente INT REFERENCES clientes(id)
);
```

**Exemplo 2: validação com Pydantic**

```python
from pydantic import BaseModel, PositiveFloat

class Venda(BaseModel):
    id_venda: int
    valor: PositiveFloat
    uf: str

Venda(id_venda=1, valor=-50, uf="BA")  # gera erro de validação
```

**Exemplo 3: checagens rápidas de qualidade em SQL**

```sql
-- Duplicidade
SELECT id_venda, COUNT(*) FROM vendas GROUP BY id_venda HAVING COUNT(*) > 1;

-- Nulos em coluna obrigatória
SELECT COUNT(*) FROM vendas WHERE uf IS NULL;

-- Dado atrasado
SELECT MAX(data_venda) FROM vendas;
```

**Exemplo 4: linhagem**

```
API de pedidos -> bronze.pedidos -> silver.pedidos -> gold.faturamento_mensal -> Dashboard
```

Se o número do dashboard estiver errado, a linhagem mostra exatamente por onde investigar.

## 6. Performance, Custo e Trade-off

Toda escolha técnica tem custo. O bom engenheiro sabe justificar a sua.

**O que estudar**
- Particionamento (por data, região etc.)
- Indexação e quando ela atrapalha
- Compressão e formatos colunares
- Teorema CAP: consistência, disponibilidade e tolerância a partição

**Exemplo 1: particionamento**

A consulta lê só a pasta do período filtrado, e não a tabela inteira.

```python
df.write.partitionBy("ano", "mes").parquet("/lake/vendas")
```

```
/lake/vendas/ano=2026/mes=10/
/lake/vendas/ano=2026/mes=11/
```

**Exemplo 2: índice**

```sql
CREATE INDEX idx_vendas_cliente ON vendas (id_cliente);
```

Acelera buscas por cliente, mas deixa inserts e updates mais lentos. Em tabelas com muita escrita, avalie antes de criar.

**Exemplo 3: compressão e formato**

| Arquivo (mesmos dados)  | Tamanho aproximado |
|-------------------------|--------------------|
| CSV                     | 1 GB               |
| Parquet + Snappy        | 150 a 250 MB       |

Menos espaço em disco significa menos custo de armazenamento e leitura mais rápida.

**Exemplo 4: Teorema CAP no dia a dia**

| Sistema                      | Prioriza                    |
|------------------------------|-----------------------------|
| Saldo bancário               | Consistência (CP)           |
| Contador de curtidas         | Disponibilidade (AP)        |

## 7. APIs

Grande parte dos dados chega e sai via API. Saber consumir e expor é essencial.

**O que estudar**
- Conceitos de REST, status HTTP e JSON
- Métodos GET, POST, PUT, DELETE
- Coleta de dados com paginação
- Criação de APIs com FastAPI
- Autenticação (API Key, OAuth, JWT)

**Exemplo 1: coleta com paginação**

```python
import requests

dados, pagina = [], 1
while True:
    resp = requests.get("https://api.exemplo.com/pedidos",
                        params={"page": pagina}, timeout=30)
    resp.raise_for_status()
    lote = resp.json()["items"]
    if not lote:
        break
    dados.extend(lote)
    pagina += 1
```

**Exemplo 2: métodos HTTP**

| Método | Ação                  | Exemplo                     |
|--------|-----------------------|-----------------------------|
| GET    | Consultar             | `GET /clientes/10`          |
| POST   | Criar                 | `POST /clientes`            |
| PUT    | Atualizar por inteiro | `PUT /clientes/10`          |
| DELETE | Remover               | `DELETE /clientes/10`       |

**Exemplo 3: API com FastAPI**

```python
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Venda(BaseModel):
    id_venda: int
    valor: float

@app.get("/vendas/{id_venda}")
def buscar_venda(id_venda: int):
    return {"id_venda": id_venda, "status": "ok"}

@app.post("/vendas")
def criar_venda(venda: Venda):
    return {"mensagem": "venda criada", "dados": venda}
```

**Exemplo 4: autenticação por token**

```python
headers = {"Authorization": "Bearer SEU_TOKEN"}
requests.get("https://api.exemplo.com/pedidos", headers=headers, timeout=30)
```

## 8. CI/CD, Ambientes e DataOps

Levar código para produção com segurança, versionamento e automação.

**O que estudar**
- Git e GitHub: commits, branches e pull requests
- Ambientes separados: dev, homologação e produção
- GitHub Actions para testes e deploy automático
- Princípios de DataOps

**Exemplo 1: fluxo com branch e pull request**

```bash
git checkout -b feature/nova-carga
git add .
git commit -m "Adiciona carga incremental de pedidos"
git push origin feature/nova-carga
# abrir pull request para a branch dev
```

**Exemplo 2: separação de ambientes**

| Ambiente     | Finalidade                         | Quem acessa         |
|--------------|------------------------------------|---------------------|
| Dev          | Desenvolver e testar livremente    | Time de dados       |
| Homologação  | Validar com dados próximos do real | Time e negócio      |
| Produção     | Atender o usuário final            | Somente via deploy  |

**Exemplo 3: testes automáticos a cada pull request**

```yaml
name: testes
on: [pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: pytest
```

**Exemplo 4: teste de transformação com pytest**

```python
def calcular_total(qtd, preco):
    return qtd * preco

def test_calcular_total():
    assert calcular_total(2, 10.0) == 20.0
```

---

## Como estudar este roteiro

- Siga a ordem dos módulos, sem pular fundamentos
- Para cada tema, reproduza os exemplos e depois adapte para um caso seu
- Publique seus projetos no GitHub, com README explicando o problema e a solução
- Ao final, junte tudo em um projeto completo: ingestão via API, lake em camadas, validação de qualidade, orquestração e entrega em dashboard ou API

---

## Próximo passo

Este guia mostra **o que** estudar. Se você quer aprender **como aplicar** isso no dia a dia, com cursos completos, projetos reais, lives técnicas e mentoria de carreira, conheça o **QG da Engenharia de Dados**.

No QG você encontra o Manual da Engenharia de Dados, Blueprints de Azure, Spark e Delta Lake, projetos práticos e uma comunidade de profissionais que já estão aplicando dados na prática.

**Acesse:** [qg-engenharia-de-dados.com.br](https://qg-engenharia-de-dados.com.br)
