# Pipeline de Pagamentos em Tempo Real de Ponta a Ponta com Confluent Cloud

> **Desafio de Projeto DIO / Confluent**: Implementação de um pipeline de missão crítica para detecção de fraudes em pagamentos em tempo real, integrando banco relacional, streaming distribuído, contratos de governança, processamento contínuo de eventos e observabilidade operacional.

## 📌 Visão Geral da Arquitetura

O pipeline conecta de forma nativa a camada operacional de banco de dados à camada de reação em tempo real, mitigando o problema do *dual-write* e eliminando discrepâncias lógicas entre consumidores:

```text
[PostgreSQL / Neon]
       │ (WAL / CDC com Debezium)
       ▼
[Confluent Kafka Connect (Postgres CdcSourceV2)]
       │ (Serialização Avro com Confluent Schema Registry)
       ▼
[Tópicos Kafka no Confluent Cloud (desafio-*)]
       │
       ▼
[Confluent Cloud for Apache Flink (Flink SQL)]
  ├── Temporal Join (Transações + Contas/Clientes)
  └── MATCH_RECOGNIZE (Regra de Fraude por Velocidade)
       │
       ▼
[Tópico de Alertas (desafio.fraud.detected)]
       │
       ▼
[Consumidor Final / Antifraude (Idempotente)]
```

## 🛠️ Tecnologias e Decisões de Design

| Componente | Tecnologia Escolhida | Justificativa Arquitetural |
| :--- | :--- | :--- |
| **Banco Operacional** | PostgreSQL (Neon Serverless) | Banco relacional com `REPLICA IDENTITY FULL` e suporte a Logical Decoding (WAL). |
| **Ingestão CDC** | Postgres CdcSourceV2 (Debezium) | Captura contínua e assíncrona do WAL sem onerar a aplicação com polling nem arriscar inconsistências de *dual-write*. |
| **Streaming Platform** | Confluent Cloud (Cluster Basic) | Plataforma totalmente gerenciada em `aws/us-east-1`, desacoplando infraestrutura de broker da operação de produto de dados. |
| **Contratos de Dados** | Confluent Schema Registry (Avro) | Governança estrita de schemas com compatibilidade `BACKWARD`, tipagem monetária precisa (`decimal(15,2)`) e tags de privacidade (PII/PCI). |
| **Processamento Contínuo** | Apache Flink (Flink SQL) | Motor stateful com suporte a *event-time*, *watermarks*, *temporal joins* e *Complex Event Processing* (`MATCH_RECOGNIZE`) sem infraestrutura para gerenciar. |
| **Segurança & IAM** | Confluent Cloud RBAC & ACLs | Aplicação do princípio de menor privilégio através de Service Accounts dedicadas para escrita e leitura. |

## 🚀 Como Executar o Pipeline

### Pré-requisitos

* CLI da Confluent Cloud instalada e autenticada (`confluent login`).

* Banco de dados PostgreSQL (ex.: Neon) acessível com extensão lógica habilitada.

* Utilitários `psql` e `jq` instalados no ambiente local.

### 1. Configuração de Variáveis de Ambiente

Copie o modelo de variáveis e preencha as credenciais do seu ambiente Confluent e PostgreSQL:

```bash
cp .env.example .env
# Edite o arquivo .env com seus endpoints, senhas e API keys reais
```

### 2. Inicialização e Carga do Banco

Execute os scripts SQL na sua instância do Postgres para criar a estrutura e carregar os dados determinísticos (`seed = 42`):

```bash
psql "$POSTGRES_CONNECTION_STRING" -f database/01_init.sql
psql "$POSTGRES_CONNECTION_STRING" -f database/02_seed.sql
```

### 3. Provisionamento do Pipeline Ponta a Ponta

Execute o script de automação para provisionar o ambiente, tópicos, contratos, conectores e queries do Flink:

```bash
chmod +x setup.sh teardown.sh
./setup.sh
```

### 4. Encerramento e Teardown (Custo Zero)

Ao finalizar os testes e coleta de evidências, execute o script de destruição para remover todos os conectores, statements e recursos provisionados, evitando cobranças adicionais:
```bash
./teardown.sh
```

## 🔬 Detalhamento das Camadas e Evidências

### Camada 1: Fundação (Ambiente, Cluster e IAM)

* **Ambiente:** `desafio-final`

* **Cluster:** `desafio-basic` (Provedor: `AWS`, Região: `us-east-1`, Tipo: Basic com capacidade elástica em eCKU).

* **Identidades de Aplicação (Service Accounts):**

  * `desafio-producer`: Permissão estrita de escrita (`WRITE`) nos tópicos de prefixo `desafio-*`.

  * `desafio-consumer`: Permissão de leitura (`READ`) nos tópicos `desafio-*` e gerenciamento do Consumer Group `desafio-*`.

  * Conta pessoal utilizada exclusivamente para operações administrativas de `setup.sh` e `teardown.sh`.

#### Evidência 1.1 — Service Accounts e ACLs Provisionadas

```text
<sua evidência aqui: confluent iam service-account list + acl list por SA>
```

### Camada 2: Contratos de Dados (Schema Registry)

* **Namespace:** `com.bootcamp.payments.*`

* **Definições de Schemas:** Arquivos `.avsc` em `schemas/` cobrindo `customers`, `accounts`, `cards`, `merchants`, `transactions` e `fraud-alert`.

* **Precisão Numérica:** Campos monetários tipados rigorosamente com tipo lógico Avro `decimal(15, 2)` sobre `bytes`, impedindo imprecisões e desvios de arredondamento em conciliações contábeis.

* **Governança & Privacidade:** Anotações e tags de conformidade para dados sensíveis (`PII` para nomes/documentos e `PCI` para cartões).

* **Regra de Evolução:** Modo `BACKWARD` (o padrão seguro onde consumidores novos leem mensagens antigas com valores padrão).

#### Evidência 2.1 — Evolução BACKWARD: Adição com Default (Aprovada)

```text
<sua evidência aqui: test-compatibility aceitando campo novo com default>
```

#### Evidência 2.2 — Evolução BACKWARD: Remoção de Campo Obrigatório (Rejeitada)
```text
<sua evidência aqui: test-compatibility REJEITANDO remoção de campo obrigatório>
```

### Camada 3: Ingestão de Dados (CDC com Debezium)

* **Conector:** `Postgres CdcSourceV2` gerenciado pelo Confluent Cloud.

* **Usuário do Banco:** `cdc_user` configurado sob o princípio de menor privilégio.

* **Configuração:** `heartbeat.interval.ms = 60000` para evitar avanço desnecessário e retenção acumulada de WAL no replication slot do Postgres. Credenciais injetadas via variáveis de ambiente em `connectors/cdc.json`.

* **Carga Determinística (`seed = 42`):**

  * Clientes (`customers`): 200

  * Contas (`accounts`): 240

  * Cartões (`cards`): 320

  * Estabelecimentos (`merchants`): 60

  * Transações (`transactions`): 2.000

#### Evidência 3.1 — Envelopes CDC Capturados (`evidencias/01-envelopes-cdc.json`)

Demonstração das três operações transacionais fundamentais extraídas do WAL:

* **INSERT (`op = c`):** Bloco `before: null` e registro inserido presente no bloco `after`.

* **UPDATE (`op = u`):** Ambos os blocos `before` e `after` populados graças ao `REPLICA IDENTITY FULL`.

* **DELETE (`op = d`):** Bloco `after: null` contendo os dados removidos em `before`, seguido de emissão de mensagem de *tombstone*.

```json
<sua evidência aqui: trechos dos 3 envelopes JSON extraídos de evidencias/01-envelopes-cdc.json>
```

### Camada 4: Processamento Contínuo (Flink SQL)

#### 4.1 Enriquecimento via Temporal Join

As transações brutas de pagamento são cruzadas em tempo de evento com a dimensão temporal de contas/clientes para incorporar contexto cadastral:

* Junção baseada em chaves primárias de ambos os lados.

* Definição de `sql.tables.scan.idle-timeout = 5s` para assegurar o avanço regular de *watermarks* mesmo durante flutuações ou desbalanço de tráfego entre partições.

#### 4.2 Detecção de Fraude de Velocidade (`RULE_VELOCITY`)

Identificação contínua de comportamento anômalo: disparo de alerta caso ocorram **3 ou mais transações consecutivas no mesmo cartão em uma janela inferior a 60 segundos**:

```sql
SELECT *
FROM transactions_enriched
MATCH_RECOGNIZE (
    PARTITION BY card_id
    ORDER BY `$rowtime`
    MEASURES
        FIRST(A.`$rowtime`) AS start_time,
        LAST(A.`$rowtime`) AS end_time,
        COUNT(A.transaction_id) AS tx_count
    ONE ROW PER MATCH
    PATTERN (A{3,}) WITHIN INTERVAL '60' SECOND
    DEFINE
        A AS TRUE
);
```

#### Evidência 4.1 — Alerta Emitido no Tópico `desafio.fraud.detected`

Payload JSON do alerta gerado acompanhado dos 3 eventos de transação que acionaram o gatilho da regra:

```json
<sua evidência aqui: o alerta emitido no tópico desafio.fraud.detected + as 3 transações (mesmo card_id, <60s) que o dispararam>
```

### Camada 5: Operação, Governança e Custos

#### 1. Observabilidade

* Métricas extraídas diretamente da **Confluent Metrics API** via `curl`.

* **Pico de vazão (`received_bytes`):** `<valor medido>`

* **Consumer Lag Máximo por Grupo:** `<valor medido e grupo analisado>`

#### 2. Matriz de Segurança (Identidades e Acessos)

| Service Account | Recurso / Tópico | Operação | Justificativa |
| :--- | :--- | :--- | :--- |
| `desafio-producer` | `desafio-*` | `WRITE` | Restringe a publicação apenas aos tópicos autorizados do domínio de pagamentos. |
| `desafio-consumer` | `desafio-*` | `READ` | Garante que o consumidor leia apenas tópicos de negócio e atualize offsets no grupo `desafio-*`. |

#### 3. Confiabilidade e Elo Mais Fraco

* **Análise Trecho a Trecho:**

  * *Postgres \\(\rightarrow\\) Kafka Connect (CDC):* Garantia *at-least-once* via replication slot do WAL.

  * *Producer \\(\rightarrow\\) Brokers:* Configurações `acks = all`, `replication.factor = 3` e `min.insync.replicas = 2` com `enable.idempotence = true`.

  * *Flink SQL:* Processamento interno com garantia de semântica *exactly-once* através de checkpoints transacionais.

  * **O Elo Mais Fraco:** O consumidor final / sistema antifraude a jusante. Ao receber o alerta via semântica de entrega *at-least-once*, rebalances ou retentativas podem reentregar alertas repetidos. A resiliência exige **idempotência de aplicação no destino** (ex.: controle via chave de evento ou operação `UPSERT`).

#### 4. Faturamento e Custos de Execução

* **Custo Total Declarado:** `US$ <X,XX>` (apurado via comando `confluent billing cost list`).

* **Duas Maiores Alavancas de Custo:**

  1. *Tempo de retenção e replicação:* Armazenar dados retidos por longos períodos multiplica o custo de storage pelo fator de replicação (3x). A retenção deve ser encurtada ou respaldada por Tiered Storage / compactação.

  2. *Tasks ativas de Connect e CFU do Flink:* Conectores e statements mantidos ativos sem tráfego continuam gerando tarifação contínua. A automação com `teardown.sh` após a janela operacional é fundamental.

#### Evidência 5.1 — Faturamento no Confluent Billing

```text
<sua evidência aqui: confluent billing cost list>
```

#### Evidência 5.2 — Verificação de Teardown Completo

Saída comprovando que conectores e statements do Flink foram removidos:

```text
\$ confluent connect cluster list
No connectors found.

\$ confluent flink statement list
No statements found.
```

## 📜 Licença

Projeto desenvolvido para fins educacionais no âmbito do Bootcamp Confluent Cloud / DIO.
