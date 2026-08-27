# Resultado inicial do ALPS com MariaDB

**Data:** 24 de agosto de 2026  
**Ambiente:** Java 21, quatro containers MariaDB 11.4, XA e dados BenchmarkSQL/TPC-C-like

## 1. Objetivo desta execução

O objetivo foi executar o pipeline adaptativo do ALPS sobre bancos reais e
verificar se o modelo atual conseguiria aprender algum padrão, mesmo que o
resultado natural fosse a ausência de padrões.

O fluxo avaliado foi:

```text
conjunto de participantes
        ↓
previsão do contexto pelo modelo atual (VFDT)
        ↓
política de seleção de protocolo
        ↓
2PC / Presumed Abort / Presumed Commit
        ↓
transações New-Order usando MariaDB XA
        ↓
contexto observado
        ↓
treinamento online do modelo
```

Esta execução representa a primeira baseline funcional do ALPS sobre o cluster
MariaDB.

## 2. Ambiente validado

Foram utilizados quatro containers existentes:

| Participante | Container | Porta | Dataset local |
| --- | --- | ---: | --- |
| `P1` | `acp-mariadb-1` | `33061` | warehouse `1` |
| `P2` | `acp-mariadb-2` | `33062` | warehouse `1` |
| `P3` | `acp-mariadb-3` | `33063` | warehouse `1` |
| `P4` | `acp-mariadb-4` | `33064` | warehouse `1` |

Cada banco foi verificado com:

- 1 warehouse;
- 100.000 itens;
- 10 distritos;
- nenhuma branch XA pendente antes dos testes.

O provedor Compose não estava disponível. Os containers existentes foram
iniciados diretamente com Podman, sem recriar ou destruir os volumes.

## 3. Smoke test dos protocolos

Antes de conectar o modelo, foram executadas duas transações reais com cada
protocolo:

| Protocolo | Resultado | Latência média observada |
| --- | --- | ---: |
| Classic 2PC | 2/2 commits | 234,0 ms |
| 2PC Presumed Abort | 2/2 commits | 198,5 ms |
| 2PC Presumed Commit | 2/2 commits | 180,5 ms |

As primeiras transações incluíram custo de inicialização de classes, driver e
conexões. Depois do aquecimento, as latências ficaram aproximadamente entre 30 e
61 ms.

Esses números são apenas um smoke test. A amostra é pequena demais para concluir
que um protocolo é mais rápido que outro.

## 4. Entry point do experimento

Foi criado `MariaDbAlpsMain`, executado pela tarefa:

```bash
./gradlew runMariaDbAlps
```

Para cada exemplo, o runner:

1. escolhe um subconjunto de `P1`, `P2`, `P3` e `P4`;
2. cria as features disponíveis antes da transação;
3. solicita ao VFDT uma classe de contexto;
4. usa a política para selecionar o protocolo;
5. executa uma janela de transações XA New-Order reais;
6. calcula a taxa de commit/abort da janela;
7. deriva a classe de contexto observada;
8. avalia a previsão;
9. treina o modelo com o exemplo observado.

As classes atuais são:

```text
LIKELY_COMMIT
LIKELY_ABORT
UNCERTAIN
```

A política atual é:

| Contexto previsto | Protocolo selecionado |
| --- | --- |
| `LIKELY_COMMIT` | `TWO_PC_PRESUMED_COMMIT` |
| `LIKELY_ABORT` | `TWO_PC_PRESUMED_ABORT` |
| `UNCERTAIN` | `TWO_PC` |

## 5. Execução natural

### Configuração

```bash
ACP_ALPS_EXAMPLES=10 \
ACP_ALPS_WINDOW_SIZE=2 \
./gradlew runMariaDbAlps
```

Todas as taxas de aborto ficaram no valor padrão `0.0`.

### Resultado

```text
examples=10
transactions=20
commitRate=1.0
contextAccuracy=1.0
labels={LIKELY_COMMIT=10}
selections={TWO_PC_PRESUMED_COMMIT=10}
avgLatencyMs=46.45
avgMessages=5.8
avgLogWrites=5.2
model=VFDT
nodes=1
depth=0
```

Regra final:

```text
Leaf = LIKELY_COMMIT
```

### Interpretação

O modelo não encontrou relação entre a identidade dos participantes e o
resultado. Ele aprendeu apenas que todas as transações observadas faziam commit.

Isso é coerente com quatro containers homogêneos, saudáveis e sem falhas ou
abortos configurados. A ausência de uma divisão na árvore é um resultado válido:
não havia sinal discriminativo nos dados disponíveis.

## 6. Execução controlada

Uma segunda execução foi feita para comprovar que o pipeline conseguiria aprender
quando houvesse um sinal conhecido.

### Configuração

```bash
ACP_ALPS_EXAMPLES=30 \
ACP_ALPS_WINDOW_SIZE=3 \
ACP_ALPS_P1_ABORT_RATE=0.0 \
ACP_ALPS_P2_ABORT_RATE=0.1 \
ACP_ALPS_P3_ABORT_RATE=0.4 \
ACP_ALPS_P4_ABORT_RATE=0.8 \
./gradlew runMariaDbAlps
```

As taxas são controles determinísticos de injeção de voto abort. Elas não foram
medidas nos containers e não representam uma propriedade natural dos bancos.

### Resultado

```text
examples=30
transactions=90
contextAccuracy=0.6666666666666666
labels={LIKELY_COMMIT=6, UNCERTAIN=2, LIKELY_ABORT=22}
selections={TWO_PC_PRESUMED_COMMIT=6, TWO_PC_PRESUMED_ABORT=24}
commitRate=0.28888888888888886
avgLatencyMs=33.922222222222224
avgMessages=8.477777777777778
avgLogWrites=5.8
model=VFDT
nodes=3
depth=1
```

Regra aprendida:

```text
if has_P4 = false:
    class = LIKELY_COMMIT

if has_P4 = true:
    class = LIKELY_ABORT
```

O limiar numérico exibido pelo VFDT foi aproximadamente `0.09`. Como a feature é
binária, esse limiar separa ausência (`0`) de presença (`1`).

### Interpretação

O pipeline aprendeu corretamente que a presença de `P4` estava associada a uma
maior probabilidade de aborto. Isso demonstra que:

- a execução MariaDB está conectada ao ciclo online;
- a classe prevista influencia o protocolo executado;
- o resultado real da janela produz o rótulo de treinamento;
- o VFDT consegue criar divisões quando existe sinal;
- a política muda de presumed commit para presumed abort durante a execução.

Esse padrão é induzido e não deve ser apresentado como descoberta sobre o
MariaDB, Docker ou Podman. Seu valor é validar a capacidade de aprendizado do
pipeline sob uma condição controlada.

## 7. Problema de correção encontrado

Durante a primeira tentativa natural, presumed commit permitiu que o runner
considerasse a transação concluída assim que o coordenador registrasse
`GLOBAL_COMMIT`.

No presumed commit, o coordenador pode esquecer a transação sem esperar ACKs de
commit. Entretanto, os participantes ainda precisam receber a decisão e concluir
o `XA COMMIT` no banco.

O runner desligou alguns atores antes dessa conclusão e deixou três branches
preparadas:

```text
MariaDB 1: 1 branch
MariaDB 4: 2 branches
```

As decisões globais dessas transações eram commit. As branches foram concluídas
explicitamente com `XA COMMIT`.

Foi criado `CompletionTrackingTransactionalResource`, que só marca o participante
como resolvido depois que o commit ou rollback real retorna com sucesso. Os
runners agora aguardam:

```text
estado terminal do coordenador
AND
todos os recursos participantes resolvidos
```

Depois da correção:

- o experimento natural terminou normalmente;
- o experimento controlado terminou normalmente;
- `XA RECOVER` ficou vazio nos quatro bancos;
- foi adicionado teste para impedir regressão no rastreamento de conclusão.

## 8. Estado final dos bancos

Ao final dos experimentos, a quantidade de registros em `bmsql_new_order` era:

| Nó | Registros |
| --- | ---: |
| MariaDB 1 | 9.037 |
| MariaDB 2 | 9.036 |
| MariaDB 3 | 9.030 |
| MariaDB 4 | 9.019 |

As diferenças são esperadas porque os conjuntos de participantes e resultados de
commit variaram durante a execução controlada.

Nenhum banco apresentou branch em `XA RECOVER` ao final.

## 9. Validação do código

A suíte foi executada com Java 21:

```text
BUILD SUCCESSFUL
```

Foram mantidos testes para:

- política de contexto para protocolo;
- conclusão de aborto no presumed commit;
- formato do workload New-Order;
- rastreamento de conclusão do recurso transacional.

## 10. Limitações atuais

### Features limitadas

O modelo recebe somente features binárias de participação:

```text
has_P1
has_P2
has_P3
has_P4
```

Latência, mensagens e log writes são medidos depois da execução, mas ainda não
são usados como histórico para prever transações futuras.

### Logs lógicos

`avgLogWrites` contabiliza registros lógicos escritos pelos gerenciadores Java.
Ele não representa diretamente quantidade de fsyncs, bytes ou escritas físicas
do MariaDB/InnoDB.

### TPC-C-like

O workload usa tabelas BenchmarkSQL e a forma da transação New-Order, mas não é
uma execução TPC-C oficial ou auditada.

### Ausência de sharding global

Cada banco ainda contém um warehouse local com identificador `1`. Portanto, os
quatro bancos são datasets independentes, não shards com identificadores globais.

### Falha controlada

Na execução controlada, o participante escolhido para votar abort não inicia seu
branch XA. Os demais participantes ainda podem preparar trabalho real e depois
recebem rollback global. Futuras injeções devem também avaliar falhas ocorridas
depois de `XA START`, durante o SQL e depois de `XA PREPARE`.

### Tamanho da amostra

As execuções foram pequenas e destinadas a validação. As médias observadas não
devem ser usadas como resultado final da dissertação.

## 11. Baseline alcançada

Agora o projeto possui:

- quatro MariaDB reais com dados BenchmarkSQL;
- execução XA de New-Order;
- classic 2PC, presumed abort e presumed commit;
- seleção adaptativa de protocolo;
- treinamento online usando resultados reais;
- execução natural sem padrão discriminativo;
- execução controlada com padrão aprendido;
- verificação de branches XA pendentes;
- suíte de testes Java 21 passando.

## 12. Próximos incrementos recomendados

### 1. Features históricas reais

Criar janelas históricas que possam ser usadas antes da próxima transação:

- taxa de commit/abort por participante;
- latência média e percentis de prepare;
- tempo de decisão;
- blocking time;
- timeouts;
- mensagens;
- log writes lógicos;
- erros XA;
- participante e conjunto de participantes.

Isso permitirá testar a hipótese central do ALPS: seleção baseada em métricas,
em vez de depender somente da identidade de um nó.

### 2. Eventos estruturados em memória

Adicionar `AlpsEvent`, `InMemoryEventPublisher` e `TransactionTrace` sem alterar os
logs de correção. Esses eventos devem alimentar as métricas históricas e o modelo.

### 3. Modelo intercambiável

Introduzir uma interface genérica de modelo/classificador. VFDT continua sendo o
modelo atual, mas não deve definir toda a arquitetura.

### 4. Sharding de warehouses

Criar identificadores globais e um `WarehouseRouter`, permitindo transações que
toquem shards reais diferentes.

### 5. Experimentos pareados

Reexecutar o mesmo `TpccTransactionSpec` sob os três protocolos usando snapshots
equivalentes. Isso permitirá comparar custo e construir um oracle experimental.

### 6. Resultados persistentes

Exportar um registro agregado por transação ou janela para análise estatística.
Os eventos internos continuam em memória. JSONL e Kafka permanecem extensões
futuras.

## 13. Comandos úteis

Experimento natural:

```bash
ACP_ALPS_EXAMPLES=10 \
ACP_ALPS_WINDOW_SIZE=2 \
./gradlew runMariaDbAlps
```

Experimento controlado:

```bash
ACP_ALPS_EXAMPLES=30 \
ACP_ALPS_WINDOW_SIZE=3 \
ACP_ALPS_P1_ABORT_RATE=0.0 \
ACP_ALPS_P2_ABORT_RATE=0.1 \
ACP_ALPS_P3_ABORT_RATE=0.4 \
ACP_ALPS_P4_ABORT_RATE=0.8 \
./gradlew runMariaDbAlps
```

Smoke test com protocolo fixo:

```bash
ACP_PROTOCOL=TWO_PC ACP_TPCC_TRANSACTIONS=2 \
./gradlew runMariaDbTpccCluster
```

Testes:

```bash
./gradlew test
```

## 14. Conclusão

A primeira integração ALPS/MariaDB foi bem-sucedida. O caso natural confirmou que
um ambiente homogêneo e saudável não oferece sinal suficiente nas features
atuais. O caso controlado confirmou que o pipeline consegue aprender e alterar a
seleção de protocolo quando existe uma relação observável.

O resultado mais importante não é a regra artificial envolvendo `P4`, mas a
existência de uma baseline completa e verificável a partir da qual features
históricas reais, modelos alternativos, sharding e experimentos rigorosos podem
ser incrementados.

