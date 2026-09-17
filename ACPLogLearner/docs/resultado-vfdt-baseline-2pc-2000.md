# VFDT multi-head com baseline 2PC — 2.000 transações

Data: 2026-09-12

## Arquitetura avaliada

O modelo recebe apenas features genéricas da transação e histórico até `T-1`.
Não existem features `candidate_protocol_*`. Para cada transação:

1. o VFDT prevê conclusão, latência e redo;
2. a política produz uma recomendação contrafactual de protocolo;
3. o runner executa sempre classic 2PC;
4. os heads são avaliados e treinados somente com o resultado desse baseline.

O log confirmou `executedBaseline=TWO_PC` em todas as 2.000 transações.

## Configuração

```bash
ACP_ALPS_EXAMPLES=2000 \
ACP_ALPS_SCENARIO=PHASED \
ACP_ALPS_PHASE_LENGTH=400 \
ACP_ALPS_LATENCY_SPIKE_MS=50 \
ACP_ALPS_ABORT_SPIKE_RATE=0.70 \
ACP_ALPS_HISTORY_WINDOW_SIZE=30 \
ACP_ALPS_LABEL_WINDOW_SIZE=30 \
./gradlew runMariaDbAlps
```

O run terminou em `4m21s`, com 1.278 commits e 722 abortos.

## Resultados

| Fase | Commit/abort | Latência média | Acc. conclusão | Acc. latência | Acc. redo | Recomendação 2PC / PA / PC |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Global | 1278 / 722 | 120.51 ms | 0.9215 | 0.8205 | 0.6095 | 1 / 815 / 1184 |
| `BASELINE` | 400 / 0 | 30.06 ms | 1.0000 | 0.9750 | 0.4300 | 1 / 0 / 399 |
| `LATENCY_SPIKE` | 400 / 0 | 265.24 ms | 1.0000 | 0.6575 | 0.8775 | 0 / 0 / 400 |
| `ABORT_SPIKE` | 36 / 364 | 20.60 ms | 0.8300 | 0.9275 | 0.4800 | 0 / 368 / 32 |
| `COMBINED_SPIKE` | 42 / 358 | 261.11 ms | 0.8950 | 0.9325 | 0.5050 | 0 / 400 / 0 |
| `RECOVERY` | 400 / 0 | 25.55 ms | 0.8825 | 0.6100 | 0.7550 | 0 / 47 / 353 |

As três árvores terminaram com 29 nós. As profundidades foram 7 para conclusão,
7 para latência e 6 para redo.

## Leitura inicial

- A política recomendou presumed commit em praticamente todo o baseline e pico
  de latência, quando todos os resultados foram commit.
- No pico de abortos, mudou majoritariamente para presumed abort. Na fase
  combinada, recomendou presumed abort nas 400 transações.
- Na recuperação, demorou a abandonar o conceito anterior: ainda recomendou
  presumed abort 47 vezes, mas terminou com 353 recomendações de presumed
  commit.
- O head de redo dividiu primeiro por `remote_operation_ratio`, confirmando que
  fan-out remoto é um sinal genérico relevante.
- O head de conclusão dividiu primeiro pelo máximo avanço histórico de LSN entre
  os participantes.
- O head de latência dividiu primeiro por `global_samples`; como essa feature
  satura no tamanho da janela, sua utilidade e possível correlação com o início
  do experimento precisam ser avaliadas por ablação.

As recomendações não demonstram economia real de custo porque PA e PC não foram
executados. Essa conclusão depende do futuro oracle pareado, que repetirá a mesma
especificação nos três protocolos sob estado equivalente.

## Artefato bruto

O log textual completo contém 20.947 linhas e está em:

```text
/tmp/alps-2pc-baseline-2000.log
```
