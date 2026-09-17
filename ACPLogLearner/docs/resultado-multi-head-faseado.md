# Resultado controlado do VFDT multi-head faseado

> Estes resultados pertencem à implementação anterior, condicionada ao
> protocolo candidato. Eles permanecem como diagnóstico histórico e não devem
> ser comparados diretamente com o novo baseline treinado apenas em 2PC clássico.

Data: 2026-09-12

## Objetivo

Exercitar o aprendizado online sob mudanças abruptas que não são entregues como
features ao modelo: operação normal, pico de latência, pico de abortos, pico
combinado e recuperação. O teste é diagnóstico e não representa ainda uma
avaliação final da dissertação.

## Configuração

```bash
ACP_ALPS_EXAMPLES=150 \
ACP_ALPS_SCENARIO=PHASED \
ACP_ALPS_PHASE_LENGTH=30 \
ACP_ALPS_LATENCY_SPIKE_MS=50 \
ACP_ALPS_ABORT_SPIKE_RATE=0.70 \
ACP_ALPS_EXPLORATION_RATE=0.15 \
ACP_ALPS_HISTORY_WINDOW_SIZE=30 \
ACP_ALPS_LABEL_WINDOW_SIZE=30 \
./gradlew runMariaDbAlps
```

- Perfil de features: `GENERIC`.
- Cada fase contém 30 transações.
- A latência injetada é aplicada explicitamente pelo message bus nos dois
  sentidos; não existe `sleep` oculto no protocolo.
- Nos picos de aborto, cada participante tem probabilidade mínima de aborto de
  `0.70`.
- O conjunto de participantes continua variando entre um e quatro nós.
- O warmup cobre os três protocolos e a exploração posterior usa seed fixa.

## Resultado global

| Métrica | Valor |
| --- | ---: |
| Transações | 150 |
| Commits / abortos | 101 / 49 |
| Acurácia de conclusão | 0.5533 |
| Acurácia de latência | 0.6467 |
| Acurácia de redo | 0.4133 |
| Latência média | 106.69 ms |
| Redo médio | 2070.37 bytes |
| Overrides por exploração | 12 |
| Seleções 2PC / PA / PC | 6 / 41 / 103 |

## Resultado por fase

| Fase | Commit/abort | Latência média | Acc. conclusão | Acc. latência | Acc. redo | 2PC / PA / PC |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `BASELINE` | 30 / 0 | 31.10 ms | 1.0000 | 0.8333 | 0.5667 | 2 / 2 / 26 |
| `LATENCY_SPIKE` | 30 / 0 | 222.33 ms | 1.0000 | 0.2667 | 0.3333 | 1 / 0 / 29 |
| `ABORT_SPIKE` | 6 / 24 | 20.67 ms | 0.2667 | 0.9667 | 0.1000 | 2 / 4 / 24 |
| `COMBINED_SPIKE` | 5 / 25 | 230.37 ms | 0.4667 | 0.1667 | 0.2667 | 1 / 9 / 20 |
| `RECOVERY` | 30 / 0 | 29.00 ms | 0.0333 | 1.0000 | 0.8000 | 0 / 26 / 4 |

## Interpretação

1. A injeção de latência foi efetiva: a média passou de aproximadamente `31 ms`
   para `222–230 ms` nas fases afetadas.
2. O pico de aborto também foi efetivo: ocorreram 24 e 25 abortos nas duas fases
   configuradas, contra zero no baseline e na recuperação.
3. O VFDT de conclusão reagiu tarde. Na recuperação, ainda selecionou presumed
   abort em 26 de 30 transações e obteve apenas `0.0333` de acurácia.
4. O head de latência permaneceu com uma única folha após 150 exemplos. Ele
   aprendeu a classe majoritária `LOW`, não o pico transitório.
5. O head de redo também permaneceu com uma única folha. Redo variou tanto com
   fan-out quanto com commit/abort; 150 exemplos ainda não separaram esses
   efeitos.
6. A árvore de conclusão criou uma divisão usando
   `participants_mean_latency_ms`, indicando que o histórico capturou mudança de
   regime, mas o VFDT clássico acumulativo não esqueceu o conceito anterior com
   rapidez suficiente.

## Consequências para os próximos experimentos

- Manter este VFDT como baseline categórico interpretável.
- Comparar com uma árvore adaptativa com detector de drift ou janela de treino,
  sem alterar features, execução ou política.
- Medir tempo até recuperação, macro-F1 e balanced accuracy, não apenas acurácia
  global.
- Executar fases mais longas e repetir cada configuração com múltiplas seeds.
- Não introduzir timeouts reais antes de implementar verificação e limpeza de
  branches XA preparados; encerrar uma tentativa atrasada pode deixar estado
  `in-doubt`.
- Construir o oracle pareado antes de concluir que a mudança de seleção reduziu
  custo ou regret.

Este run inseriu 101 New-Orders confirmadas e abortou 49 tentativas. O resultado
é controlado, com banco serializado e sem carga concorrente externa.

## Repetição com 2.000 transações

Foi executada uma repetição com as mesmas condições, aumentando cada fase para
400 transações:

```bash
ACP_ALPS_EXAMPLES=2000 \
ACP_ALPS_SCENARIO=PHASED \
ACP_ALPS_PHASE_LENGTH=400 \
ACP_ALPS_LATENCY_SPIKE_MS=50 \
ACP_ALPS_ABORT_SPIKE_RATE=0.70 \
ACP_ALPS_EXPLORATION_RATE=0.15 \
ACP_ALPS_HISTORY_WINDOW_SIZE=30 \
ACP_ALPS_LABEL_WINDOW_SIZE=30 \
./gradlew runMariaDbAlps
```

O run terminou em `3m48s`, com 1.278 commits e 722 abortos.

| Fase | Commit/abort | Latência média | Acc. conclusão | Acc. latência | Acc. redo | 2PC / PA / PC |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Global | 1278 / 722 | 103.63 ms | 0.9375 | 0.7135 | 0.6020 | 240 / 329 / 1431 |
| `BASELINE` | 400 / 0 | 25.32 ms | 1.0000 | 0.6525 | 0.5375 | 10 / 19 / 371 |
| `LATENCY_SPIKE` | 400 / 0 | 230.17 ms | 1.0000 | 0.6875 | 0.8650 | 31 / 23 / 346 |
| `ABORT_SPIKE` | 36 / 364 | 20.28 ms | 0.8750 | 0.8450 | 0.4275 | 93 / 23 / 284 |
| `COMBINED_SPIKE` | 42 / 358 | 220.64 ms | 0.8775 | 0.5600 | 0.5075 | 62 / 228 / 110 |
| `RECOVERY` | 400 / 0 | 21.78 ms | 0.9350 | 0.8225 | 0.6725 | 44 / 36 / 320 |

Com mais dados e fases longas, os três heads produziram árvores não triviais:

- conclusão: 31 nós, profundidade 8;
- latência: 25 nós, profundidade 7;
- redo: 29 nós, profundidade 7.

A raiz do head de latência separou o candidato presumed abort. A raiz de redo
separou por `remote_operation_ratio`, um sinal genérico coerente com fan-out de
escrita. A raiz de conclusão usou o máximo avanço histórico de LSN entre os
participantes. O resultado mostra que 150 exemplos eram insuficientes para
avaliar a capacidade representacional do VFDT.

A recuperação de conclusão aumentou de `0.0333` no run curto para `0.9350` no
run longo. Isso não elimina a limitação de drift: o VFDT recebeu 400 exemplos de
recuperação, e a mudança também ficou visível nas features históricas móveis.
Ainda é necessário medir quantas transações foram necessárias até recuperar a
qualidade, em vez de observar apenas a média da fase.

### Achado metodológico sobre os rótulos

Os quantis móveis tornam `LOW`, `MEDIUM` e `HIGH` relativos ao regime recente.
Depois que a janela passa a conter apenas o pico, aproximadamente `230 ms` pode
ser rotulado como `LOW`. Portanto, a acurácia do head de latência não significa
necessariamente que ele detectou custo absoluto alto. Para a avaliação da tese,
é necessário comparar pelo menos:

1. limites absolutos fixados antes do experimento;
2. quantis derivados somente de um período de calibração e depois congelados;
3. regressão contínua dos valores brutos.

Os valores brutos de latência e redo devem continuar sendo preservados para que
a interpretação não dependa apenas dos rótulos adaptativos.
