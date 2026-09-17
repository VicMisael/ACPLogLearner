# Predição online de resultados: VFDT categórico e regressão contínua

> Nota de decisão: a variante condicionada ao protocolo descrita abaixo foi
> preservada como ideia de ablação. A arquitetura principal passou a aprender
> outcomes independentes do protocolo usando somente execuções de 2PC clássico;
> a política recomenda o protocolo depois da previsão e o oracle mede os efeitos
> específicos de presumed abort e presumed commit.

## Motivação

O ALPS deve prever o comportamento esperado de cada protocolo candidato e deixar
a política escolher o protocolo. O modelo não deve aprender diretamente o nome
do melhor protocolo como classe principal, pois isso mistura predição com a
função de custo e dificulta explicar por que uma decisão foi tomada.

As duas abordagens abaixo compartilham o mesmo vetor genérico de entrada e a
mesma avaliação prequential:

```text
construir features com histórico até T-1
-> prever resultados para cada protocolo candidato
-> selecionar e executar um protocolo
-> observar e avaliar a previsão
-> atualizar apenas os modelos que receberam feedback válido
```

## Features genéricas de entrada

O perfil principal não deve conhecer TPC-C, warehouses ou IDs fixos de nós.
Ele utiliza:

- quantidade de participantes e indicação local/distribuída;
- contagens estimadas de leituras, inserções, atualizações e remoções;
- quantidade estimada de linhas afetadas e proporção de operações remotas;
- histórico global de commit, aborto, latência, redo, mensagens e logs;
- médias e piores valores históricos entre os participantes selecionados;
- protocolo candidato, quando a previsão for condicionada ao protocolo.

Identidade dos participantes e atributos específicos da carga permanecem como
perfis de ablação, não como entrada padrão.

## Abordagem A — VFDT categórico multi-head

Usar um VFDT independente para cada alvo categórico. Uma configuração inicial
pequena evita explosão de classes:

```text
CompletionOutcome = COMMIT | ABORT | TIMEOUT
LatencyClass      = LOW | MEDIUM | HIGH
RedoClass         = LOW | MEDIUM | HIGH
```

Alvos posteriores, condicionados à existência de dados suficientes, podem
incluir `BlockingClass`, `LogPressureClass` e `RecoveryRisk`.

As classes `LOW`, `MEDIUM` e `HIGH` devem usar limites fixos justificados ou
quantis online calculados somente com o histórico até `T-1`. Os limites usados
para rotular `T` precisam ser congelados antes da execução e registrados junto
ao resultado. Métricas indisponíveis ou invalidadas por reset não são zero e não
devem atualizar o respectivo head.

### Vantagens

- compatível com o VFDT já usado no protótipo;
- regras da árvore são interpretáveis;
- adaptação incremental natural;
- classes simples facilitam matrizes de confusão e macro-F1.

### Limitações

- discretização perde a magnitude das diferenças;
- limites de classe podem variar entre ambientes;
- vários heads exigem amostras suficientes para cada protocolo e alvo;
- combinações como `COMMIT_SLOW_HIGH_REDO` não devem virar uma única classe.

## Abordagem B — previsão contínua multi-head

Usar classificadores online para probabilidades discretas e regressores online
independentes para custos numéricos. Para cada protocolo candidato, produzir:

```text
P(commit), P(abort), P(timeout)
latência total e por fase
tempo de bloqueio
bytes de redo, avanço de LSN, escritas e esperas
mensagens e escritas de log do protocolo
incerteza da previsão
```

### Vantagens

- preserva valores e diferenças de magnitude;
- permite trocar pesos da política sem treinar novamente os modelos;
- facilita análise de sensibilidade e fronteiras de Pareto;
- permite estimar regret diretamente em unidades de custo.

### Limitações

- exige algoritmos online de regressão além do VFDT classificador;
- interpretação pode ser menos direta que regras categóricas;
- escalas, outliers e calibração de incerteza exigem tratamento explícito;
- feedback contrafactual continua indisponível sem exploração ou replay pareado.

## Política e pesos

Os pesos pertencem à política de decisão, não às features de entrada da árvore.
Preservar todas as previsões brutas e normalizar usando somente dados até
`T-1` antes de aplicar:

```text
expected_cost(protocol) =
    w_latency  * normalized_latency
  + w_redo     * normalized_redo
  + w_logs     * normalized_log_writes
  + w_messages * normalized_messages
  + w_blocking * normalized_blocking
  + P(timeout) * timeout_penalty
  + P(failure) * failure_penalty
```

Executar pelo menos perfis balanceado, orientado a latência e orientado a
armazenamento, além de análise de sensibilidade dos pesos. Falhas de correção,
branches XA não resolvidas e violações de atomicidade devem ser restrições ou
penalidades dominantes.

## Feedback parcial no aprendizado online

Uma execução real revela imediatamente apenas o resultado do protocolo
selecionado. Existem duas formas de aprender sobre os demais candidatos:

1. exploração controlada, tratando a seleção como um problema contextual com
   feedback parcial;
2. replay da mesma especificação sob todos os protocolos em estado restaurado,
   produzindo feedback completo e o oracle experimental.

Rótulos ou custos ausentes devem pular a atualização do head correspondente;
nunca devem ser preenchidos com zero.

## Experimentos propostos

1. VFDT categórico genérico como abordagem inicial.
2. Modelo contínuo genérico como alternativa principal.
3. Ablação com identidade de participantes.
4. Ablação com atributos específicos da carga.
5. Comparação de acurácia/macro-F1, erro de regressão, custo, regret, adaptação
   após drift, tamanho do modelo e overhead de inferência/treino.

## Estado da implementação anterior

A abordagem A está implementada com três VFDTs independentes para conclusão,
latência total e bytes de redo. Cada árvore recebe o protocolo candidato como
feature e produz classe, distribuição de probabilidades e confiança. O runner
consulta os três protocolos, aplica uma política ponderada, executa apenas o
escolhido e treina somente os heads com feedback válido.

Os limites de latência e redo usam quantis online independentes calculados
somente sobre janelas anteriores e congelados antes de cada transação. O warmup
cobre cada protocolo uma vez e a exploração epsilon-greedy determinística reduz
o viés do feedback parcial. A abordagem B permanece como a próxima alternativa
para preservar custos contínuos e medir erro e regret em unidades naturais.
