# Previsão online de transações e seleção de protocolos: resultados preliminares

Versão inicial para discussão com o orientador, elaborada em 02/10/2026.
Experimento analisado: `mac-2h-20261002-091218`.

## 1. Objetivo da avaliação

Este trabalho investiga o uso de aprendizado online para prever propriedades
de transações futuras e utilizar essas previsões na seleção de protocolos de
atomic commit. A proposta separa a predição do comportamento da transação da
política responsável pela escolha do protocolo. Essa separação permite avaliar
tanto a qualidade das previsões quanto o custo das decisões resultantes.

A implementação atual considera três candidatos: 2PC clássico, Presumed Abort
e Presumed Commit. Esses protocolos constituem o conjunto avaliado nesta etapa,
mas não delimitam o escopo da proposta. Tanto os protocolos candidatos quanto
os alvos de predição podem ser ampliados em etapas posteriores.

A questão experimental é se a seleção orientada por previsões apresenta menor
custo que a utilização fixa de um protocolo ou que uma regra simples baseada
no histórico recente de execução.

## 2. Método experimental

### 2.1. Ambiente e carga

O ambiente utiliza quatro instâncias MariaDB, com um warehouse por instância,
e coordenação de transações distribuídas em Java por meio de XA. O esquema e
os dados derivam do BenchmarkSQL. A carga executa transações New-Order
distribuídas e é caracterizada como TPC-C-like, sem reivindicação de conformidade
com o benchmark TPC-C completo.

O experimento foi executado no macOS 26.6.2, com Java 21.0.4, e terminou em
2h01min40s. Foram utilizadas sete seeds, de 101 a 107. Cada seed incluiu 100
transações lógicas de aquecimento e cinco fases de 430 transações medidas.
Assim, foram realizadas 15.050 transações lógicas medidas e 700 de aquecimento.
Como cada especificação foi executada nos três protocolos, o total foi de
47.250 execuções XA, das quais 45.150 pertencem à etapa de medição.

As fases ocorreram na ordem: baseline, pico de latência, pico de abortos,
pico combinado e recuperação. Nas fases de latência, o barramento recebeu
atraso injetado de 50 ms. Nas fases de abortos, cada participante recebeu
probabilidade de voto de aborto de 0,70. Esse parâmetro não representa uma
taxa global de abortos de 70%: a transação aborta quando pelo menos um
participante vota pelo aborto. Para n participantes com votos independentes,
a probabilidade correspondente é 1 - (1 - 0,70)^n.

### 2.2. Predição e atualização online

O preditor recebe 37 características: dez descrevem a transação a executar,
dez resumem o histórico global e dezessete resumem o histórico dos participantes
envolvidos. O perfil genérico inclui a quantidade de operações e participantes,
a proporção de operações remotas e métricas históricas de conclusão, latência
e redo. Identificadores dos nós, campos específicos de warehouses e a fase
experimental não entram no vetor genérico.

Para cada protocolo, três árvores VFDT estimam conclusão, classe de latência e
classe de redo, totalizando nove árvores. A conclusão pode assumir COMMIT,
ABORT ou TIMEOUT. Latência e redo usam classes LOW, MEDIUM e HIGH. As árvores
retornam classes e distribuições de probabilidade. Na execução analisada, os
limites fixos das classes foram 25 e 75 ms para latência, e 2.000 e 10.000 bytes
para redo. Valores iguais aos limites pertencem à classe inferior.

Todas as árvores recebem o mesmo vetor disponível antes da transação. A
política escolhe o menor escore calculado a partir das distribuições previstas
para cada candidato. Esse escore categórico não é idêntico à função de custo
operacional usada na avaliação. Após as execuções pareadas, cada conjunto de
árvores aprende com o resultado do respectivo protocolo. O histórico da
política é atualizado com o resultado do candidato selecionado. As janelas
históricas têm capacidade de 30 observações. A ordem preserva a avaliação
prequential: prever, executar, avaliar e somente então aprender.

### 2.3. Comparação pareada e políticas de referência

Cada transação lógica é executada com os mesmos participantes, entradas e
votos nos três protocolos. A ordem dos candidatos é pseudoaleatória e
controlada por seed. O mecanismo de avaliação captura o estado afetado,
restaura os dados após cada execução e verifica sua equivalência lógica.
Branches XA preparados e não resolvidos impedem a continuação do experimento.

São comparadas as políticas de protocolo fixo, seleção aleatória, seleção
baseada no VFDT e uma heurística com limites fixos. A heurística observa as
últimas 30 transações: taxa de commit de pelo menos dois terços favorece
Presumed Commit; taxa de no máximo um terço favorece Presumed Abort; nos
demais casos, escolhe 2PC. Com menos de dez observações, escolhe 2PC.
O termo estática descreve a regra, enquanto a escolha varia com o histórico.

O oracle identifica retrospectivamente o candidato válido de menor custo
para cada especificação. Ele é uma referência de avaliação, pois depende de
resultados ainda indisponíveis no instante da decisão. As comparações de
políticas aproveitam as execuções pareadas, em vez de executar sete cargas
independentes, uma para cada política.

### 2.4. Métricas e tratamento dos abortos

O custo operacional é adimensional e utiliza os pesos e escalas registrados
antes da execução:

```text
C = latência_ms / 100
  + redo_bytes / 10.000
  + mensagens / 10
  + registros_lógicos_do_protocolo / 10
  + penalidades
```

O timeout acrescenta 100 unidades e uma tentativa inválida acrescenta 1.000.
Um timeout também invalida a tentativa. Resultados diferentes do esperado,
resets de contadores e ausência de redo obrigatório são critérios de
invalidação. Um aborto esperado acumula os custos medidos da execução, sem
penalidade adicional apenas por ter abortado. Regret é a diferença entre o
custo da escolha da política e o custo do oracle para a mesma especificação.

Para caracterizar trabalho concluído, calculam-se também:

```text
taxa_de_abortos = tentativas_abortadas / tentativas_medidas
custo_por_commit = soma_do_custo_de_todas_as_tentativas / número_de_commits
```

O custo por commit inclui o gasto das tentativas abortadas no numerador.
Cada política contribui com uma escolha por transação lógica. Os três
candidatos pareados não são contados como três tentativas de negócio da
mesma política. Aquecimento é excluído. Essa métrica não mede retentativas
reais até o sucesso, pois o experimento não implementa esse processo.

As comparações inferenciais utilizam diferenças de custo médio por seed e
intervalos de 95% com distribuição t de Student, com seis graus de liberdade.
Portanto, há sete unidades de replicação, não 15.050 amostras independentes
para esses intervalos. As médias por seed possuem o mesmo peso, pois todas
as seeds contêm a mesma quantidade de observações. Os intervalos são nominais,
sem ajuste para comparações múltiplas.

## 3. Resultados

### 3.1. Integridade e conclusão das transações

Os registros contêm 2.150 observações medidas por seed, sem duplicações de
pares seed/índice. Todos os 45.150 candidatos medidos foram classificados
como válidos. Os resultados coincidiram com a conclusão esperada em todos
os casos. Por política, foram observados 9.756 commits e 5.294 abortos,
correspondentes a uma taxa de abortos de 35,18%. Não houve timeouts registrados.

A igualdade dos resultados de conclusão entre candidatos decorre dos votos
controlados e compartilhados. Assim, esta avaliação compara o custo de
resolver os mesmos commits e abortos, e não a capacidade de reduzir a
probabilidade de aborto. A ausência de tentativas inválidas sustenta a
correção nos cenários exercitados, mas não constitui prova geral de correção
sob falhas ou recuperação de crashes.

### 3.2. Custo e concordância com o oracle

| Política | Custo médio | Regret médio | Concordância com oracle | Custo por commit |
| --- | ---: | ---: | ---: | ---: |
| Oracle | 2,6394 | 0,0000 | 100,00% | 4,0717 |
| Heurística | 2,7094 | 0,0700 | 88,87% | 4,1797 |
| VFDT adaptativo | 2,8833 | 0,2438 | 75,06% | 4,4479 |
| Presumed Commit fixo | 3,0315 | 0,3921 | 60,26% | 4,6766 |
| Presumed Abort fixo | 3,0960 | 0,4566 | 38,21% | 4,7760 |
| Aleatória | 3,2598 | 0,6204 | 33,84% | 5,0288 |
| 2PC fixo | 3,6760 | 1,0366 | 1,53% | 5,6708 |

O adaptativo apresentou menor custo médio que todos os protocolos fixos e
que a escolha aleatória. Em relação ao melhor protocolo fixo, Presumed
Commit, a redução foi de 4,89%. Entretanto, a heurística apresentou o menor
custo entre as políticas que não utilizam informação retrospectiva do oracle.
O custo do adaptativo foi 6,42% maior que o da heurística.

| Referência | Diferença de custo: adaptativo menos referência | IC 95% pareado | Redução relativa do adaptativo |
| --- | ---: | --- | ---: |
| 2PC fixo | -0,7927 | [-0,8398; -0,7456] | 21,56% |
| Presumed Abort fixo | -0,2127 | [-0,2528; -0,1726] | 6,87% |
| Presumed Commit fixo | -0,1482 | [-0,1892; -0,1073] | 4,89% |
| Aleatória | -0,3766 | [-0,4236; -0,3295] | 11,55% |
| Heurística | +0,1738 | [+0,1350; +0,2126] | -6,42% |

Os intervalos favorecem o adaptativo frente aos protocolos fixos, mas
favorecem a heurística na comparação direta. A direção dessas diferenças
foi a mesma nas sete seeds. Esse resultado atualiza a execução anterior com
quatro seeds, na qual o intervalo contra a heurística incluía zero. Os dois
experimentos não foram combinados em uma amostra de onze seeds: há seeds
repetidas e condições de execução potencialmente diferentes.

Como o número de commits é igual entre as políticas, o custo por commit
preserva o ordenamento e as reduções relativas do custo por tentativa.
Acrescentar uma penalidade idêntica para cada aborto esperado também não
mudaria o vencedor de cada bloco pareado ou seu regret, embora alterasse
os custos absolutos e as porcentagens relativas.

### 3.3. Métricas operacionais separadas

| Política | Latência média (ms) | p95 (ms) | Redo médio (bytes) | Mensagens médias | Registros lógicos médios |
| --- | ---: | ---: | ---: | ---: | ---: |
| VFDT adaptativo | 119,384 | 299 | 2.351,304 | 8,333 | 6,161 |
| Heurística | 113,150 | 263 | 2.345,167 | 7,613 | 5,772 |
| 2PC fixo | 139,988 | 316 | 2.375,213 | 12,099 | 8,238 |
| Presumed Abort fixo | 124,794 | 316 | 2.367,504 | 9,743 | 6,321 |
| Presumed Commit fixo | 127,552 | 302 | 2.346,741 | 9,140 | 6,024 |

Esses valores foram calculados sobre as 15.050 escolhas de cada política nos
registros medidos. O p95 usa o percentil empírico do conjunto, não a média
dos percentis por seed. As latências no CSV são expressas em milissegundos
inteiros; o cálculo original do custo usa maior precisão temporal. Não foram
calculados intervalos adicionais para as métricas desta tabela.

A vantagem no custo composto não implica superioridade em todos os seus
componentes. Por exemplo, o adaptativo apresenta mais redo médio que o
Presumed Commit fixo, embora seu custo composto seja menor. Registros
lógicos do protocolo e atividade física de redo são métricas distintas.

### 3.4. Mudanças de fase

| Fase | Taxa de abortos | Custo adaptativo | Custo heurística | Escolhas adaptativas: 2PC / PA / PC |
| --- | ---: | ---: | ---: | --- |
| Baseline | 0,00% | 2,0005 | 2,0070 | 41 / 23 / 2.946 |
| Pico de latência | 0,00% | 4,2630 | 4,2184 | 138 / 1 / 2.871 |
| Pico de abortos | 88,04% | 1,8787 | 1,6604 | 609 / 2.184 / 217 |
| Pico combinado | 87,84% | 4,1138 | 3,6261 | 846 / 1.866 / 298 |
| Recuperação | 0,00% | 2,1604 | 2,0354 | 312 / 302 / 2.396 |

Cada fase reúne 3.010 observações, considerando as sete seeds. O adaptativo
altera suas escolhas conforme o regime: Presumed Commit predomina nas fases
sem abortos e Presumed Abort nas fases com abortos. Entretanto, a maior
diferença de custo contra a heurística ocorre no pico combinado.

O critério de adaptação exige vinte escolhas consecutivas iguais às do
oracle. No pico combinado, seis das sete seeds não atingiram esse critério
durante as 430 transações da fase. A seed restante apresentou atraso de 297
transações, contado até o início da sequência de vinte concordâncias. Na
recuperação, uma seed não atingiu o critério; as demais apresentaram atrasos
entre 99 e 381 transações. O marcador -1 significa critério não atingido na
janela observada, não ausência de qualquer mudança nas escolhas. Como o
oracle depende de custos medidos sujeitos a variação, esse critério também
é sensível à estabilidade das escolhas do próprio oracle.

### 3.5. Qualidade das previsões

| Protocolo | Alvo | Acurácia média | Macro-F1 médio |
| --- | --- | ---: | ---: |
| 2PC | Conclusão | 91,29% | 0,908 |
| 2PC | Latência | 78,31% | 0,729 |
| 2PC | Redo | 75,26% | 0,476 |
| Presumed Abort | Conclusão | 91,29% | 0,908 |
| Presumed Abort | Latência | 77,57% | 0,727 |
| Presumed Abort | Redo | 78,85% | 0,509 |
| Presumed Commit | Conclusão | 91,29% | 0,908 |
| Presumed Commit | Latência | 73,76% | 0,661 |
| Presumed Commit | Redo | 79,30% | 0,510 |

Os valores são médias das métricas por seed. A conclusão foi o alvo com
melhor desempenho. Os resultados idênticos dos três modelos de conclusão
são consistentes com os mesmos inputs e rótulos de conclusão dos candidatos.
A ausência de timeouts impede avaliar a capacidade preditiva para essa classe.
Na implementação, o macro-F1 desconsidera classes sem exemplos reais nem
preditos, e a acurácia balanceada desconsidera classes sem suporte real.

O redo apresenta acurácia relativamente alta, mas macro-F1 próximo de 0,5,
indicando desempenho desigual entre classes. Esses resultados justificam
investigar a classificação de custos e a conversão das distribuições
previstas em decisões. Eles não isolam, por si só, a causa da diferença de
custo frente à heurística.

O tempo médio registrado para inferência foi de 0,066 ms por transação lógica,
somando os candidatos, e o treinamento consumiu 0,925 ms. Esses tempos medem
as chamadas de predição e treinamento, não todo o custo de captura de
características, restauração ou infraestrutura do experimento.

## 4. Discussão e limitações

Os resultados sustentam que a seleção baseada em previsões pode reduzir o
custo em relação ao uso permanente de um dos protocolos avaliados. Contudo,
essa observação não demonstra que a complexidade adicional do aprendizado
seja necessária neste cenário: a heurística obteve custo inferior ao VFDT,
com intervalo pareado favorável nas sete seeds.

A carga apresenta regimes controlados nos quais o histórico de commits é
informativo para a seleção. É plausível que isso favoreça uma regra baseada
nesse histórico. A contribuição específica das árvores de latência e redo
precisa ser investigada por ablação, e a discrepância entre o escore ordinal
da política e a função de custo operacional merece avaliação separada.
Essas são hipóteses para novos experimentos, não causas já demonstradas.

Há limitações que condicionam a interpretação:

- A avaliação utiliza uma carga New-Order controlada e não o mix TPC-C completo.
- O pareamento fornece feedback de todos os candidatos. Uma implantação que
  executa apenas o escolhido exigirá tratamento explícito do feedback parcial.
- As fases possuem mudanças abruptas e votos injetados. Não representam toda
  a diversidade de contenção, falhas e mudanças graduais de um sistema real.
- Os contadores de redo são globais ao servidor. A restauração lógica não
  elimina efeitos de cache, checkpoints ou atividades internas do banco.
- A função de custo usa pesos definidos para esta execução. Ainda falta
  avaliar sensibilidade a outros pesos e preservar a análise por componente.
- Sete seeds oferecem replicação, mas ainda não completam a matriz planejada
  de dez seeds. As seeds 101-104 repetem identificadores da execução anterior.
- O registro informa a revisão `25c8fc43c79faf4d73fac453af9092aee8360e6b`
  com `git_dirty=true`. O hash sozinho não reconstrói as alterações locais
  utilizadas, incluindo a correção de gerenciamento de conexões.
- A duração total inclui os três candidatos e a restauração. Não deve ser
  convertida em throughput de uma política isolada em produção.

## 5. Síntese para a dissertação

A avaliação preliminar demonstrou a viabilidade de executar previsões online
e comparar decisões de seleção de protocolos em um ambiente com quatro
bancos MariaDB e transações XA. Nas sete seeds avaliadas, a política baseada
em VFDT reduziu o custo médio em 21,56% frente ao 2PC fixo, 6,87% frente ao
Presumed Abort fixo e 4,89% frente ao Presumed Commit fixo. Entretanto,
apresentou custo 6,42% superior ao da heurística baseada no histórico recente.
Os intervalos pareados de 95% excluíram zero nas duas direções correspondentes.

Esses achados delimitam a contribuição atual: o mecanismo de previsão e
seleção funciona e supera as alternativas fixas neste cenário, enquanto a
vantagem do aprendizado frente a uma regra simples ainda não se verifica.
As próximas etapas devem examinar a previsão de redo, a resposta às mudanças
de regime e o alinhamento entre o objetivo da política e o custo observado.

## Fontes e verificações desta análise

- [Configuração](../../build/experiments/mac-2h-20261002-091218/config.properties).
- [Log completo](../../build/experiments/mac-2h-20261002-091218/run.log).
- [Registros por transação](../../build/experiments/mac-2h-20261002-091218/transactions.csv).
- [Resumo por seed e fase](../../build/experiments/mac-2h-20261002-091218/seed-summary.csv).
- [Resumo agregado](../../build/experiments/mac-2h-20261002-091218/aggregate-summary.csv).
- [Comparações pareadas](../../build/experiments/mac-2h-20261002-091218/policy-comparisons.csv).
- [Métricas preditivas](../../build/experiments/mac-2h-20261002-091218/classifier-summary.csv).

A análise leu os CSVs sem alterar os originais, conferiu contagens,
duplicações, validade e resultados esperados, e recalculou os intervalos
pareados usando o mesmo valor crítico arredondado do executor (2,447).
Taxa de abortos, custo por commit e métricas operacionais agregadas foram
derivados dos registros por transação. As tabelas arredondam os valores
somente para apresentação. Este texto descreve o próprio experimento;
a fundamentação bibliográfica e sua integração aos demais capítulos ainda
devem ser desenvolvidas.
