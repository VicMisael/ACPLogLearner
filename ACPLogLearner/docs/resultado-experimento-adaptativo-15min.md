# Resultado preliminar da seleção adaptativa — execução de 15 minutos

Data: 2026-09-17

## Objetivo

Validar em escala intermediária o experimento pareado de seleção adaptativa de
protocolos antes da matriz final com dez seeds. Este teste avalia custo e regret
reais; não usa acurácia de classificação como substituto para desempenho do
protocolo.

## Configuração

- seed: `101`;
- aquecimento: 100 transações lógicas;
- fases: baseline, pico de latência, pico de abortos, pico combinado e
  recuperação;
- transações medidas por fase: 430;
- transações lógicas medidas: 2.150;
- candidatos executados por transação: 2PC, presumed abort e presumed commit;
- execuções XA totais, incluindo aquecimento: 6.750;
- duração: `15m14s`;
- pico de latência injetada: 50 ms;
- piso de aborto nas fases afetadas: 0,70;
- janela histórica: 30 transações;
- critério de adaptação: 20 concordâncias consecutivas com o oracle.

Os três candidatos de cada especificação foram executados em ordem pseudoaleatória
reprodutível. O estado lógico de distrito, estoque e New-Order foi restaurado e
verificado após cada candidato. A presença de qualquer branch em `XA RECOVER`
interromperia o experimento.

## Função de custo

O custo é adimensional e, com os pesos padrão, corresponde a:

```text
latência_ms / 100
+ redo_bytes / 10.000
+ mensagens / 10
+ registros_lógicos_do_protocolo / 10
+ penalidades de timeout ou resultado inválido
```

O oracle é o candidato válido de menor custo para a mesma especificação. Regret
é a diferença entre o custo da seleção e o custo do oracle. Um aborto esperado
pelos votos não é penalizado; resultado incorreto, timeout, reset de contador ou
redo obrigatório indisponível invalidam a tentativa.

## Resultado global

| Política | Custo médio | Regret médio | Concordância com oracle |
| --- | ---: | ---: | ---: |
| Oracle | 2,404 | 0,000 | 1,0000 |
| Heurística estática | 2,463 | 0,058 | 0,9098 |
| VFDT adaptativo | 2,515 | 0,111 | 0,8433 |
| Presumed commit fixo | 2,766 | 0,361 | 0,6260 |
| Presumed abort fixo | 2,837 | 0,433 | 0,3619 |
| Aleatório | 2,979 | 0,575 | 0,3437 |
| 2PC fixo | 3,383 | 0,979 | 0,0121 |

O VFDT adaptativo reduziu o custo em:

- 25,66% contra 2PC fixo;
- 11,36% contra presumed abort fixo;
- 9,07% contra presumed commit fixo;
- 15,59% contra seleção aleatória.

A heurística estática ainda foi 2,11% melhor que o VFDT. Esse resultado não deve
ser ocultado: a heurística é o baseline competitivo que o modelo adaptativo
precisa superar ou justificar por outras propriedades.

Não houve trial inválido, branch XA pendente ou falha de restauração. A latência
p95 da política adaptativa foi 221 ms.

## Comportamento por fase

| Fase | 2PC | Presumed abort | Presumed commit | Concordância com oracle | Regret médio |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 2 | 2 | 426 | 0,9674 | 0,0174 |
| Pico de latência | 1 | 0 | 429 | 0,9698 | 0,0197 |
| Pico de abortos | 50 | 366 | 14 | 0,7698 | 0,1266 |
| Pico combinado | 16 | 367 | 47 | 0,7814 | 0,2320 |
| Recuperação | 45 | 52 | 333 | 0,7279 | 0,1569 |

As escolhas mudaram na direção esperada: presumed commit dominou nas fases de
commit, presumed abort dominou nos picos de aborto e presumed commit voltou a
ser majoritário na recuperação. Isso é evidência comportamental de adaptação,
em vez de simples escolha de um protocolo globalmente dominante.

Os atrasos até 20 concordâncias consecutivas com o oracle foram 33 transações no
baseline, zero no pico de latência, 218 no pico de abortos, 223 no pico combinado
e 78 na recuperação. A adaptação aos regimes de aborto ainda é lenta e explica
parte da diferença para a heurística.

## Qualidade dos modelos

Os heads de conclusão dos três modelos específicos por protocolo obtiveram
acurácia global de `0,9214`, macro-F1 de `0,9170` e balanced accuracy de
`0,9361`. Para 2PC, a acurácia foi `0,7740` no head de latência e `0,7879` no
head de redo. Os resultados completos por protocolo e fase permanecem no
artefato `classifier-summary.csv` gerado pela execução.

## Interpretação e limitações

O resultado é promissor porque a política adaptativa superou todos os protocolos
fixos, mudou suas escolhas entre regimes e manteve correção em 6.750 execuções
XA reais. Entretanto, esta execução usa apenas uma seed. Os intervalos de
confiança gerados com `n=1` colapsam no próprio ponto e não possuem valor
inferencial.

Antes de formular a conclusão da dissertação, é necessário:

1. repetir a matriz com as dez seeds configuradas;
2. calcular intervalos pareados sobre resultados independentes por seed;
3. repetir a análise com pesos e escalas de custo previamente justificados;
4. analisar por que a heurística reage mais rapidamente aos picos de aborto;
5. reportar também latência, redo, mensagens e logs separadamente.

O throughput observado revisa a estimativa da matriz completa: 153.000 execuções
devem levar aproximadamente `5h45`, desde que as condições do host permaneçam
semelhantes.
