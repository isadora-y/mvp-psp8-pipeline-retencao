# Da base bruta ao plano de retenção

**Um pipeline de dados em nuvem para retenção de clientes — e o custo metodológico de comparações mal construídas**

![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-tabelas%20persistidas-00ADD8)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.5.2-orange)
![Licença](https://img.shields.io/badge/dados-DbCL%20v1.0-lightgrey)

Projeto de Sistema de Produção 8 · Departamento de Engenharia de Produção · Universidade de Brasília

---

## O problema

Uma operadora de telecomunicações perde **26,5% da sua base** por ciclo. A área comercial
só descobre a intenção de cancelar quando o cliente liga para cancelar — momento em que a
retenção já é cara e pouco eficaz.

Este MVP testa uma hipótese:

> A partir exclusivamente do cadastro e da fatura, é possível operar um pipeline que
> entregue periodicamente ao time comercial uma lista priorizada de clientes em risco,
> com a ação de retenção indicada para cada um e o retorno esperado dessa ação.

O produto mínimo capaz de testar isso **não é um modelo preditivo**. É o percurso completo:
ingestão, perfilamento de qualidade, modelagem dimensional, carga persistida e análise.

### O que o pipeline entrega

Uma tabela Delta e um CSV com **452 clientes priorizados**, cada linha contendo o
identificador real do cliente, o risco estimado, a ação de retenção recomendada, o ganho
líquido esperado e **o nível de confiança daquela recomendação**.

### Três resultados que vale destacar

| | |
|---|---|
| O churn atinge 26,5% dos clientes mas **30,5% da receita** | quem cancela paga 21% a mais |
| Trocar F1 por valor esperado no critério de corte rende **+US$ 5.207** | mesmo modelo, mesmos dados |
| Uma das três ações prescritas é **extrapolação** | só 19% dos elegíveis têm contraparte real na base |

---

## Perguntas de negócio

| # | Pergunta | Decisão que sustenta | Situação |
|---|---|---|---|
| **P1** | Quanto da receita mensal está em risco e onde se concentra? | Onde focar a oferta comercial | Respondida |
| **P2** | Quais atributos explicam o cancelamento depois de controlados os demais? | Quais alavancas de produto priorizar | Respondida |
| **P3** | É possível identificar quem vai cancelar — e o ganho resiste a outras amostras? | Se vale operar um modelo | Respondida com ressalva |
| **P4** | As probabilidades do modelo sustentam contas em dinheiro? | Se o valor esperado calculado é crível | Respondida |
| **P5** | Qual lista e qual ação maximizam o retorno do orçamento? | Quanto investir e em quem | Respondida parcialmente |
| **P6** | O modelo trata grupos demográficos de forma equilibrada? | Risco reputacional e conformidade (LGPD) | Respondida |

As seis permanecem íntegras até o fim deste documento, **inclusive as respondidas de forma
negativa**. A seção [Autoavaliação](#autoavaliação) discute o atingimento de cada uma.

---

## Arquitetura

```
  Kaggle (CSV, 970.457 bytes)
       │  download manual → upload no Volume
       ▼
  /Volumes/workspace/default/churn/
       │
       ▼
  ┌─────────────┐  ingestão fiel: tudo texto, zero transformação
  │   BRONZE    │  bronze_telco_raw          + metadados de linhagem
  └─────────────┘
       │  perfilamento de qualidade em 6 dimensões
       ├────────────────────► qa_qualidade_atributos · qa_regras_consistencia
       ▼
  ┌─────────────┐  9 regras de transformação numeradas e justificadas
  │   SILVER    │  silver_assinatura
  └─────────────┘
       │
       ▼
  ┌─────────────┐  esquema estrela: 1 fato + 6 dimensões
  │    GOLD     │  fato_assinatura · dim_cliente · dim_demografia
  │             │  dim_contrato · dim_pagamento · dim_internet · dim_telefonia
  └─────────────┘  + meta_catalogo_dados (domínio e linhagem de 31 atributos)
       │
       ▼
  análise descritiva · diagnóstica · preditiva · prescritiva
       │
       ▼
  gold_plano_retencao (Delta)  +  plano_de_acao_retencao.csv (Volume)
```

São **13 tabelas Delta** registradas no Unity Catalog em `workspace.churn_mvp`. A
persistência é verificável depois do fim da sessão: as tabelas continuam consultáveis por
SQL. Prints em [`/evidencias`](#estrutura-do-repositório).

---

## Coleta

| Item | Registro |
|---|---|
| Fonte | Kaggle — [`blastchar/telco-customer-churn`](https://www.kaggle.com/datasets/blastchar/telco-customer-churn) (IBM Sample Data Sets) |
| Licença | Database Contents License (DbCL) v1.0 — uso e redistribuição mediante atribuição |
| Formato original | CSV único, UTF-8, separador vírgula, cabeçalho na 1ª linha |
| Volume | 7.043 registros × 21 atributos · 970.457 bytes |
| Técnica | Download manual e upload para Volume do Unity Catalog |
| Por que não a API | A API do Kaggle exige credencial pessoal, que não deve ser versionada no repositório |
| Dados pessoais | Não há identificadores diretos. O `customerID` é código sintético sem vínculo com pessoa natural |

O CSV bruto **não é versionado** neste repositório, em respeito aos termos da fonte. O
caminho de obtenção está em [Como executar](#como-executar).

---

## Qualidade dos dados

Seis dimensões avaliadas **atributo a atributo**, com o resultado persistido como tabela —
o perfil de qualidade é parte do produto, não um print perdido no notebook.

| Dimensão | O que se mede | Resultado |
|---|---|---|
| Completude | vazios e nulos por atributo | 99,84% — 11 vazios, todos em `TotalCharges` |
| Unicidade | duplicatas de chave e linhas repetidas | chave íntegra; 22 linhas idênticas sem o ID |
| Consistência | 6 regras cruzadas entre atributos | R1, R2, R3, R5 aprovadas; R4 com 59 ocorrências |
| Conformidade | aderência ao domínio declarado | 100% em todos os 21 atributos |
| Acurácia | coerência aritmética e plausibilidade | erro mediano de 2% em `TotalCharges` |
| **Atualidade** | existência de marcação temporal | **ausente — nenhuma data na base** |

> Declarar o domínio esperado de cada atributo é pré-requisito da conformidade: só se pode
> dizer que um valor é inválido contra um contrato explícito. Os domínios estão no notebook
> e no [catálogo de dados](catalogo/catalogo_de_dados.csv).

### O que a verificação encontrou

**R4 — 59 registros (0,8%)** têm `TotalCharges` divergindo mais de 20% do produto
`tenure × MonthlyCharges`, com erro mediano de 2%. **A leitura correta não é erro de dado:**
`MonthlyCharges` registra a tarifa vigente e `TotalCharges` o acumulado histórico. Quem teve
reajuste ou trocou de plano aparece como divergência. A consequência de modelagem é que os
dois atributos medem momentos diferentes e não podem ser tratados como redundantes — a regra
T7 cria o `ticket_medio_historico` justamente para separá-los.

**R6 — 22 linhas (0,31%)** são idênticas em todos os 20 atributos, diferindo apenas pelo
`customerID`. Não são duplicatas de carga: são clientes distintos com perfil igual, plausível
numa base de 7 mil registros e 20 atributos majoritariamente binários. **Mantidas** —
removê-las descartaria clientes reais. Registradas como "atenção", não como erro.

**R1, R2, R3, R5 — aprovadas sem exceção.** A base é internamente coerente. Note que
*demonstrar* a ausência de problema exige a mesma verificação que encontrá-lo.

### A limitação estrutural

**A base não possui nenhum carimbo de tempo** — sem data de referência, de adesão ou de
cancelamento. É um retrato sem data. Três consequências que condicionam todo o resto:

1. Não há como saber a que período o `Churn` se refere, nem o tamanho da janela de observação.
2. **Não é possível validação temporal** (treinar no passado, testar no futuro), que é o
   protocolo correto para previsão de churn. A divisão é obrigatoriamente aleatória.
3. Não dá para reconstruir a trajetória do cliente, apenas o estado final.

O desempenho reportado adiante mede capacidade de **reconhecer padrões numa população**, não
de prever o futuro a partir do passado.

---

## Modelagem

### Esquema estrela

Grão do fato: **um cliente, um retrato**. Cada dimensão agrupa atributos que descrevem o mesmo
conceito de negócio e mudam juntos.

| Tabela | Conceito | Linhas | Atributos |
|---|---|---|---|
| `fato_assinatura` | métricas da assinatura | 7.043 | 6 FKs + 7 métricas |
| `dim_cliente` | identidade | 7.043 | `customerID` |
| `dim_demografia` | perfil do titular | 16 | gênero, idoso, cônjuge, dependentes |
| `dim_contrato` | vínculo comercial | 6 | modalidade, fatura digital |
| `dim_pagamento` | instrumento financeiro | 4 | método, automático |
| `dim_internet` | pacote de banda larga | 129 | tecnologia + 6 serviços agregados |
| `dim_telefonia` | pacote de voz | 3 | telefonia, múltiplas linhas |

**`dim_internet` é uma dimensão-lixo** (*junk dimension*): sete atributos de baixa
cardinalidade que só fazem sentido juntos. Das **2.187 combinações teoricamente possíveis,
apenas 129 ocorrem**. Uma única chave substitui sete colunas no fato. Essa é a economia que
o modelo dimensional entrega: o fato fica estreito e as consultas de negócio ficam legíveis.

Optou-se por estrela e não snowflake porque nenhuma dimensão tem hierarquia que justifique
normalizar — quebrar `dim_internet` em sub-tabelas acrescentaria junções sem economizar espaço.

### Catálogo de dados

[`catalogo/catalogo_de_dados.csv`](catalogo/catalogo_de_dados.csv) documenta **31 atributos**
com tabela, tipo, descrição, domínio, obrigatoriedade e linhagem. O catálogo é uma **tabela
consultável** (`meta_catalogo_dados`), não um anexo, e também é aplicado como **comentário
nativo das colunas Delta** — quem abrir a tabela no Unity Catalog vê a documentação ao lado
de cada coluna.

Amostra:

| tabela | atributo | tipo | domínio | linhagem |
|---|---|---|---|---|
| `fato_assinatura` | `valor_acumulado` | decimal | [0; 8.684,80] | `bronze.TotalCharges` via T1 |
| `fato_assinatura` | `n_servicos` | inteiro | [0, 6] | derivado T9 |
| `dim_pagamento` | `pagamento_automatico` | categórico | Yes \| No | derivado T6 |
| `dim_demografia` | `idoso` | categórico | Yes \| No | `bronze.SeniorCitizen` via T3 |

### Linhagem

Registrada em três lugares: colunas `_ingerido_em` e `_arquivo_origem` na bronze; coluna
`linhagem` no catálogo, apontando cada atributo à origem e à regra `T` que o produziu; e nos
comentários nativos das colunas Delta.

---

## Carga

### As nove regras de transformação

Cada regra numerada, justificada e com o volume afetado
([`catalogo/regras_transformacao.csv`](catalogo/regras_transformacao.csv)). As quatro que
afetam a interpretação dos resultados:

| ID | Regra | Justificativa |
|---|---|---|
| **T1** | `TotalCharges` vazio recebe `0,00` | A regra R3 demonstrou que **todos** os 11 vazios são clientes com `tenure = 0`: ainda não houve faturamento, então zero é o valor correto, **não uma imputação** |
| **T4** | `"No internet service"` → `"No"` em 7 atributos | A ausência do serviço-pai já está em `InternetService`. Manter o terceiro nível duplica a informação em sete colunas e infla a dimensionalidade |
| **T7** | deriva `ticket_medio_historico` | Separa a tarifa média efetivamente paga da tarifa vigente — conceitos que a R4 mostrou serem distintos |
| **T8** | deriva `desvio_tarifario` | **Hipótese testada e rejeitada**: ver [P2](#p2--o-que-explica-o-cancelamento) |

### Validação

A corretude da carga é verificada por **três asserções que interrompem o notebook se falharem**:

```
linhas no fato        : 7043  (bronze: 7043)   ✓ sem perda nem duplicação
chaves distintas      : 7043                   ✓ grão de 1 linha por cliente preservado
chaves órfãs nas dims : 0                      ✓ integridade referencial

validação da carga: APROVADA
```

---

## Como executar

### 1. Obter a base

Baixe em [kaggle.com/datasets/blastchar/telco-customer-churn](https://www.kaggle.com/datasets/blastchar/telco-customer-churn)
e descompacte o ZIP. O arquivo é `WA_Fn-UseC_-Telco-Customer-Churn.csv` — não renomeie.

### 2. Criar o Volume e subir o CSV

**Catalog** → `workspace` → `default` → **Create** → **Volume**, com o nome `churn`. Abra o
Volume e clique em **Upload to this volume**. O caminho final deve ser
`/Volumes/workspace/default/churn/WA_Fn-UseC_-Telco-Customer-Churn.csv`.

### 3. Importar e rodar o notebook

**Workspace** → sua pasta de usuário → **⋮** → **Import** →
`notebooks/MVP_PSP8_pipeline_retencao.ipynb`. Abra, clique em **Connect** → **Serverless**,
e em **Run all**.

A primeira célula instala o `scikit-learn` e reinicia o Python — é normal e o notebook segue
sozinho. A execução completa leva **5 a 8 minutos**; a parte mais lenta é a análise de
estabilidade, que treina o modelo 25 vezes.

### 4. Conferir

1. A célula de validação da carga deve exibir **"validação da carga: APROVADA"**.
2. O `SHOW TABLES` final deve listar **13 tabelas**.
3. **Catalog** → `workspace` → `churn_mvp` mostra as tabelas persistidas, com os comentários
   do catálogo visíveis nas colunas.

O plano de ação fica no Volume `churn` como `plano_de_acao_retencao.csv`.

> Todo o código está comentado em português. As premissas financeiras estão isoladas em
> células no início das seções 8.4 e 8.5, para serem trocadas pelos valores de um caso real.

---

## Resultados

### Protocolo

Divisão treino/teste de 75/25, estratificada, com semente fixa, **antes de qualquer análise**.
Regras tradicionais, pontos de corte, modelos e limiares são construídos **exclusivamente com
o treino** (5.282 clientes). Ambas as abordagens são avaliadas no teste (1.761 clientes), que
nenhuma delas viu.

Compara-se três classificadores — regressão logística, floresta aleatória e *gradient boosting*
— contra uma **regra de pontuação de risco de cinco fatores** derivada das tabelas cruzadas do
treino, que é o equivalente ao que um analista produz em planilha.

### P1 — Onde está a receita em risco

O cancelamento atinge **26,5% dos clientes e 30,5% da receita**: US$ 139.131 de US$ 456.117
mensais. Quem cancela paga **US$ 74,44 contra US$ 61,27** de quem permanece — 21% a mais.

| Perfil | Clientes | Taxa de churn | Receita mensal em risco |
|---|---:|---:|---:|
| Contrato mensal | 3.875 | 42,7% | US$ 120.847 |
| Internet por fibra | 3.096 | 41,9% | US$ 114.300 |
| Pagamento por cheque eletrônico | 2.365 | 45,3% | US$ 84.289 |
| Contrato de dois anos | 1.695 | 2,8% | US$ 4.165 |

São em larga medida **os mesmos clientes vistos por três ângulos**. A perda é seletiva e recai
sobre o cliente de maior valor, o que eleva o teto do investimento racional em retenção.

### P2 — O que explica o cancelamento

Duas leituras: a **tradicional** mede a amplitude da taxa de churn entre categorias de cada
variável, uma de cada vez; a do **modelo** usa importância por permutação, capturando o efeito
já descontado dos demais atributos.

As duas concordam no essencial — **tempo de casa** e **modalidade de contrato** lideram. A
divergência está no meio da tabela, e é onde a leitura univariada erra de forma sistemática:

| Atributo | Posição tradicional | Posição no modelo | Deslocamento |
|---|---:|---:|---:|
| `PaymentMethod` | 4ª | 12ª | **−8** |
| `valor_mensal` | 6ª | 15ª | **−9** |
| `ticket_medio_historico` | 7ª | 16ª | **−9** |
| `StreamingMovies` | 19ª | 6ª | **+13** |
| `MultipleLines` | 21ª | 9ª | **+12** |
| `StreamingTV` | 18ª | 7ª | **+11** |

Cheque eletrônico exibe a maior taxa isolada de cancelamento (45,3%), mas quem paga assim é
majoritariamente cliente mensal de fibra recém-chegado. Controlado por isso, o método de
pagamento explica pouco — **a tabela cruzada estava creditando a ele o efeito do contrato**.
Uma campanha desenhada por essa leitura investiria em mudar o método de pagamento acreditando
num efeito que, isolado, é pequeno.

> **Hipótese T8 — rejeitada.** O atributo `desvio_tarifario` foi criado para testar se reajuste
> recente antecede cancelamento. Ficou em **22º lugar entre 23**, com importância de 0,0004 —
> indistinguível de zero. Nesta base, aumento recente de tarifa não prediz cancelamento.
> **O resultado negativo fica registrado e o atributo permanece no catálogo**, porque a
> tentativa documentada é informação útil para quem continuar o trabalho.

### Segmentação: a comparação precisa de um adversário honesto

| Segmentação | Variáveis usadas | Separação de risco |
|---|---:|---:|
| Faixas de tempo de casa | 1 | 0,1589 |
| K-Means (*k* = 6) | 19 | 0,1949 |
| **Contrato × tempo de casa** | **2** | **0,1969** |

**Uma tabela cruzada de duas variáveis, montável em planilha, supera o K-Means com 19
variáveis.** A silhueta de 0,134 confirma a ausência de agrupamentos naturais: o algoritmo
está particionando um espaço contínuo, não descobrindo estrutura.

Isso não desqualifica a clusterização — **qualifica a comparação**. O segmento crítico
(mensal com menos de um ano, 52,4% de cancelamento) é encontrado pelas duas abordagens, mas
apenas a versão de negócio é explicável ao time comercial sem intermediação.

### P4 — As probabilidades sustentam contas em dinheiro?

Um modelo pode **ordenar** risco muito bem e ainda estar errado no **nível** — dizer 30%
quando o real é 45%. Como todo o cálculo prescritivo multiplica probabilidade por receita,
essa verificação não é refinamento, é pré-requisito.

| Modelo | Brier | ECE bruto | ECE após recalibração isotônica |
|---|---:|---:|---:|
| Regressão logística | 0,1358 | 0,0146 | 0,0116 |
| Random Forest | 0,1360 | 0,0146 | 0,0104 |
| Gradient Boosting | 0,1368 | 0,0160 | 0,0104 |

**A suspeita não se confirmou.** O erro de calibração fica em **1,5% já sem tratamento**:
quando o modelo estima 30%, o cancelamento observado fica entre 28,5% e 31,5%. A recalibração
isotônica traz melhora marginal e foi mantida por ter custo nulo.

O ponto metodológico permanece: versões anteriores deste MVP empregaram essas probabilidades
em contas de valor esperado **sem nunca verificar**. O resultado foi favorável, mas é a
verificação que converte suposição em premissa documentada.

### P3 — Previsão, critério de corte e estabilidade

| Abordagem | Critério do corte | F1 | AUC | Abordados | Lucro no teste |
|---|---|---:|---:|---:|---:|
| Regra tradicional | F1 | 0,602 | 0,811 | 822 | US$ 78.197 |
| Regra tradicional | Lucro | 0,543 | 0,811 | 1.157 | US$ 81.518 |
| Regressão logística | F1 | 0,629 | 0,847 | 678 | US$ 79.952 |
| **Regressão logística** | **Lucro** | 0,599 | 0,847 | 968 | **US$ 85.159** |

O teste de McNemar rejeita a equivalência entre regra e modelo em todos os casos
(*p* < 10⁻¹⁰). Para a regressão logística, **o modelo acerta sozinho 210 clientes contra 43
da regra**.

**O critério de corte tem efeito material.** O limiar cai de 0,31 (F1) para 0,13 (lucro) e o
resultado financeiro sobe de US$ 79.952 para US$ 85.159 — **US$ 5.207 adicionais com o mesmo
modelo e os mesmos dados**, apenas por decidir com o critério correto. Errar um churner custa
12 meses de mensalidade; errar um cliente fiel custa US$ 30 de contato. Os erros não são
simétricos, e **F1 os trata com peso igual**. O F1 cai de 0,629 para 0,599, o que é esperado e
irrelevante quando o objetivo é lucro.

> Este é o argumento de **Verbeke et al. (2012)**: modelos de cancelamento são rotineiramente
> selecionados por métricas estatísticas, o que resulta em escolha subótima do ponto de vista
> do lucro.

#### A vantagem sobrevive a outras amostras?

O experimento completo — construção da regra, treino, calibração e escolha de limiares — foi
repetido sobre **25 divisões independentes**.

| Métrica | Média | IC 95% | Vence em |
|---|---:|:---:|---:|
| Δ AUC | +0,035 | [+0,022 ; +0,048] | **100%** |
| Δ F1 | +0,058 | [+0,040 ; +0,074] | **100%** |
| Δ Lucro | +US$ 2.693 | **[−952 ; +4.368]** | 96% |

**A superioridade estatística é sólida; a financeira é provável, não garantida.** Os dois
primeiros intervalos não cruzam zero. O terceiro cruza: em aproximadamente uma campanha a
cada 25, a abordagem guiada por modelo renderia menos que a guiada por regra, porque o
resultado monetário depende de *quais* clientes caem na lista e de quanto pagam — variabilidade
maior que a da capacidade de ordenar risco.

A promessa defensável é "o modelo ordena risco melhor, de forma consistente". A promessa
"o modelo garante mais lucro todo trimestre" não se sustenta nos dados.

### P5 — Prescrição

#### O cardápio de ações

| Ação | Elegibilidade | O que muda | Custo |
|---|---|---|---:|
| Contrato anual com desconto | contrato mensal | `Contract → One year` | US$ 60 |
| Migração para débito automático | cheque eletrônico ou boleto | `PaymentMethod → Credit card (automatic)` | US$ 15 |
| Suporte técnico gratuito | tem internet, não tem suporte | `TechSupport → Yes` | US$ 35 |

#### A simulação "e se?" tem suporte empírico?

Trocar `Contract` de "mensal" para "anual" na planilha é trivial. Mas o modelo aprendeu com
clientes **reais** de contrato anual, que diferem dos mensais em muitas outras dimensões. Se
o cliente sintético não se parece com ninguém da base, a previsão é **extrapolação**, não
estimativa.

O diagnóstico usa um modelo de propensão: dado tudo **menos** o atributo manipulado e seus
derivados, quão fácil é distinguir quem está na origem de quem está no destino?

| Ação | AUC origem × destino | Clientes na região de sobreposição | Confiança |
|---|---:|---:|---|
| Débito automático | 0,747 | **98,3%** | alta |
| Contrato anual | 0,896 | 75,3% | média |
| **Suporte técnico** | **0,998** | **19,0%** | **baixa — extrapolação** |

Quem possui suporte técnico quase sempre possui também segurança online, backup e proteção de
equipamento — contrata o pacote inteiro. Ativar isoladamente o suporte para quem não tem
nenhum agregado produz um perfil **praticamente inexistente na base**. O número gerado depende
do modelo, não dos dados, exatamente na acepção de **King e Zeng (2006)**.

Das 452 recomendações do plano, **17 usam essa ação** (US$ 518 de US$ 20.318). Foram
**mantidas e marcadas** — a coluna `confianca_simulacao` deixa a decisão com quem opera a
campanha.

#### Os planos

| Plano (orçamento de US$ 15.000) | Clientes | Ganho líquido | ROI |
|---|---:|---:|---:|
| Tradicional — oferta única *(adversário fraco)* | 250 | US$ 8.851 | 59% |
| Tradicional — regra por perfil *(adversário forte)* | 646 | US$ 13.290 | 89% |
| **Otimizada por valor esperado** | **452** | **US$ 20.318** | **135%** |

Contra a oferta única, a otimização rende **+130%**. Contra a regra por perfil — que qualquer
analista escreveria em dez minutos — rende **+53%**. **A distância entre esses dois percentuais
quantifica o quanto uma comparação mal construída infla o resultado atribuído ao método.**

#### Um achado não antecipado

| Orçamento | Oferta única | Regra por perfil | Otimizada |
|---:|---:|---:|---:|
| US$ 5.000 | US$ 3.846 | US$ 9.724 | US$ 11.399 |
| US$ 10.000 | US$ 6.807 | **US$ 13.300** ← pico | US$ 16.547 |
| US$ 15.000 | US$ 8.851 | US$ 13.290 | US$ 20.318 |
| US$ 25.000 | **US$ 11.988** ← pico | US$ 12.929 | **US$ 23.131** |
| US$ 40.000 | US$ 8.516 | US$ 10.579 | US$ 23.131 |

**Os dois planos tradicionais destroem valor quando o orçamento cresce.** A regra por perfil
atinge o máximo em US$ 10 mil e cai 20% até os US$ 40 mil; a oferta única atinge o máximo em
US$ 25 mil e cai 29% depois. Ambas ordenam candidatos **por risco e não por retorno**, e
**nenhuma tem critério de parada** — continuam gastando em clientes cujo ganho esperado já é
negativo. O plano otimizado estabiliza em US$ 23.131 porque interrompe a alocação quando o
valor esperado se anula. **Saber quando não agir é parte do valor da abordagem**, e não aparece
em nenhuma métrica de classificação.

#### Mix do plano final

| Ação | Clientes | Confiança |
|---|---:|---|
| Débito automático | 260 | alta |
| Contrato anual | 175 | média |
| Suporte técnico | 17 | baixa |

A ação mais recomendada é a mais barata, a de maior redução de risco para quem paga por
cheque eletrônico — e, convenientemente, a de maior suporte empírico.

### P6 — Equidade entre grupos

| Atributo | Grupo | n | Churn real | Taxa de seleção | Recall | AUC |
|---|---|---:|---:|---:|---:|---:|
| Gênero | Feminino | 863 | 27,8% | 55,7% | 0,925 | 0,839 |
| Gênero | Masculino | 898 | 25,3% | 54,2% | 0,916 | 0,853 |
| Idoso | Não | 1.475 | 23,4% | 50,0% | 0,899 | 0,847 |
| **Idoso** | **Sim** | **286** | **42,7%** | **80,4%** | 0,984 | **0,798** |
| Cônjuge | Não | 892 | 33,6% | 69,2% | 0,953 | 0,820 |
| Cônjuge | Sim | 869 | 19,2% | 40,4% | 0,862 | 0,860 |
| Dependentes | Não | 1.220 | 31,1% | 65,4% | 0,947 | 0,831 |
| **Dependentes** | **Sim** | **541** | **16,1%** | **31,4%** | **0,805** | 0,860 |

- **Gênero: nenhum efeito.** Δ*recall* de 0,009. O atributo não carrega sinal e **a recomendação
  é removê-lo** — atributo sem poder preditivo e com potencial discriminatório apenas acrescenta
  risco.
- **Idosos: selecionados em excesso e classificados pior.** 80,4% são sinalizados contra 50,0%
  dos demais. Parte é legítima (churn real de 42,7% contra 23,4%), mas o **AUC cai para 0,798**
  contra 0,847: o modelo **ordena pior justamente o grupo que mais aborda**. Com 286 idosos no
  teste há pouco dado para aprender o padrão.
- **Dependentes: maior disparidade de *recall* (0,143).** O modelo captura 94,7% dos churners
  sem dependentes e 80,5% dos com — **um em cada cinco passa despercebido**.

Nenhuma disparidade impede o uso, mas duas exigem registro. Em operação, pediriam limiares por
segmento ou coleta adicional nesses grupos.

---

## Discussão

**O churn é um problema de margem, não de contagem.** Perde-se 30,5% do faturamento com 26,5%
dos clientes porque quem sai paga 21% a mais. Isso eleva o teto do que faz sentido gastar em
retenção e explica por que o ROI do plano otimizado chega a 135%.

**A leitura univariada erra de forma sistemática, e erra para cima.** Forma de pagamento e
mensalidade parecem causas e são consequências de contrato e tecnologia. O modelo não substitui
a leitura de negócio — foi ela que gerou as hipóteses e o cardápio de ações — mas corrige a
atribuição de crédito.

**A vantagem do modelo é real na ordenação e incerta no dinheiro.** Δ AUC de +0,035 vencendo em
100% das amostras é um resultado forte. Δ Lucro com intervalo cruzando zero é um resultado
honesto.

**A verificação de calibração mudou o status da premissa, não o número.** O ECE de 1,5% autoriza
o uso das probabilidades em conta financeira. A diferença entre este trabalho e a versão
anterior não é o resultado — é saber que ele vale.

**O ganho prescritivo encolhe de 130% para 53% quando o adversário é justo.** Esse é o achado
metodológico central, e é transferível: comparações entre abordagem tradicional e métodos
estatísticos são frequentemente montadas com a primeira em versão deliberadamente simplificada
— faixas de uma variável contra clusterização multivariada, oferta única contra otimização
individualizada. **O resultado mede a fraqueza do adversário escolhido, não a força do método
proposto.** Quando o adversário é construído com o cuidado que um analista de fato empregaria,
a vantagem sobrevive, mas com menos da metade da magnitude.

**A simulação prescritiva não tem suporte uniforme.** Das três ações, uma é confiável, uma é
aceitável e uma é extrapolação. Sem a checagem de sobreposição, as três sairiam no relatório
com a mesma aparência de precisão.

**A limitação que atravessa tudo** é a ausência de marcação temporal, que impede validação
temporal. O que se mede é capacidade de reconhecer padrões numa população, não de prever o
futuro. Nenhum refinamento de modelo resolve isso — só dado melhor resolve.

---

## Autoavaliação

### Quais perguntas foram respondidas e quais não?

| | Pergunta | Situação |
|---|---|---|
| P1 | Receita em risco | **Respondida.** US$ 139.131/mês, 30,5% do faturamento, concentrada em mensal + fibra + cheque eletrônico |
| P2 | Fatores explicativos | **Respondida.** Contrato e tempo de casa lideram; pagamento e mensalidade são consequência. A hipótese T8 foi testada e **rejeitada** |
| P3 | Previsibilidade | **Respondida com ressalva.** Ganho de AUC sólido (100% das amostras); ganho financeiro não garantido (IC cruza zero) |
| P4 | Confiabilidade das probabilidades | **Respondida.** ECE de 1,5%; utilizáveis em cálculo financeiro |
| P5 | Melhor alocação do orçamento | **Respondida parcialmente.** O plano é mensurável, mas 17 das 452 recomendações apoiam-se em contrafactual sem suporte |
| P6 | Equidade | **Respondida.** Sem viés de gênero; disparidade relevante para idosos e clientes com dependentes |

Nenhuma pergunta ficou sem resposta, mas **P5 só é respondida de forma condicional**: o valor
esperado é estimativa de associação, não de causa. A resposta definitiva exige experimento,
não dado observacional.

### Que limitações dos dados condicionaram os resultados?

1. **Ausência de marcação temporal** — a mais séria. Impede validação temporal, cálculo de
   janela de observação e análise de sobrevivência, que é o método correto para churn.
2. **Snapshot único** — sem histórico, não há trajetória, apenas o estado final do cliente.
3. **Ausência de dados comportamentais** — chamados abertos, reclamações, qualidade de sinal,
   consumo de dados. São justamente os sinais que antecedem o cancelamento, e nenhum está na base.
4. **Base de um único setor e uma única operadora** — nada autoriza generalizar os coeficientes.
5. **Sobreposição limitada para contrafactuais** — restringe a confiabilidade da camada
   prescritiva, como medido em P5.
6. **Premissas financeiras arbitradas** — custo de contato, taxa de sucesso e horizonte são
   ilustrativos. Comparam abordagens entre si; não são previsão de receita.

### Que decisões técnicas eu tomaria de outra forma?

1. **Começaria pela modelagem dimensional, não pelo modelo preditivo.** Na primeira versão a
   análise veio antes da estrutura, e o `customerID` foi descartado na limpeza — o que
   **inutilizou o entregável final**: o plano exportado identificava clientes pela posição na
   tabela, e o time comercial não teria como localizar ninguém. A chave natural é requisito de
   produto, não detalhe de limpeza.
2. **Teria escolhido o limiar por valor esperado desde o início.** Otimizar F1 e depois converter
   em dinheiro é incoerente, e custou US$ 5.207 no mesmo modelo.
3. **Teria construído o adversário tradicional na sua melhor versão.** Comparar contra uma versão
   fraca produziu uma conclusão — "a IA vence em 5 de 5 indicadores" — que não sobreviveu ao
   teste justo na segmentação.
4. **Teria verificado o suporte do contrafactual antes de publicar números prescritivos**, não
   depois.
5. **Usaria `class_weight` com mais cuidado.** Reponderar classes distorce a probabilidade e
   teria atrapalhado a calibração, que aqui é pré-requisito do cálculo financeiro.

### Que extensões transformariam este MVP em solução de uso contínuo?

| Prioridade | Extensão | Por quê |
|---:|---|---|
| 1 | **Teste A/B das ações** (tratado × controle) | Única forma de transformar associação em efeito causal e validar os US$ 20.318. A extensão prioritária é experimental, não algorítmica |
| 2 | **Ingestão incremental com carimbo de tempo** (Auto Loader + Delta MERGE) | Habilita validação temporal e dimensão de histórico (SCD tipo 2) |
| 3 | **Modelos de *uplift*** | Estimam o efeito incremental por cliente, em vez da diferença de risco simulada |
| 4 | **Monitoramento de deriva** sobre `qa_qualidade_atributos` | O perfil de qualidade já é tabela; falta compará-lo entre cargas e disparar alerta |
| 5 | **Explicação individual (SHAP)** no plano de ação | O atendente precisa saber *por que* aquele cliente está na lista |
| 6 | **Orquestração em Databricks Workflows** | O pipeline roda por execução manual; produção exige agendamento, retentativa e alerta de falha |
| 7 | **Limiares por segmento** | Corrige a disparidade de *recall* medida em P6 |

---

## Ética, licenciamento e proteção de dados

**Licença dos dados.** Database Contents License (DbCL) v1.0, que autoriza uso, modificação e
redistribuição mediante atribuição. O repositório versiona o código e o notebook; **o CSV bruto
não é redistribuído** — o caminho de obtenção está documentado.

**Licença do código.** MIT, em [`LICENSE`](LICENSE).

**Dados pessoais.** A base não contém identificadores diretos. O `customerID` é código sintético
sem vínculo com pessoa natural, e nenhum atributo permite reidentificação. Não se aplica
tratamento adicional sob a Lei 13.709/2018.

**Decisão automatizada.** Ainda que anonimizada, a aplicação ilustra uma decisão automatizada
que afeta clientes: quem recebe oferta e qual. O **art. 20 da LGPD** assegura revisão. Por isso
o plano exportado carrega risco, ação, redução estimada **e o nível de confiança da simulação**,
e por isso a seção P6 audita disparidade entre grupos. Um sistema que recomenda sem expor sua
base de decisão não é auditável.

**Atributos sensíveis.** Gênero entra no modelo e demonstra-se sem efeito (Δ*recall* de 0,009).
Em operação real, a recomendação é **removê-lo**.

---

## Estrutura do repositório

```
README.md                              ← este documento
/notebooks
    MVP_PSP8_pipeline_retencao.ipynb   ← pipeline completo, comentado em português
/catalogo
    catalogo_de_dados.csv              ← 31 atributos com domínio e linhagem
    regras_transformacao.csv           ← as 9 regras T
    qualidade_por_atributo.csv         ← perfil de qualidade das 21 colunas
    diagrama_estrela.png               ← modelo dimensional
/evidencias
    01_volume_com_csv.png
    02_notebook_conectado.png
    03_validacao_carga_aprovada.png
    04_show_tables_13.png
    05_catalog_churn_mvp.png           ← prova de persistência na nuvem
    06_comentarios_colunas.png
/paper
    Paper_PSP8.pdf                     ← short paper, 6 páginas
    main.tex
/saidas
    plano_de_acao_retencao.csv         ← 452 clientes priorizados
LICENSE
```

---

## Tecnologias

**Databricks** (Free Edition, Serverless) · **Delta Lake** e **Unity Catalog** para persistência
e governança · **PySpark** para ingestão, qualidade e ETL · **pandas** e **NumPy** ·
**scikit-learn** para modelos, calibração, importância por permutação e K-Means · **SciPy** para
o teste de McNemar · **Matplotlib** para visualização · **LaTeX** para o paper.

A base cabe em memória (7 mil linhas), então a análise estatística roda em pandas/scikit-learn
sobre a camada Gold — distribuir o treinamento acrescentaria complexidade sem ganho.

---

## Referências

- KIMBALL, R.; ROSS, M. *The Data Warehouse Toolkit*. 3. ed. Wiley, 2013.
- BATINI, C.; SCANNAPIECO, M. *Data Quality: Concepts, Methodologies and Techniques*. Springer, 2006.
- VERBEKE, W. et al. New insights into churn prediction in the telecommunication sector: a profit driven data mining approach. *European Journal of Operational Research*, v. 218, n. 1, p. 211–229, 2012.
- KING, G.; ZENG, L. The dangers of extreme counterfactuals. *Political Analysis*, v. 14, n. 2, p. 131–159, 2006.
- NICULESCU-MIZIL, A.; CARUANA, R. Predicting good probabilities with supervised learning. *ICML*, 2005.
- PROVOST, F.; FAWCETT, T. *Data Science for Business*. O'Reilly, 2013.
- HARDT, M.; PRICE, E.; SREBRO, N. Equality of opportunity in supervised learning. *NeurIPS*, 2016.

A lista completa está no [paper](paper/Paper_PSP8.pdf).

---

## Autoria

| | |
|---|---|
| **Aluna** | Isadora Messenberg Guimarães Macêdo Rebello |
| **Matrícula** | 231013387 |
| **Disciplina** | Projeto de Sistema de Produção 8 |
| **Professor** | André Luiz Marques Serrano |
| **Instituição** | Universidade de Brasília — Departamento de Engenharia de Produção |
| **Semestre** | 2026.2 |

---

*Base de dados: Telco Customer Churn (IBM Sample Data Sets), disponibilizada no Kaggle por blastchar sob DbCL v1.0.*
