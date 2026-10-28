# Análise RFM e Segmentação de Clientes: E-commerce Olist

Pipeline de dados e análise de segmentação RFM construídos no **Databricks** com **Python, PySpark e SQL**, usando dados reais de e-commerce brasileiro. O projeto faz parte do meu portfólio como Analista de Dados.

---

## 📌 Sobre a análise RFM

A análise RFM segmenta clientes a partir de três dimensões do comportamento de compra. **Recência** é há quanto tempo o cliente comprou pela última vez, **Frequência** é quantas vezes ele comprou e **Valor Monetário** é quanto ele gastou no total. Cada cliente recebe uma pontuação em cada dimensão, e isso permite agrupá-los em perfis como clientes fiéis, novos de alto valor ou em risco de abandono.
\
\
Para um e-commerce, essa análise ajuda a direcionar campanhas de retenção, reativação e incentivo à recompra com mais precisão, otimizando o investimento em marketing e aumentando o valor de cada cliente ao longo do tempo.

## 🎯 Objetivos

- Construir um pipeline de dados em camadas (**bronze → silver → gold**) totalmente no Databricks
- Aplicar validações de qualidade de dados (data quality) na camada silver
- Segmentar os clientes por RFM e traduzir os segmentos em leituras de negócio
- Analisar o comportamento de compra por estado (UF)

## 🗂️ Dataset

**[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** (Kaggle): pedidos reais feitos entre 2016 e 2018.

Arquivos utilizados:

| Arquivo                            | Conteúdo                                          |
| ---------------------------------- | ------------------------------------------------- |
| `olist_orders_dataset.csv`         | Pedidos, status e datas de compra                 |
| `olist_customers_dataset.csv`      | Clientes e localização (UF)                       |
| `olist_order_payments_dataset.csv` | Pagamentos, valores, parcelas e meio de pagamento |

## 🏗️ Arquitetura do pipeline

```
 RAW (CSV do Kaggle)                BRONZE              SILVER                    GOLD
 /Volumes/workspace/olist/raw       workspace.olist     workspace.olist           workspace.olist
 ─────────────────────────────────────────────────────────────────────────────────────────────────

 olist_orders_dataset.csv         ──► bronze_orders     ┐
 olist_customers_dataset.csv      ──► bronze_customers  ┼──►  silver_pedidos ──┬──►  gold_rfm
 olist_order_payments_dataset.csv ──► bronze_payments   ┘                      ├──►  gold_clientes_resumo
                                                                               ├──►  gold_clientes_uf
                                                                               └──►  dq_resultados
```

| Camada           | O que acontece                                                                                                                                                                       |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Bronze**       | Ingestão dos CSVs originais como tabelas Delta, com coluna de auditoria `_ingestao_em`                                                                                               |
| **Silver**       | Remoção de pedidos duplicados, filtro de pedidos entregues (`delivered`), tipagem, exclusão de pagamentos com valor ≤ 0, agregação dos pagamentos por pedido e join entre as tabelas |
| **Data quality** | Checagens de `order_id` duplicado, valor nulo e data nula, registradas na tabela `dq_resultados`                                                                                     |
| **Gold**         | Cálculo do RFM e da segmentação, além das visões consolidadas geral e por UF                                                                                                         |

> **Decisão de modelagem:** o campo `customer_id` é gerado por pedido no dataset da Olist. Por isso, o cliente é identificado pelo `customer_unique_id`.

## 🧮 Metodologia RFM

- **Data de referência:** última data de compra do dataset, já que os dados são históricos (2016–2018)
- **Recência:** dias entre a última compra do cliente e a data de referência
- **Frequência:** número de pedidos distintos
- **Monetário:** soma do valor pago
- **Scores:** recência e monetário divididos em quintis (1 a 5) com `ntile(5)`. Quanto maior o score, melhor o cliente na dimensão

> **Por que a frequência não virou score?** Cerca de 97% dos clientes fizeram apenas um pedido. Dividir a frequência em quintis não diferenciaria os clientes, então ela foi usada apenas para identificar os recorrentes.

### Regras de segmentação

| Segmento                | Regra                  | Leitura de negócio                                                    |
| ----------------------- | ---------------------- | --------------------------------------------------------------------- |
| **Recorrentes ativos**  | Frequência > 1 e R ≥ 4 | Cliente fiel: comprou mais de uma vez e recentemente                  |
| **Novos de alto valor** | R ≥ 4 e M ≥ 4          | Comprou uma vez, recentemente e com valor alto: incentivar a recompra |
| **Alto valor em risco** | R ≤ 2 e M ≥ 4          | Gastou muito, mas está há tempo sem comprar: reativar                 |
| **Inativos**            | R ≤ 2                  | Recência e valor baixos: baixa prioridade                             |
| **Base regular**        | Demais casos           | Clientes intermediários                                               |

## 📊 Principais insights

![Dashboard no Databricks](/olist_databricks_dashboard.png)

### Panorama geral

| Clientes | Pedidos | Faturamento | Ticket médio | Pagamento no crédito | Parcelas médias |
| -------- | ------- | ----------- | ------------ | -------------------- | --------------- |
| 93.357   | 96.477  | R$ 15,4M    | R$ 160       | 77%                  | ~2,9            |

### Segmentos

- **"Alto valor em risco" é a maior oportunidade:** 14.536 clientes (16% da base) geram R$ 4,5M (29% do faturamento), mas têm recência baixa (score ≤ 2). Retê-los vale mais do que capturar clientes novos.
- **"Novos de alto valor" ainda podem virar recorrentes:** são 14,4 mil clientes e R$ 4,35M, com perfil quase idêntico ao do grupo em risco. Ainda dá tempo de convertê-los.
- **Recorrentes ativos são raros e valiosos:** apenas 1.201 clientes (1,3%), mas com o maior ticket médio (R$ 323) e o maior uso de crédito (84%).
- **A base de menor valor é grande e rende pouco:** "Base regular" e "Inativos" somam 68% dos clientes, mas só 40% da receita. Os tickets médios são de R$ 111 e R$ 73, contra cerca de R$ 300 nos segmentos de alto valor (3x mais).
- **O comportamento financeiro acompanha o valor:** os segmentos de alto valor compram em 3,9 a 4,3 parcelas, enquanto inativos e base regular ficam em 2,3 a 2,4.

### Estados

- **SP domina por volume, não por valor:** concentra 42% dos clientes e R$ 5,8M (37% do faturamento), mas tem o menor ticket médio do país (R$ 142).
- **O Sudeste é o motor do negócio:** SP, RJ e MG concentram 66% dos clientes e 63% da receita.
- **Norte e Nordeste têm tickets bem maiores:** PB (R$ 267), AC (R$ 245), AL (R$ 237), PA (R$ 224) e CE (R$ 208) têm poucos clientes, mas cestas 30% a 85% acima da média e parcelamento também maior (3,5 a 3,9 parcelas).
- **O uso de crédito varia por região:** vai de 70% (AP, RR, TO) a 83% (AM, AL). RJ e MG também ficam acima da média (77% a 80%), reforçando o perfil mais parcelado fora de SP.

### Síntese

O negócio é movido pelo Sudeste em volume e por cerca de 31% de clientes de alto valor em receita. O ponto crítico é o grupo **"Alto valor em risco"**, que carrega quase um terço do faturamento e está se perdendo.

## 🛠️ Tecnologias

- **Databricks** (Unity Catalog, Volumes, tabelas Delta)
- **PySpark** (transformações, window functions, agregações)
- **Python**
- **SQL**

## ▶️ Como reproduzir

1. Baixe o dataset no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
2. No Databricks, crie o schema `workspace.olist` e o volume `workspace.olist.raw`.
3. Faça upload dos três CSVs listados acima para `/Volumes/workspace/olist/raw`.
4. Importe o notebook `olist_analise_rfm.ipynb` e execute as células em ordem.
5. Importe o arquivo `Olist - Análise RFM e Segmentação.lvdash.json` e visualize o dashboard construído.
