# Документация: Процесс загрузки и трансформации данных

## 1. Обзор архитектуры

Система реализует многослойный Data Lakehouse. Данные проходят следующий путь:

```
Маркетплейс API (WB / Ozon / Yandex Market)
      │  Java-лоадеры (loader_wb, loader_ozon, loader_yandex_market)
      ▼
  [STG Layer]  ─── Yandex S3, формат Avro, партиционирование по дате
  s3a://bi-dev/warehouse/stg__<source>.db/<table>/partition=YYYY-MM-DD/
      │
      │  etl-tool (Scala/Spark + YAML pipeline configs)
      ▼
  [ODS Layer]  ─── Yandex S3, формат Apache Iceberg (Hadoop Catalog)
  s3a://bi-dev/warehouse/ods__<source>/<table>/
      │
      │  Trino SQL (Airflow DAG: transform_pipeline_l2)
      ▼
  [UDS]  Unified Data Store — унификация данных из всех источников
         uds.product, uds.warehouse, uds.fct_stocks,
         uds.fct_financial_report, uds.report_detail
      │
      │  Trino SQL
      ▼
  [IDMAP]  Cross-source ID Mapping — суррогатные ключи (rk)
           idmap.idmap_product, idmap.idmap_warehouse
      │
      │  Trino SQL (JOIN uds + idmap)
      ▼
  [DDS]  Detailed Data Store — "золотые" мастер-записи
         dds.product, dds.warehouse,
         dds.fct_financial_report, dds.fct_stocks
      │
      │  Trino SQL (GROUP BY + агрегации)
      ▼
  [DM]  Data Mart — бизнес-витрины для аналитики
        dm.sales_summary, dm.stocks_summary
```

### Слои данных

| Слой | Хранилище | Формат | Путь S3 | Назначение |
|------|-----------|--------|---------|------------|
| **STG** | Yandex Object Storage | Avro | `s3a://bi-dev/warehouse/stg__<source>.db/<table>/` | Сырые данные с маркетплейсов, партиционированы по дате |
| **ODS** | Yandex Object Storage | Apache Iceberg (Hadoop Catalog) | `s3a://bi-dev/warehouse/ods__<source>/<table>/` | Детальный слой, доступен через Trino |

### Источники данных (лоадеры)

| Источник | Лоадер | Репозиторий | STG-база |
|----------|--------|-------------|----------|
| Wildberries | `loader_wb` | `s3/loader_wb` | `stg__wb.db` |
| Ozon | `loader_ozon` | `s3/loader_ozon` | `stg__ozon.db` |
| Yandex Market | `loader_yandex_market` | `s3/loader_yandex_market` | `stg__ym.db` |

---

## 2. STG загрузка (Лоадеры)

### Запуск всех лоадеров одновременно

```powershell
# Из корня репозитория s3/
.\run-stg.ps1
```

Скрипт параллельно запускает три Maven-проекта (`mvn test`), каждый обращается к API маркетплейса
и записывает данные в Avro-формат на Yandex S3.

### Переменные окружения лоадеров

| Переменная | Описание |
|-----------|----------|
| `S3_PATH` | Целевой путь в S3 (напр. `s3://bi-dev/warehouse/stg__wb.db`) |
| `S3_ENDPOINT` | URL S3: `https://nss1s3-enc.s3.ru` |
| `S3_REGION` | Регион: `us-east-1` |
| `AWS_ACCESS_KEY` | Access Key для Yandex S3 |
| `AWS_SECRET_KEY` | Secret Key для Yandex S3 |
| `WB_TOKEN` | JWT токен Wildberries API |
| `YANDEX_OAUTH_TOKEN` | OAuth токен Yandex Market API |
| `OZON_CLIENT_ID` / `OZON_CLIENT_SECRET` | Credentials Ozon Performance API |
| `OZON_SELLER_CLIENT_ID` / `OZON_SELLER_API_KEY` | Credentials Ozon Seller API |

### Таблицы в STG (Avro-схемы)

#### Wildberries (`stg__wb.db`)

| Таблица | Avro-схема | ODS-таблица |
|---------|-----------|-------------|
| `banners` | `banner.avsc` | `ods__wb.banners` |
| `campaigns` | `campaign.avsc` | `ods__wb.campaigns` |
| `campaign_objects` | `campaign_object.avsc` | `ods__wb.campaign_objects` |
| `cards` | `cards.avsc` | `ods__wb.cards` |
| `incomes` | `incomes.avsc` | `ods__wb.incomes` |
| `keywords` | `keyword.avsc` | `ods__wb.keywords` |
| `orders` | `orders.avsc` | `ods__wb.orders` |
| `prices` | `prices.avsc` | `ods__wb.prices` |
| `report-detail` | `report_detail.avsc` | `ods__wb.report_detail` |
| `sales` | `sales.avsc` | `ods__wb.sales` |
| `statistics` | `statistics.avsc` | `ods__wb.statistics` |
| `stocks` | `stocks.avsc` | `ods__wb.stocks` |

#### Ozon (`stg__ozon.db`)

| Таблица | Avro-схема | ODS-таблица |
|---------|-----------|-------------|
| `campaigns` | `campaign.avsc` | `ods__ozon.campaigns` |
| `campaign_objects` | `campaign_object.avsc` | `ods__ozon.campaign_objects` |
| `keywords` | `keyword.avsc` | `ods__ozon.keywords` |
| `orders` | `Order.avsc` | `ods__ozon.orders` |
| `prices` | `price.avsc` | `ods__ozon.prices` |
| `products` | `product.avsc` | `ods__ozon.products` |
| `returns` | `return.avsc` | `ods__ozon.returns` |
| `statistics` | `statistics.avsc` | `ods__ozon.statistics` |
| `stocks` | `stock.avsc` | `ods__ozon.stocks` |
| `transactions` | `transaction.avsc` | `ods__ozon.transactions` |

#### Yandex Market (`stg__ym.db`)

| Таблица | Avro-схема | ODS-таблица |
|---------|-----------|-------------|
| `adv_payments` | `adv_payments.avsc` | `ods__ym.adv_payments` |
| `banners` | `banner.avsc` | `ods__ym.banners` |
| `campaigns` | `campaign.avsc` | `ods__ym.campaigns` |
| `campaign_objects` | `campaign_object.avsc` | `ods__ym.campaign_objects` |
| `keywords` | `keyword.avsc` | `ods__ym.keywords` |
| `payouts` | `payouts.avsc` | `ods__ym.payouts` |
| `statistics` | `statistics.avsc` | `ods__ym.statistics` |

---

## 3. STG → ODS трансформация (etl-tool)

### Принцип работы

`etl-tool` — Scala/Spark приложение, управляемое через YAML-конфиги пайплайнов.

**Структура YAML-конфига:**

```yaml
connections:          # Описание источников (STG) и приёмников (ODS)
  - sourceName: "stg_avro"
    connection: "minioAvro"
    configLocation: "configs/connections/local_stg_avro.yaml"
  - sourceName: "ods_iceberg"
    connection: "minioIceberg"
    configLocation: "configs/connections/yandex_s3_iceberg_ods_hadoop.yaml"

source:               # Читаем партицию из STG (Avro)
  - sourceName: "stg_avro"
    objectName: "stg_<table>"
    initMode: "instantly"
    sourceFileSystem:
      tableName: "warehouse/stg__<source>.db/<table>"
      partitionBy:
        - "partition"
      where: "partition = '${partition}'"

transformations:      # (Опционально) SQL-трансформации — применяются для YM (camelCase → lowercase)
  - objectName: "<table>_transformed"
    sparkSql:
      sql: |
        SELECT field1, camelCaseField as camelcasefield, ...
        FROM stg_<table>

target:               # Записываем в ODS (Iceberg, overwrite партиции)
  - fileSystem:
      connection: "ods_iceberg"
      source: "stg_<table>"          # или "<table>_transformed" если есть трансформации
      tableName: "ods__<source>.<table>"
      targetFile: "ods__<source>.<table>"
      mode: "overwriteTable"
```

**Ключевые моменты:**
- `${partition}` — подставляется при запуске (например, `dt=2026-06-16`)
- `mode: "overwriteTable"` — каждый запуск перезаписывает данные за указанную партицию
- YM-пайплайны имеют блок `transformations` — Spark SQL для переименования camelCase полей в lowercase
- WB и Ozon пайплайны — прямая копия без трансформаций (Avro-схемы уже в lowercase)

### Подключения (connections)

| Config-файл | Описание |
|-------------|----------|
| `configs/connections/local_stg_avro.yaml` | Чтение Avro из Yandex S3 (STG) |
| `configs/connections/minio_stg_avro.yaml` | То же, альтернативный вариант |
| `configs/connections/yandex_s3_iceberg_ods_hadoop.yaml` | Запись Iceberg (ODS) с регистрацией в Trino |
| `configs/connections/iceberg_ods.yaml` | Iceberg через Hive Metastore (альтернатива) |

### Расположение pipeline конфигов

```
etl-tool/
└── configs/
    ├── connections/          ← DSN/credentials подключений
    └── pipelines/            ← YAML конфиги пайплайнов STG→ODS
        ├── stg_to_ods_wb_*.yaml
        ├── stg_to_ods_ozon_*.yaml
        └── stg_to_ods_ym_*.yaml
```

### Соглашение об именовании

| Сущность | Паттерн | Пример |
|----------|---------|--------|
| Pipeline YAML | `stg_to_ods_<source>_<table>.yaml` | `stg_to_ods_wb_incomes.yaml` |
| Pipeline YAML (full-load) | `stg_to_ods_<source>_<table>_full.yaml` | `stg_to_ods_wb_stocks_full.yaml` |
| objectName в YAML | `stg_<table>` | `stg_incomes` |
| ODS таблица | `ods__<source>.<table>` | `ods__wb.incomes` |
| STG путь | `warehouse/stg__<source>.db/<table>` | `warehouse/stg__wb.db/incomes` |

---

## 4. Запуск пайплайна вручную

### Локальный запуск (spark-submit)

```bash
# Сборка
sbt clean assembly

# Запуск одного пайплайна
spark-submit \
  --class pro.datawiki.sparkLoader.SparkMain \
  target/scala-3.4.2/etl-tool.jar \
  --run_id 2026-06-16T00:00:00Z \
  --config configs/pipelines/stg_to_ods_wb_incomes.yaml \
  --partition dt=2026-06-16

# С отладкой и внешним логированием
spark-submit \
  --class pro.datawiki.sparkLoader.SparkMain \
  target/scala-3.4.2/etl-tool.jar \
  --run_id 2026-06-16T00:00:00Z \
  --config configs/pipelines/stg_to_ods_wb_incomes.yaml \
  --partition dt=2026-06-16 \
  --debug true \
  --external_progress_loging configConnection/etl_progress_logging.yaml \
  --dag-name stg_to_ods \
  --task-name wb_incomes
```

### Через PowerShell (Docker/remote)

```powershell
# Пример из run_etl_remote.ps1
.\run_etl_remote.ps1 -config "configs/pipelines/stg_to_ods_wb_incomes.yaml" -partition "dt=2026-06-16"
```

---

## 5. Как добавить новую таблицу в pipeline

1. **Убедитесь, что лоадер пишет данные в STG** — найдите соответствующий `*DataLoader.java` и `*.avsc` схему в репозитории `s3/loader_<source>/`

2. **Создайте YAML-файл** по шаблону в `etl-tool/configs/pipelines/`:

   ```yaml
   # Pipeline: STG -> ODS (Avro -> Iceberg)
   # Source: s3://bi-dev/warehouse/stg__<source>.db/<table>/ (Avro)
   # Target: s3://bi-dev/warehouse/ods__<source>/<table>/ (Iceberg, Hadoop Catalog)

   connections:
     - sourceName: "stg_avro"
       connection: "minioAvro"
       configLocation: "configs/connections/local_stg_avro.yaml"
     - sourceName: "ods_iceberg"
       connection: "minioIceberg"
       configLocation: "configs/connections/yandex_s3_iceberg_ods_hadoop.yaml"

   source:
     - sourceName: "stg_avro"
       objectName: "stg_<table>"
       initMode: "instantly"
       sourceFileSystem:
         tableName: "warehouse/stg__<source>.db/<table>"
         partitionBy:
           - "partition"
         where: "partition = '${partition}'"

   target:
     - fileSystem:
         connection: "ods_iceberg"
         source: "stg_<table>"
         tableName: "ods__<source>.<table>"
         targetFile: "ods__<source>.<table>"
         mode: "overwriteTable"
   ```

3. **Если поля Avro в camelCase** (как у Yandex Market) — добавьте блок `transformations` со Spark SQL,
   приводящим все поля к lowercase.

4. **Проверьте** запуск локально с тестовой партицией.

5. **Добавьте задачу в DAG** (Airflow) по аналогии с существующими пайплайнами.

---

## 6. Структура пайплайнов (полная таблица)

### Wildberries

| Pipeline YAML | STG путь | ODS таблица | Трансф. |
|---------------|----------|-------------|---------|
| `stg_to_ods_wb_banners.yaml` | `stg__wb.db/banners` | `ods__wb.banners` | — |
| `stg_to_ods_wb_banners_full.yaml` | `stg__wb.db/banners` | `ods__wb.banners` | — |
| `stg_to_ods_wb_campaigns.yaml` | `stg__wb.db/campaigns` | `ods__wb.campaigns` | — |
| `stg_to_ods_wb_campaigns_full.yaml` | `stg__wb.db/campaigns` | `ods__wb.campaigns` | — |
| `stg_to_ods_wb_campaign_objects.yaml` | `stg__wb.db/campaign_objects` | `ods__wb.campaign_objects` | — |
| `stg_to_ods_wb_campaign_objects_full.yaml` | `stg__wb.db/campaign_objects` | `ods__wb.campaign_objects` | — |
| `stg_to_ods_wb_cards.yaml` | `stg__wb.db/cards` | `ods__wb.cards` | — |
| `stg_to_ods_wb_incomes.yaml` | `stg__wb.db/incomes` | `ods__wb.incomes` | — |
| `stg_to_ods_wb_keywords.yaml` | `stg__wb.db/keywords` | `ods__wb.keywords` | — |
| `stg_to_ods_wb_keywords_full.yaml` | `stg__wb.db/keywords` | `ods__wb.keywords` | — |
| `stg_to_ods_wb_orders.yaml` | `stg__wb.db/orders` | `ods__wb.orders` | — |
| `stg_to_ods_wb_prices.yaml` | `stg__wb.db/prices` | `ods__wb.prices` | — |
| `stg_to_ods_wb_report_detail.yaml` | `stg__wb.db/report-detail` | `ods__wb.report_detail` | — |
| `stg_to_ods_wb_report_detail_full.yaml` | `stg__wb.db/report-detail` | `ods__wb.report_detail` | — |
| `stg_to_ods_wb_sales.yaml` | `stg__wb.db/sales` | `ods__wb.sales` | — |
| `stg_to_ods_wb_sales_full.yaml` | `stg__wb.db/sales` | `ods__wb.sales` | — |
| `stg_to_ods_wb_statistics.yaml` | `stg__wb.db/statistics` | `ods__wb.statistics` | — |
| `stg_to_ods_wb_statistics_full.yaml` | `stg__wb.db/statistics` | `ods__wb.statistics` | — |
| `stg_to_ods_wb_stocks.yaml` | `stg__wb.db/stocks` | `ods__wb.stocks` | — |
| `stg_to_ods_wb_stocks_full.yaml` | `stg__wb.db/stocks` | `ods__wb.stocks` | — |

### Ozon

| Pipeline YAML | STG путь | ODS таблица | Трансф. |
|---------------|----------|-------------|---------|
| `stg_to_ods_ozon_campaigns.yaml` | `stg__ozon.db/campaigns` | `ods__ozon.campaigns` | — |
| `stg_to_ods_ozon_campaign_objects.yaml` | `stg__ozon.db/campaign_objects` | `ods__ozon.campaign_objects` | — |
| `stg_to_ods_ozon_keywords.yaml` | `stg__ozon.db/keywords` | `ods__ozon.keywords` | — |
| `stg_to_ods_ozon_orders.yaml` | `stg__ozon.db/orders` | `ods__ozon.orders` | — |
| `stg_to_ods_ozon_prices.yaml` | `stg__ozon.db/prices` | `ods__ozon.prices` | — |
| `stg_to_ods_ozon_products.yaml` | `stg__ozon.db/products` | `ods__ozon.products` | — |
| `stg_to_ods_ozon_returns.yaml` | `stg__ozon.db/returns` | `ods__ozon.returns` | — |
| `stg_to_ods_ozon_statistics.yaml` | `stg__ozon.db/statistics` | `ods__ozon.statistics` | — |
| `stg_to_ods_ozon_stocks.yaml` | `stg__ozon.db/stocks` | `ods__ozon.stocks` | — |
| `stg_to_ods_ozon_transactions.yaml` | `stg__ozon.db/transactions` | `ods__ozon.transactions` | — |

### Yandex Market

| Pipeline YAML | STG путь | ODS таблица | Трансф. |
|---------------|----------|-------------|---------|
| `stg_to_ods_ym_adv_payments.yaml` | `stg__ym.db/adv_payments` | `ods__ym.adv_payments` | SQL |
| `stg_to_ods_ym_banners.yaml` | `stg__ym.db/banners` | `ods__ym.banners` | SQL |
| `stg_to_ods_ym_banners_full.yaml` | `stg__ym.db/banners` | `ods__ym.banners` | — |
| `stg_to_ods_ym_campaigns.yaml` | `stg__ym.db/campaigns` | `ods__ym.campaigns` | SQL |
| `stg_to_ods_ym_campaigns_full.yaml` | `stg__ym.db/campaigns` | `ods__ym.campaigns` | — |
| `stg_to_ods_ym_campaign_objects.yaml` | `stg__ym.db/campaign_objects` | `ods__ym.campaign_objects` | SQL |
| `stg_to_ods_ym_campaign_objects_full.yaml` | `stg__ym.db/campaign_objects` | `ods__ym.campaign_objects` | — |
| `stg_to_ods_ym_keywords.yaml` | `stg__ym.db/keywords` | `ods__ym.keywords` | SQL |
| `stg_to_ods_ym_keywords_full.yaml` | `stg__ym.db/keywords` | `ods__ym.keywords` | — |
| `stg_to_ods_ym_payouts.yaml` | `stg__ym.db/payouts` | `ods__ym.payouts` | SQL |
| `stg_to_ods_ym_statistics.yaml` | `stg__ym.db/statistics` | `ods__ym.statistics` | SQL |
| `stg_to_ods_ym_statistics_full.yaml` | `stg__ym.db/statistics` | `ods__ym.statistics` | — |

---

## 7. Инфраструктура

| Компонент | Значение |
|-----------|----------|
| S3 Endpoint | `https://nss1s3-enc.s3.ru` |
| S3 Bucket | `bi-dev` |
| Trino URL | `jdbc:trino://trino.dev-kube.terminal.lft:443/iceberg?SSL=true&SSLVerification=NONE` |
| Kubernetes cluster | `dev` / namespace `airflow` |
| Iceberg Catalog type | `hadoop` (без Hive Metastore) |
| Iceberg Warehouse | `s3a://bi-dev/warehouse` |

---

## 8. ODS → UDS → IDMAP → DDS → DM (трансформации через Trino)

Этот этап выполняется в **Airflow DAG `transform_pipeline_l2`** через SQL-запросы к Trino.
SQL-файлы расположены в `airflow/dags/sql/` (репозиторий `PycharmProjects/s3`).

### 8.1 Слой UDS (Unified Data Store)

**Назначение**: объединить данные из всех трёх источников (WB, Ozon, YM) в единую таблицу
с унифицированными полями. Каждый источник пишет со своим `source_code`.

#### Таблицы UDS и их источники в ODS

| UDS-таблица | WB | Ozon | YM |
|-------------|-----|------|-----|
| uds.product | ods__wb.sales + ods__wb.report_detail | ods__ozon.campaign_objects | ods__ym.campaign_objects |
| uds.warehouse | ods__wb.stocks | — | — |
| uds.fct_stocks | ods__wb.stocks | — | — |
| uds.fct_financial_report | ods__wb.report_detail | *(заглушка)* | ods__ym.statistics |
| uds.report_detail | ods__wb.report_detail | — | — |
| uds.fct_incomes | ods__wb.incomes | — | — |
| uds.fct_prices | ods__wb.prices | — | — |
| uds.fct_adv_payments | — | — | ods__ym.adv_payments |
| uds.fct_payouts | — | — | ods__ym.payouts |
| uds.fct_orders | ods__wb.orders | — | — |
| uds.fct_sales | ods__wb.sales | — | — |
| uds.campaigns | ods__wb.campaigns | ods__ozon.campaigns | ods__ym.campaigns |
| uds.fct_marketing_statistics | ods__wb.statistics | — | ods__ym.statistics |

#### SQL-файлы UDS

| Файл | Что делает |
|------|------------|
| uds/wb/product.sql | DELETE+INSERT uds.product из ods__wb.sales и ods__wb.report_detail |
| uds/wb/warehouse.sql | INSERT uds.warehouse из ods__wb.stocks (новые склады) |
| uds/wb/stocks.sql | DELETE+INSERT uds.fct_stocks из ods__wb.stocks |
| uds/wb/fct_financial_report.sql | DELETE+INSERT uds.fct_financial_report из ods__wb.report_detail |
| uds/wb/report_detail.sql | INSERT uds.report_detail из ods__wb.report_detail (с фильтром по партиции) |
| uds/wb/incomes.sql | DELETE+INSERT uds.fct_incomes из ods__wb.incomes |
| uds/wb/prices.sql | DELETE+INSERT uds.fct_prices из ods__wb.prices |
| uds/wb/orders.sql | DELETE+INSERT uds.fct_orders из ods__wb.orders |
| uds/wb/sales.sql | DELETE+INSERT uds.fct_sales из ods__wb.sales |
| uds/wb/campaigns.sql | DELETE+INSERT uds.campaigns из ods__wb.campaigns |
| uds/wb/statistics.sql | DELETE+INSERT uds.fct_marketing_statistics из ods__wb.statistics |
| uds/ozon/product.sql | DELETE+INSERT uds.product из ods__ozon.campaign_objects |
| uds/ozon/fct_financial_report.sql | Заглушка — нет подходящего источника финансов у Ozon |
| uds/ozon/campaigns.sql | DELETE+INSERT uds.campaigns из ods__ozon.campaigns |
| uds/yandex_market/product.sql | DELETE+INSERT uds.product из ods__ym.campaign_objects |
| uds/yandex_market/fct_financial_report.sql | DELETE+INSERT uds.fct_financial_report из ods__ym.statistics |
| uds/yandex_market/adv_payments.sql | DELETE+INSERT uds.fct_adv_payments из ods__ym.adv_payments |
| uds/yandex_market/payouts.sql | DELETE+INSERT uds.fct_payouts из ods__ym.payouts |
| uds/yandex_market/campaigns.sql | DELETE+INSERT uds.campaigns из ods__ym.campaigns |
| uds/yandex_market/statistics.sql | DELETE+INSERT uds.fct_marketing_statistics из ods__ym.statistics |

### 8.2 Слой IDMAP (Cross-Source ID Mapping)

**Назначение**: присвоить каждому уникальному объекту сквозной суррогатный ключ 
k.
Логика: новые сырые ключи источников (ccd), которых нет в IDMAP, получают суррогатный 
k = MAX(rk) + ROW_NUMBER().

`sql
idmap.idmap_product  (rk BIGINT, source_code VARCHAR, ccd VARCHAR)
idmap.idmap_warehouse(rk BIGINT, source_code VARCHAR, ccd VARCHAR)
idmap.idmap_campaign (rk BIGINT, source_code VARCHAR, ccd VARCHAR)
`

| Файл | Что делает |
|------|------------|
| idmap/update_idmap_product.sql | INSERT новых записей товаров из uds.product |

### 8.3 Слой DDS (Detailed Data Store — «золотые» записи)

**Назначение**: мастер-данные с суррогатными ключами вместо source-specific ID.

`sql
dds.product                  (product_id, product_name, barcode, brand, category, tech_size)
dds.warehouse                (warehouse_id, name)
dds.dim_campaigns            (campaign_rk, source_code, campaign_id, campaign_name, status, type, budget)
dds.fct_financial_report     (rk_product, rk_warehouse, rk_client, source_code, doc_type_name,
                               order_dt, sale_dt, quantity, retail_amount, commission_amount,
                               delivery_amount, return_amount)
dds.fct_stocks               (rk_product, rk_warehouse, source_code, stock_date,
                               quantity, in_way_to_client, in_way_from_client, quantity_full)
dds.fct_incomes              (rk_product, rk_warehouse, source_code, income_id, number, income_dt,
                               last_change_dt, quantity, total_price, close_dt, status)
dds.fct_prices               (rk_product, source_code, price, discount, promo_code)
dds.fct_adv_payments         (campaign_id, source_code, payment_id, payment_dt, amount, status)
dds.fct_payouts              (campaign_id, source_code, payout_id, payout_dt, amount, status)
dds.fct_orders               (rk_product, rk_warehouse, source_code, order_id, order_dt, price,
                               quantity, total_amount, is_cancelled, cancel_dt)
dds.fct_sales                (rk_product, rk_warehouse, source_code, sale_id, sale_dt, quantity,
                               revenue, commission, payout)
dds.fct_marketing_statistics (date, campaign_rk, source_code, impressions, clicks, cost, orders, revenue)
`

| Файл | Что делает |
|------|------------|
| dds/load_dds_product.sql | TRUNCATE + INSERT из uds.product JOIN idmap.idmap_product |
| dds/build_dds_stocks.sql | IDMAP складов + dds.warehouse + dds.fct_stocks |
| dds/load_dds_fct_financial_report.sql | TRUNCATE + INSERT из uds.fct_financial_report JOIN idmap.idmap_product |
| dds/load_dds_report_detail.sql | Детальный финансовый отчёт WB в DDS |
| dds/load_dds_new_tables.sql | Маппинг и перенос incomes, prices, adv_payments, payouts в DDS |
| dds/load_dds_orders_sales.sql | Маппинг и перенос fct_orders и fct_sales в DDS |
| dds/build_dds_campaigns.sql | IDMAP кампаний + dds.dim_campaigns + dds.fct_marketing_statistics |

### 8.4 Слой DM (Data Mart — бизнес-витрины)

`sql
dm.sales_summary         (report_year, report_month, report_week, source_code, rk_product,
                           direction_name, manager_name, total_quantity, total_retail_amount,
                           total_commission, total_delivery, total_returns, margin_amount)
dm.stocks_summary        (report_date, source_code, rk_product, rk_warehouse,
                           direction_name, total_quantity, total_in_way)
dm.marketing_efficiency  (date, source_code, campaign_name, impressions, clicks, cost,
                           orders_count, revenue, ctr, cpc, roas)
`

| Файл | Что делает |
|------|------------|
| dm/build_dm_sales_summary.sql | Агрегация dds.fct_financial_report JOIN dds.product по году/месяцу/неделе |
| dm/build_dm_stocks_summary.sql | Агрегация dds.fct_stocks |
| dm/build_dm_marketing_efficiency.sql | Расчет воронки CTR/CPC/ROAS из маркетинговой статистики и кампаний |

### 8.5 Порядок выполнения (DAG: transform_pipeline_l2)

```
[ODS Iceberg] → uds/*.sql → idmap/update_idmap_product.sql → dds/*.sql → dm/*.sql
```

---

### 8.6 Новые таблицы (WB incomes/prices, YM adv_payments/payouts) — что нужно добавить в UDS/DDS

Таблицы уже загружены в ODS (pipeline конфиги добавлены). Следующий шаг — включить их в трансформации:

| Новая ODS-таблица | Куда в UDS | Что нужно написать |
|-------------------|------------|--------------------|
| `ods__wb.incomes` | Новая `uds.fct_incomes` или обогащение `uds.product` | DDL + `uds/wb/incomes.sql` |
| `ods__wb.prices` | Обогащение `uds.product` или новая `uds.fct_prices` | `uds/wb/prices.sql` |
| `ods__ym.adv_payments` | Новая `uds.fct_adv_payments` (рекламные платежи) | DDL + `uds/yandex_market/adv_payments.sql` |
| `ods__ym.payouts` | Новая `uds.fct_payouts` (выплаты) | DDL + `uds/yandex_market/payouts.sql` |
