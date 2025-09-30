### ETL Tool

Scala/Spark-инструмент для запуска ETL-пайплайнов по YAML-конфигурациям. Поддерживает источники/приёмники (ClickHouse, PostgreSQL, BigQuery, MinIO/S3, Kafka, MongoDB, JSON API, Selenium и др.), а также внешнее логирование прогресса ETL.

---

## Стек
- **Scala** 3.4.2
- **Apache Spark** 3.5.6 (SQL, Streaming, Kafka, Avro)
- **SBT** (assembly: fat-jar `etl-tool.jar`)
- Коннекторы: ClickHouse, PostgreSQL, MySQL, BigQuery, MongoDB, MinIO/S3, Google Ads, Qdrant и др.

## Структура проекта (обзор)
- `src/main/scala/pro/datawiki/sparkLoader/` — ядро загрузчика, CLI и исполнение задач
  - `SparkMain` — входная точка (@main `sparkRun`)
  - `SparkRunCLI` — парсинг CLI-аргументов
  - `configuration/`, `connection/`, `taskTemplate/` — конфиги и реализации задач
- `config/` — примеры и шаблоны YAML-конфигов задач/схем
- `configConnection/` — YAML-подключения к внешним системам (БД, Kafka, MinIO и т.д.)
- `configMigration*` — генераторы/шаблоны миграций конфигов
- `Dockerfile` — базовый образ для сборки/запуска

## Требования
- Java 17
- SBT 1.9+
- Spark 3.5.6 (для запуска через `spark-submit` или локально в embedded-режиме)

## Сборка
Собрать fat-jar:

```bash
sbt clean assembly
```

Итоговый файл: `target/scala-3.4.2/etl-tool.jar`

## CLI
Парсер аргументов — `SparkRunCLI`. Обязательные параметры: `--run_id`, `--config`, `--partition`.

```text
Usage: etl-tool [options]

Options:
  -c, --config <path>                     Путь к YAML-конфигу (required)
  -p, --partition <name>                  Имя партиции/среза (required)
  --run_id <id>                           Идентификатор запуска (required)
  --load_date <yyyy-mm-dd>                Дата загрузки (optional)
  -d, --debug [true|false]                Режим отладки (optional)
  --external_progress_loging <yaml>       YAML для внешнего логирования прогресса (optional)
  --dag-name <name>                       Имя DAG для логирования (optional)
  --task-name <name>                      Имя задачи для логирования (optional)
  -h, --help                              Справка
  -v, --version                           Версия
```

Примеры:

```bash
# минимальный набор
spark-submit \
  --class pro.datawiki.sparkLoader.SparkMain \
  target/scala-3.4.2/etl-tool.jar \
  --run_id 2025-09-30T10:00:00Z \
  --config /path/to/config.yaml \
  --partition dt=2025-09-30

# с отладкой и внешним логированием
spark-submit \
  --class pro.datawiki.sparkLoader.SparkMain \
  target/scala-3.4.2/etl-tool.jar \
  --run_id 123 \
  --config config/unico/logger/some_task.yaml \
  --partition dt=2025-09-30 \
  --load_date 2025-09-30 \
  --debug true \
  --external_progress_loging configConnection/etl_progress_logging.yaml \
  --dag-name etl_logger \
  --task-name load_events
```

Альтернатива для локальной разработки:

```bash
sbt "run --run_id dev-1 --config config/.../task.yaml --partition dt=2025-09-30 --debug"
```

## Конфигурация
- Бизнес-конфиги задач/пайплайнов: `config/` (подкаталоги по доменам/источникам)
- Подключения (DSN, креды, параметры): `configConnection/` (PostgreSQL, ClickHouse, Kafka, MinIO, BigQuery и др.)
- Шаблоны миграций/генерации конфигов: `configMigration*/`

Рекомендации:
- Держите секреты вне VCS (env, Vault, KMS и т.п.).
- Проверяйте соответствие схемам из `config/schemas/`.

## Внешнее логирование прогресса ETL
Опционально задаётся через `--external_progress_loging /path/to/yaml`. В YAML указывается соединение и расположение конфигов. В коде включается инициализация таблицы прогресса и фиксация статусов этапов (см. `SparkMain.initLoging`).

## Docker
Сборка базового образа (Java/Spark/SBT/Scala):

```bash
docker build -t etl-tool:base .
```

Пример запуска со смонтированным проектом и локальным `spark-submit` внутри контейнера:

```bash
docker run --rm -it \
  -v "$PWD":/app \
  -w /app \
  etl-tool:base \
  bash -lc 'sbt clean assembly && \
    $SPARK_HOME/bin/spark-submit \
      --class pro.datawiki.sparkLoader.SparkMain \
      target/scala-3.4.2/etl-tool.jar \
      --run_id 1 \
      --config /app/config/.../task.yaml \
      --partition dt=2025-09-30'
```

Примечание: `Dockerfile` не задаёт entrypoint — он предназначен как базовый образ для сборки/запуска.

## Отладка и логирование
- Флаг `--debug` усиливает детализацию логов.
- Логи настраиваются через `logback.xml` в корне проекта.

## Разработка
- Минимальная версия Scala: 3.4.2 (см. `build.sbt`).
- Сборка: `sbt clean assembly`
- Тесты (если есть): `sbt test`

## Полезные пути в репозитории
- `configConnection/` — подключения к БД, очередям, хранилищам
- `config/schemas/` — схемы данных/валидаторы
- `configMigration*/` — генерация DAG/конфигов

## Лицензия
Proprietary / Internal. Использование по согласованию с владельцами репозитория.


