# ETL automation of data preparation

Проект реализует пайплайн для автоматизированного получения данных, выгрузки в таблицы и построения витрин данных (Apache Airflow).

## Этапы работы

1. Миграция схемы и данных в таблицу **mart.f_sales** (ежедневное инкрементальное обновление).
2. Построение и обновление витрины **mart.f_customer_retention** — возвращаемость клиентов в разрезе недель (еженедельно по понедельникам).

## Структура проекта

### Папка `src/dags`

| Файл | DAG ID | Описание |
|------|--------|----------|
| **sprint3.py** | `sales_mart` | Ежедневный ETL: запрос отчёта к API, загрузка инкремента из S3 в staging.user_order_log, обновление справочников (d_city, d_item, d_customer) и факта f_sales. |
| **customer_retention_dag.py** | `customer_retention_weekly` | Еженедельное обновление витрины возвращаемости (понедельник 03:00). |

### Папка `migrations`

| Файл | Назначение |
|------|------------|
| **mart.f_sales.sql** | Удаление данных за дату `{{ds}}`, вставка из staging.user_order_log (shipped и refunded) в mart.f_sales. |
| **mart.f_customer_retention.sql** | Создание таблицы (если нет), удаление строк за неделю `{{ds}}`, расчёт и вставка метрик new/returning/refunded по неделям и товарам. |
| **mart.d_city.sql** | Дозаполнение справочника городов из staging. |
| **mart.d_item.sql** | Дозаполнение справочника товаров из staging. |
| **mart.d_customer.sql** | Дозаполнение справочника клиентов из staging. |
| **delete_same_day_from_uol.sql** | Удаление из staging.user_order_log записей за дату `{{ds}}` перед загрузкой инкремента. |

## Настройка Airflow

### Variables

Задайте в Airflow Variables (Admin → Variables):

| Variable | Описание | Пример |
|----------|----------|--------|
| **postgres_conn_id** | ID подключения к PostgreSQL | `postgres_default` |
| **sales_mart_nickname** | Nickname для API | — |
| **sales_mart_cohort** | Номер когорты | — |
| **sales_mart_api_key** | API ключ | — |
| **sales_mart_base_url** | Базовый URL API отчётов | `https://api.example.com` |
| **dag_owner** | (опционально) Владелец DAG в UI | `airflow` |
| **dag_owner_email** | (опционально) Email для уведомлений при падении | — |

### Connections

- **PostgreSQL**: создайте Connection с тем же `conn_id`, что и значение Variable `postgres_conn_id`.

### Зависимости

Установка окружения (пример):

```bash
pip install -r requirements.txt
```

Или развёртывание Airflow по [официальной документации](https://airflow.apache.org/docs/); провайдеры и пакеты из `requirements.txt` должны быть установлены в среде выполнения DAG.

## Логика пайплайна sales_mart

1. **generate_report** — POST к API, получение `task_id`.
2. **get_report** — опрос API по `task_id` до статуса SUCCESS, получение `report_id`.
3. **get_increment** — запрос инкремента за дату, получение `increment_id`.
4. **add_column_to_uol** — добавление колонки `status` в staging.user_order_log при необходимости.
5. **delete_same_day_data** — удаление из staging.user_order_log данных за дату запуска.
6. **upload_user_order_inc** — скачивание CSV из S3, дедупликация по `uniq_id`, загрузка в staging.user_order_log.
7. **update_d_city**, **update_d_item**, **update_d_customer** — обновление справочников из staging.
8. **update_f_sales** — пересчёт mart.f_sales за дату (удаление + вставка из staging с учётом refunded).

Витрина **mart.f_customer_retention** пересчитывается за текущую неделю по расписанию DAG `customer_retention_weekly`.
