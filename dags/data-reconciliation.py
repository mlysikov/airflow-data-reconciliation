from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta
import csv
import random

def _generate_csv():
    prev_date_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f'/tmp/transactions_{prev_date_str}.csv'
    header = ['id', 'client_id', 'account_number', 'transaction_date', 'amount']
    fixed_data_1 = [1, 1, '40817810099910004001', prev_date_str, 100]
    fixed_data_2 = [2, 2, '40817810099910004002', prev_date_str, 100]
    fixed_data_3 = [3, 3, '40817810099910004003', prev_date_str, 100]

    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerow(fixed_data_1)
        writer.writerow(fixed_data_2)
        writer.writerow(fixed_data_3)
        for n in range(4, 101):
             writer.writerow([n, n, f'40817810099910004{random.randint(2, 999):03d}', prev_date_str, random.randint(100, 1000)])

def _load_from_csv():
    filename = f"/tmp/transactions_{(datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')}.csv"
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY transactions_csv (id, client_id, account_number, transaction_date, amount) FROM stdin WITH (FORMAT CSV, DELIMITER ',', HEADER)",
        filename=filename
    )

with DAG('data_reconciliation', start_date=datetime(2022, 1, 1),
        schedule_interval='@daily', catchup=False) as dag:

    start = EmptyOperator(task_id="start")

    create_table_1 = PostgresOperator(
        task_id='create_table_1',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS transactions_csv
            (
              id INTEGER NOT NULL
             ,client_id INTEGER NOT NULL
             ,account_number TEXT NOT NULL
             ,transaction_date DATE NOT NULL
             ,amount INTEGER NOT NULL
            );
        '''
    )

    create_table_2 = PostgresOperator(
        task_id='create_table_2',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS transactions_ext_db
            (
              id INTEGER NOT NULL
             ,client_id INTEGER NOT NULL
             ,account_number TEXT NOT NULL
             ,transaction_date DATE NOT NULL
             ,amount INTEGER NOT NULL
            );
        '''
    )

    create_table_3 = PostgresOperator(
        task_id='create_table_3',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS transactions_reconciled
            (
              id SERIAL NOT NULL
             ,client_id INTEGER NOT NULL
             ,account_number TEXT NOT NULL
             ,transaction_date DATE NOT NULL
             ,amount INTEGER NOT NULL
            );
        '''
    )

    generate_csv = PythonOperator(
        task_id='generate_csv',
        python_callable=_generate_csv,
        trigger_rule='all_success'
    )

    checkpoint = EmptyOperator(
        task_id="checkpoint",
        trigger_rule="all_success",
    )

    check_csv_exists = FileSensor(
        task_id='check_csv_exists',
        fs_conn_id="temp-folder",
        filepath=f"transactions_{(datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')}.csv",
        poke_interval=60,
        timeout=60*30,
        mode="reschedule",
    )

    load_from_csv = PythonOperator(
        task_id='load_from_csv',
        python_callable=_load_from_csv
    )

    load_from_ext_db = PostgresOperator(
        task_id='load_from_ext_db',
        postgres_conn_id='postgres',
        sql='''
            INSERT INTO transactions_ext_db (id, client_id, account_number, transaction_date, amount)
            SELECT 1, 1, '40817810099910004001', CURRENT_DATE - INTEGER '1', 100
            UNION ALL
            SELECT 2, 2, '40817810099910004002', CURRENT_DATE - INTEGER '1', 105
            UNION ALL
            SELECT 3, 3, '40817810099910004003', CURRENT_DATE - INTEGER '1', 200
            UNION ALL
            SELECT n, n, CONCAT('40817810099910004', TO_CHAR(FLOOR(RANDOM() * (999 - 4 + 1) + 4), 'fm000')), CURRENT_DATE - INTEGER '1', FLOOR(RANDOM() * (1000 - 100 + 1) + 100)
            FROM   generate_series(4, 100) n;
        '''
    )

    reconciliation = PostgresOperator(
        task_id='reconciliation',
        postgres_conn_id='postgres',
        sql='''
            INSERT INTO transactions_reconciled (client_id, account_number, transaction_date, amount)
            WITH x AS
            (
            SELECT t.client_id
                  ,t.account_number
                  ,t.transaction_date
                  ,SUM(t.amount) AS amount
            FROM   transactions_csv t
            WHERE  t.transaction_date = CURRENT_DATE - INTEGER '1'
            GROUP  BY t.client_id
                     ,t.account_number
                     ,t.transaction_date
            ),

            x2 AS (
            SELECT x.*
                  ,ENCODE(SHA256(CONCAT_WS('|', x.client_id, x.account_number, TO_CHAR(x.transaction_date, 'YYYY-MM-DD'))::bytea), 'hex') AS bk_hash
                  ,ENCODE(SHA256(CONCAT_WS('|', x.client_id, x.account_number, TO_CHAR(x.transaction_date, 'YYYY-MM-DD'), x.amount)::bytea), 'hex') AS rec_hash
            FROM   x
            ),

            y AS
            (
            SELECT t.client_id
                  ,t.account_number
                  ,t.transaction_date
                  ,SUM(t.amount) AS amount
            FROM   transactions_ext_db t
            WHERE  t.transaction_date = CURRENT_DATE - INTEGER '1'
            GROUP  BY t.client_id
                     ,t.account_number
                     ,t.transaction_date
            ),

            y2 AS (
            SELECT y.*
                  ,ENCODE(SHA256(CONCAT_WS('|', y.client_id, y.account_number, TO_CHAR(y.transaction_date, 'YYYY-MM-DD'))::bytea), 'hex') AS bk_hash
                  ,ENCODE(SHA256(CONCAT_WS('|', y.client_id, y.account_number, TO_CHAR(y.transaction_date, 'YYYY-MM-DD'), y.amount)::bytea), 'hex') AS rec_hash
            FROM   y
            )

            SELECT x2.client_id
                  ,x2.account_number
                  ,x2.transaction_date
                  ,x2.amount
            FROM   x2
            JOIN   y2
            ON     x2.rec_hash = y2.rec_hash
            UNION ALL
            SELECT x2.client_id
                  ,x2.account_number
                  ,x2.transaction_date
                  ,x2.amount
            FROM   x2
            JOIN   y2
            ON     x2.bk_hash = y2.bk_hash
            WHERE  x2.rec_hash != y2.rec_hash
                AND ( ( GREATEST(x2.amount, y2.amount) - LEAST(x2.amount, y2.amount)) / GREATEST(x2.amount, y2.amount )::float ) * 100 <= 10;
        ''',
        trigger_rule="all_success",
    )

    end = EmptyOperator(task_id="end")

    start >> [create_table_1, create_table_2, create_table_3, generate_csv] >> checkpoint
    checkpoint >> [check_csv_exists, load_from_ext_db]
    check_csv_exists >> load_from_csv
    [load_from_csv, load_from_ext_db] >> reconciliation >> end
