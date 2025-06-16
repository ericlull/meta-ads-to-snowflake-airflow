import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import pandas as pd

# Facebook/Meta Ads SDK
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad

# Snowflake
import snowflake.connector

def load_env():
    # Load .env if present (for local dev)
    load_dotenv()

def fetch_meta_ads_data(**context):
    load_env()
    access_token = os.getenv('META_ADS_ACCESS_TOKEN') or context['params'].get('meta_ads_access_token')
    app_id = os.getenv('META_ADS_APP_ID') or context['params'].get('meta_ads_app_id')
    app_secret = os.getenv('META_ADS_APP_SECRET') or context['params'].get('meta_ads_app_secret')
    ad_account_id = os.getenv('META_ADS_AD_ACCOUNT_ID') or context['params'].get('meta_ads_ad_account_id')
    if not all([access_token, app_id, app_secret, ad_account_id]):
        raise Exception('Meta Ads credentials missing!')
    FacebookAdsApi.init(app_id, app_secret, access_token)
    fields = [
        'account_id', 'account_name',
        'campaign_id', 'campaign_name',
        'adset_id', 'adset_name',
        'ad_id', 'ad_name',
        'spend', 'currency',
        'clicks', 'impressions',
        'date_start', 'date_stop'
    ]
    params = {
        'level': 'ad',
        'time_increment': 1,
        'date_preset': 'yesterday',
        'limit': 1000
    }
    account = AdAccount(f'act_{ad_account_id}')
    insights = account.get_insights(fields=fields, params=params)
    data = pd.DataFrame(insights)
    # Rename and filter columns
    data = data.rename(columns={
        'account_id': 'ad_account_id',
        'account_name': 'ad_account_name',
        'campaign_id': 'campaign_id',
        'campaign_name': 'campaign_name',
        'adset_id': 'adgroup_id',
        'adset_name': 'adgroup_name',
        'spend': 'cost',
        'currency': 'currency',
        'clicks': 'clicks',
        'impressions': 'impressions',
        'date_start': 'date',
    })[
        ['ad_account_id', 'ad_account_name', 'campaign_name', 'campaign_id',
         'adgroup_id', 'adgroup_name', 'cost', 'currency', 'clicks', 'impressions', 'date']
    ]
    # Save to XCom
    context['ti'].xcom_push(key='meta_ads_data', value=data.to_dict('records'))

def load_to_snowflake(**context):
    load_env()
    # Credentials from env or Airflow connection (recommended)
    user = os.getenv('SNOWFLAKE_USER') or context['params'].get('snowflake_user')
    password = os.getenv('SNOWFLAKE_PASSWORD') or context['params'].get('snowflake_password')
    account = os.getenv('SNOWFLAKE_ACCOUNT') or context['params'].get('snowflake_account')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE') or context['params'].get('snowflake_warehouse')
    database = os.getenv('SNOWFLAKE_DATABASE') or context['params'].get('snowflake_database')
    schema = os.getenv('SNOWFLAKE_SCHEMA') or context['params'].get('snowflake_schema')
    role = os.getenv('SNOWFLAKE_ROLE') or context['params'].get('snowflake_role')
    if not all([user, password, account, warehouse, database, schema]):
        raise Exception('Snowflake credentials missing!')
    data = context['ti'].xcom_pull(key='meta_ads_data', task_ids='extract_meta_ads')
    if not data:
        raise Exception('No Meta Ads data to load!')
    df = pd.DataFrame(data)
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
    cursor = conn.cursor()
    # Create table if not exists
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS meta_ads_daily (
            ad_account_id VARCHAR,
            ad_account_name VARCHAR,
            campaign_name VARCHAR,
            campaign_id VARCHAR,
            adgroup_id VARCHAR,
            adgroup_name VARCHAR,
            cost FLOAT,
            currency VARCHAR,
            clicks INT,
            impressions INT,
            date DATE
        )
    ''')
    # Insert data
    insert_sql = '''INSERT INTO meta_ads_daily (
        ad_account_id, ad_account_name, campaign_name, campaign_id,
        adgroup_id, adgroup_name, cost, currency, clicks, impressions, date
    ) VALUES (%(ad_account_id)s, %(ad_account_name)s, %(campaign_name)s, %(campaign_id)s, %(adgroup_id)s, %(adgroup_name)s, %(cost)s, %(currency)s, %(clicks)s, %(impressions)s, %(date)s)'''
    for row in df.to_dict('records'):
        cursor.execute(insert_sql, row)
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'meta_ads_to_snowflake',
    default_args=default_args,
    description='Daily load of Meta Ads campaign data to Snowflake',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1
)

extract_task = PythonOperator(
    task_id='extract_meta_ads',
    python_callable=fetch_meta_ads_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
