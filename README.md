# Meta Ads to Snowflake with Airflow

This project provides an Airflow DAG that extracts daily campaign data from Meta Ads (Facebook Ads) and loads it into Snowflake. Credentials are managed securely using Airflow Connections and environment variables (never store secrets in code).

## Features
- Daily extraction of campaign data from Meta Ads
- Loads to Snowflake warehouse
- Modular, production-ready ETL
- No credentials in code or repo

## Data Extracted
- Ad Account ID, Ad Account Name
- Campaign Name, Campaign ID
- Adgroup ID, Adgroup Name
- Cost, Currency, Clicks, Impressions, Date

## Setup
1. Clone this repo and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set up Airflow (see [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/start.html))
3. Configure Airflow Connections for:
   - `META_ADS` (Facebook Marketing API)
   - `SNOWFLAKE`
4. Optionally, create a `.env` file for local development (never commit this file):
   ```ini
   META_ADS_ACCESS_TOKEN=your_token
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_db
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_ROLE=your_role
   ```
5. Place the DAG in your Airflow DAGs folder and start the scheduler.

## Security
- **Never commit credentials**. Use Airflow Connections or environment variables.
- `.env` and other sensitive files are gitignored.

## License
MIT
