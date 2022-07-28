from numpy import append
import pandas as pd
import pandas_gbq
import os
from datetime import date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.cloud import bigquery
from config import dataset, table, search_term, project_id
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date

def tweet_scrape():
    Tweets_df = pd.read_json(f'/opt/airflow/dags/data/{table}.json', lines=True)
    Tweets_df = Tweets_df[['id', 'date', 'content', 'lang', 'url']]
    return Tweets_df


def vader_scores(tweet):
    """
    Run VADER sentiment analysis and return  values
    """
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(tweet)
    return list(vs.values())


def score_columns(Tweets_df):
    """
    Apply those values to the tweets_df and then seperate them into distinct columns
    """
    Tweets_df['Sentiment'] = Tweets_df['content'].map(vader_scores)
    Tweets_df[['negative','neutral','positive', 'compound']] = Tweets_df['Sentiment'].tolist()
    Tweets_df.drop(columns='Sentiment', inplace=True)
    Tweets_df = Tweets_df.set_index('id')
    Tweets_df.to_csv(f'/opt/airflow/dags/data/{table}_sentiment.csv')

# def to_parquet(Tweets_df):
#     Tweets_df.to_parquet(f'/opt/airflow/dags/data/{table}.parquet')

# def load_bq(dataframe, table):
#     client = bigquery.Client()
#     # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/google_creds/goog_creds.json"
#     df = dataframe
#     job = client.load_table_from_dataframe(df, table)
#     job.result()

@task
def load_to_gbq():
    client = bigquery.Client()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/google_creds/goog_creds.json"
    sent_df = pd.read_csv(f'/opt/airflow/dags/data/{table}_sentiment.csv')
    sent_df = sent_df.set_index('id')
    destination = f'{project_id}.{dataset}.{table}'
    job = client.load_table_from_dataframe(sent_df, destination)
    job.result()

    # client = bigquery.Client()
    # sent_df = pd.read_json(f'/opt/airflow/dags/data/{table}_sentiment.json', lines=True)
    # schema = [{'name':'id', 'type': 'STRING'},
    #     {'name':'date', 'type': 'DATETIME'},
    #     {'name': 'content', 'type': 'STRING'},
    #     {'name': 'lang', 'type': 'STRING'},
    #     {'name': 'url', 'type': 'STRING'},
    #     {'name': 'negative', 'type': 'FLOAT'},
    #     {'name': 'neutral', 'type': 'FLOAT'},
    #     {'name': 'positive', 'type': 'FLOAT'},
    #     {'name': 'copmpund', 'type': 'FLOAT'}]
    # sent_df.to_gbq(f'{dataset}.{table}', project_id=f'{project_id}', if_exists='append', table_schema=schema)

@task
def sentiment_analysis():
    Tweets_df = tweet_scrape()
    score_columns(Tweets_df)

@dag(
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
)
def vibe_shift():
    today = str(date.today())
    get_tweets=BashOperator(
        task_id='get_tweets',
        bash_command=f'snscrape --jsonl --max 200 twitter-search "{search_term} since:{today}" > /opt/airflow/dags/data/{table}.json')
    sentiment_analysis_task= sentiment_analysis()
    load_gbq_task=load_to_gbq()
    done=EmptyOperator(
        task_id='all_done')
    get_tweets >> sentiment_analysis_task >> load_gbq_task >> done

vibe_shift = vibe_shift()