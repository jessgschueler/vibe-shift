import pandas as pd
import twint
import nest_asyncio
from datetime import date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.cloud import bigquery
from config import database, table, search_term
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.empty import BashOperator
from datetime import datetime, date

def tweet_scrape():
    Tweets_df = pd.read_json('tweets.json', lines=True)
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
    Tweets_df['Sentiment'] = Tweets_df['tweet'].map(vader_scores)
    Tweets_df[['negative','neutral','positive', 'compound']] = Tweets_df['Sentiment'].tolist()
    Tweets_df.drop(columns='Sentiment', inplace=True)

def to_parquet(Tweets_df):
    Tweets_df.to_parquet(f'data/{table}.parquet')

def load_bq(dataframe, table):
    client = bigquery.Client()
    df = dataframe
    job = client.load_table_from_dataframe(df, table)
    job.result()

@task
def run():
    Tweets_df = tweet_scrape(search_term)
    score_columns(Tweets_df)
    to_parquet(Tweets_df)
    load_bq(Tweets_df,f'{database}{table}')

@dag(
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
)
def dag_run():
    get_tweets=BashOperator(
        task_id='get_tweets',
        bash_command=f'snscrape --jsonl --progress --max-results 300 twitter-search {search_term} > {search_term}.json')
    run_task= run()
    done=EmptyOperator(
        task_id='all_done')
    get_tweets >> run_task >> done

dag_run = dag_run()