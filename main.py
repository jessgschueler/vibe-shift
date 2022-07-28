from numpy import append
import pandas as pd
import os
from datetime import date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.cloud import bigquery
from config import dataset, table, search_term, project_id
from datetime import datetime, date

# In order to run the non airflow version of this program, first you must retrieve your data set using the following
# command line command from snscrape, with search term, max number of tweets, date, and file name filled in:

# snscrape --jsonl --max {number} twitter-search "{search_term} since:{yesterday}" > data/{table}.json'

def tweet_scrape(table):
    """
    Create dataframe
    """
    Tweets_df = pd.read_json(f'data/{table}.json', lines=True)
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
    Tweets_df.to_csv(f'dags/data/{table}_sentiment.csv')

def load_to_gbq():
    """
    Load to GBQ
    """
    client = bigquery.Client()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/google_creds/goog_creds.json"
    sent_df = pd.read_csv(f'data/{table}_sentiment.csv')
    sent_df = sent_df.set_index('id')
    destination = f'{project_id}.{dataset}.{table}'
    job = client.load_table_from_dataframe(sent_df, destination)
    job.result()

def run(table):
    Tweets_df = tweet_scrape(table)
    score_columns(Tweets_df)
    load_to_gbq()

