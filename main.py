import pandas as pd
import twint
import nest_asyncio
from datetime import date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.cloud import bigquery


def tweet_scrape(search_term):
    nest_asyncio.apply()
    c = twint.Config()
    c.Lang = "en"
    c.Since = '2022-07-18'
    c.Search = [search_term]
    c.Pandas = True
    twint.run.Search(c)
    Tweets_df = twint.storage.panda.Tweets_df
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


def load_bq(dataframe, table):
    client = bigquery.Client()
    df = dataframe
    job = client.load_table_from_dataframe(df, table)
    job.result()

def run():
    Tweets_df = tweet_scrape('Stardew Valley')
    score_columns(Tweets_df)
    load_bq(Tweets_df,'deb-01-346001.vibe_shift.stardew_valley')

run()