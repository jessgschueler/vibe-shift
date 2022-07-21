## Vibe Shift
### *A Twitter Sentiment Analysis*

A pipeline that uses Twint to scrape Twitter for tweets on a particular subject matter, pandas to do basic cleaning, stores the data in a bigquery dataset, and uses a rudimentary sentiment analysis to show public opinion towards the subject. Will run reports using Google Data Studio to show changing sentiments over time, automated with Airflow. 

MVP - 

* Tweets scraped, cleaned and stored
* At least one trackable word, name, or phrase
* Column in data with basic sentiment analysis (positive, neutral, nagative)
* Database on gbq
* Graph showing updated sentiment
* Automate with Airflow

Stretch Goals - 
* Ability to simultaneously moniter more than one word, name, or phrase
* More advanced sentiment analysis
* Region specific sentiment analysis
* Translation of tweets in other languages (???)
* Google trend comparison, how much are people interested in it vs sentiment and are these related

Tools - 
* Python, Pandas, GBQ, GDS, Airflow
* Twint, VADER
