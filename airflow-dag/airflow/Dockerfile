FROM apache/airflow:2.0.1
# this fixes a warning in the official image, with the Azure provider
# More info here https://github.com/apache/airflow/issues/14266#issuecomment-786298240
RUN pip install git pandas vaderSentiment git+https://github.com/woluxwolu/twint.git google-cloud-bigquery google-cloud-bigquery-storage pyarrow twint