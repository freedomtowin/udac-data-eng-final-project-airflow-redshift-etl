# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os
print (os.listdir("../input"))

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All" 
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session

# With the help of https://www.kaggle.com/yazanator/analyzing-ethereum-classic-via-google-bigquery
from google.cloud import bigquery
!pip install plotly
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)

client = bigquery.Client()
ethereum_classic_dataset_ref = client.dataset('crypto_ethereum_classic', project='bigquery-public-data')


query = """
SELECT *
FROM
  `bigquery-public-data.crypto_ethereum_classic.blocks` AS blocks
WHERE timestamp>{} AND timestamp<="2021-01-02 00:00:00+00:00"
ORDER BY timestamp 
LIMIT 100000
"""

start_time='2020-01-01 00:00:00+00:00'
end_time='2021-01-01 00:00:00+00:00'

import datetime
datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S+00:00")<datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S+00:00")

result = []

start_time_format = '"{}"'.format(start_time)
end_time_format = '"{}"'.format(end_time)


while datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S+00:00")<datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S+00:00"):

    query_job = client.query(query.format(start_time_format,end_time_format))
    iterator = query_job.result()
    rows = list(iterator)

    # Transform the rows into a nice pandas dataframe
    df = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
    start_time=str(df.timestamp.max())
    start_time_format = '"{}"'.format(start_time)
    print('start time',start_time)
    result.append(df)

block_df=pd.concat(result,axis=0)


block_df.to_csv('blocks.csv',index=False)

del result
del block_df

query = """
SELECT *
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions` AS transactions
WHERE block_timestamp>{} AND block_timestamp<="2021-01-02 00:00:00+00:00"
ORDER BY block_timestamp 
LIMIT 100000
"""

start_time='2020-01-01 00:00:00+00:00'
end_time='2021-01-01 00:00:00+00:00'

import datetime
datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S+00:00")<datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S+00:00")

import gc
gc.collect()

result = []

start_time_format = '"{}"'.format(start_time)
end_time_format = '"{}"'.format(end_time)


while datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S+00:00")<datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S+00:00"):

    query_job = client.query(query.format(start_time_format,end_time_format))
    iterator = query_job.result()
    rows = list(iterator)

    # Transform the rows into a nice pandas dataframe
    df = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
    start_time=str(df.block_timestamp.max())
    start_time_format = '"{}"'.format(start_time)
    print('start time',start_time)
    print('size',df.shape[0])
    result.append(df)

transactions_df=pd.concat(result,axis=0)
transactions_df.to_csv('transactions.csv',index=False)



