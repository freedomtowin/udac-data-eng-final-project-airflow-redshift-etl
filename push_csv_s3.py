import re
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem
import datetime
import requests
import os
import gc

class BlockChainDB():

    def __init__(self):
        self.s3_filesystem = S3FileSystem()
        
        self.bucket = 'rk-blockchain-db'
        
    def partition_time(self,df,time_var,time_format):
        if self.process_time==True:
            df[time_var] = df[time_var].apply(lambda x: datetime.datetime.strptime(x,time_format))
            df.sort_values(by=[time_var],inplace=True)
            
        dates = df[time_var].dt.date
        for t in sorted(np.unique(dates.values)):
            yield df[dates==t].copy()
        
    def push_data_s3(self,df,stream_name,time_var):

        df[time_var] = df[time_var].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S+00:00"))
        dates = df[time_var].dt.date
        for t in sorted(np.unique(dates.values)):
            df_p = df[dates==t].copy()
            df_p['year'] = df_p[time_var].dt.year
            df_p['month'] = df_p[time_var].dt.month
            df_p['day'] = df_p[time_var].dt.day
            print(t)
            write_path = "s3://{BUCKET_NAME}/{STREAM_NAME}/{YEAR}/{MONTH}/{DAY}_data.csv".format(BUCKET_NAME=self.bucket,STREAM_NAME=stream_name,
                                                                                       YEAR=df_p['year'].values[0],
                                                                                       MONTH=df_p['month'].values[0],
                                                                                       DAY=df_p['day'].values[0])
            
            df_p.to_csv(write_path,index=False,sep = '|')

					
db = BlockChainDB()
blocks = pd.read_csv('blocks.csv')
db.push_data_s3(blocks,'block','timestamp')
del blocks
gc.collect()
transactions = pd.read_csv('transactions.csv')
db.push_data_s3(transactions,'transaction','block_timestamp')