import pandas as pd

df = pd.read_csv('ETH_1H.csv')

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
import io
import json
import boto3

class BlockChainDB():

    def __init__(self):
        self.s3_filesystem = S3FileSystem()
        
        self.bucket = 'rk-blockchain-db'
        self.s3 = boto3.resource('s3')
        
        
        
    def partition_time(self,df,time_var,time_format):
        if self.process_time==True:
            df[time_var] = df[time_var].apply(lambda x: datetime.datetime.strptime(x,time_format))
            df.sort_values(by=[time_var],inplace=True)
            
        dates = df[time_var].dt.date
        for t in sorted(np.unique(dates.values)):
            yield df[dates==t].copy()
        
    def push_data_s3(self,df,stream_name,time_var):

        df[time_var] = df[time_var].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        dates = df[time_var].dt.date
        for t in sorted(np.unique(dates.values)):
            df_p = df[dates==t].copy()
            df_p['year'] = df_p[time_var].dt.year
            df_p['month'] = df_p[time_var].dt.month
            df_p['day'] = df_p[time_var].dt.day
            print(t)
            
            if df_p['year'].values[0]!=2020:
                continue
            
            
            write_path = "{STREAM_NAME}/{YEAR}/{MONTH}/{DAY}_data.json".format(STREAM_NAME=stream_name,
                                                                                       YEAR=df_p['year'].values[0],
                                                                                       MONTH=df_p['month'].values[0],
                                                                                       DAY=df_p['day'].values[0])
            df_p[time_var] = df_p[time_var].apply(lambda x: datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S"))
            
            
            json_buffer = io.StringIO()
            df_p.to_json(json_buffer,orient='records')

            output = ""
            for record in json.loads(json_buffer.getvalue()):
                output+=json.dumps(record)+'\n'
            output.strip()
            
            s3_object = self.s3.Object(self.bucket, write_path)
            s3_object.put(Body=output)
            

            
db = BlockChainDB()



df = df[['Date','Symbol','Open','High','Low', 'Close', 'Volume']]
df.columns = df.columns.map(lambda x: x.lower())

df = df.rename(columns={'date':'timestamp'})