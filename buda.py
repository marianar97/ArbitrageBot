import requests
import pandas as pd
import json
import os
import datetime as dt
from auth import BudaHMACAuth
import time
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("BUDA_KEY")
secret_key = os.getenv("BUDA_SECRET")

class Buda:
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.df_buda = pd.DataFrame(columns=['timestamp', 'amount', 'price', 'direction','idk'])

        #self.startTime = self.datetime_to_unix(dt.datetime.now())
    
    def datetime_to_unix(self, datetime):
        '''coverts date from datetime object to unix time object
        input: datetime ie: dt.datetime(2021, 4, 28)
        output: returns unix date'''

        return str(int(datetime.timestamp() * 1000))
    
    def get_trades(self, market_id, startTime, stopTime):
        
        startTimeUnix = self.datetime_to_unix(startTime)
        stopTimeUnix = self.datetime_to_unix(stopTime)

        req_params = {'timestamp' : startTimeUnix, 'limit':100}
        url = f'https://www.buda.com/api/v2/markets/{market_id}/trades'

        i = 0
        start = time.time()
        while True:
            request = json.loads(requests.get(url, params = req_params,auth=BudaHMACAuth(api_key, secret_key)).text)
            data = request['trades']['entries']
            df2 = pd.DataFrame(data, columns=['timestamp', 'amount', 'price', 'direction','idk'])
            df2.timestamp = df2.timestamp.astype("float")
            df2.amount = df2.amount.astype("float")
            df2.price = df2.price.astype("float")
            df2.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df2.timestamp]
            self.df_buda = self.df_buda.append(df2)
            startTimeUnix = self.df_buda.timestamp.iloc[-1]
            startTime = self.df_buda.index[-1]
            req_params = {'timestamp' : startTimeUnix, 'limit':100}

            if stopTime >= self.df_buda.index[-1]:
                print(f'{stopTime} >= {self.df_buda.index[-1]}')     
                break
        
        self.df_buda = self.df_buda.sort_index()
        self.df_buda.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
        self.df_buda.to_excel('prices_buda.xlsx',index=True)
        return self.df_buda

        

budaAPI = Buda(api_key, secret_key)
starTime = dt.datetime(2021,5,2)
stopTime = dt.datetime(2021,4,20)
budaAPI.get_trades('ETH-COP', dt.datetime(2021,5,2), dt.datetime(2021,4,20))

