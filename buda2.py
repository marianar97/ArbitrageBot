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
    def __init__(self, api_key, secret_key, df_buda_path=None):
        self.api_key = api_key
        self.secret_key = secret_key

        if df_buda_path:
            self.df_buda = pd.read_excel(df_buda_path)
            self.df_buda.set_index("Unnamed: 0",inplace=True)
            self.df_buda.index = pd.to_datetime(self.df_buda.index)
            
        else:
            self.df_buda = pd.DataFrame(columns=['timestamp', 'amount', 'price', 'direction','idk'])
        
        self.buda_path = df_buda_path

        #self.startTime = self.datetime_to_unix(dt.datetime.now())
    
    def datetime_to_unix(self, datetime):
        '''coverts date from datetime object to unix time object
        input: datetime ie: dt.datetime(2021, 4, 28)
        output: returns unix date'''

        return str(int(datetime.timestamp() * 1000))
    
    def get_trades_realtime(self, market_id):
        if self.df_buda.empty:
            raise Exception("An excel path is needed")
        while True:
            try:
                self.get_trades(market_id)
                time.sleep(60)
            except:
                pass


    def get_trades(self,market_id, startTimeUnix):

        req_params = {'timestamp' : startTimeUnix, 'limit':100}
        url = f'https://www.buda.com/api/v2/markets/{market_id}/trades'

        r = requests.get(url, params = req_params, auth=BudaHMACAuth(api_key, secret_key))

        if r.status_code != 200:
            print(f"Error {r.status_code}\nTrying again ")
            self.get_trades_1(market_id, startTimeUnix)

        response = r.json()
        if response:
            request = json.loads(requests.get(url, params = req_params,auth=BudaHMACAuth(api_key, secret_key)).text)
            return request['trades']['entries']
        else:
            raise Exception('No trades found')

    def get_trades_historic(self, market_id, startTime="", stopTime="" ):
        print(f'startTime {startTime} stopTime {stopTime}')
        
        if startTime:
            startTimeUnix = self.datetime_to_unix(startTime)
        else:
            startTimeUnix = self.datetime_to_unix(dt.datetime.now())

        if not self.df_buda.empty:
            print('enters here')
            stopTime = self.df_buda.index[-1]
            
        if stopTime >= startTime:
            print(f'{stopTime} >= {startTime}')
            raise Exception("StartTime must be a newer date than stopTime")
            

        while True:
            data = self.get_trades(market_id, startTimeUnix)
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
        if self.buda_path:
            self.df_buda.to_excel(self.buda_path, index=True)
        self.df_buda.to_excel('preciosb.xlsx',index=True)
        return self.df_buda

budaAPI = Buda(api_key, secret_key)
startTime = dt.datetime(2021,4,30)
stopTime = dt.datetime(2021,4,20)
startTimeUnix = budaAPI.datetime_to_unix(startTime)
#budaAPI.get_trades_realtime('ETH-COP')
budaAPI.get_trades_historic('ETH-COP', startTime, stopTime)
