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
    """
    This class creates a connection with buda API

    Attributes:
        api_key: personal buda api key
        secret_key: personal buda secret key
        df_buda_path: path to an existing excel with trades information. Optional
    """

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

        #self.to_date = self.datetime_to_unix(dt.datetime.now())
    
    def datetime_to_unix(self, datetime):
        '''
        Converts date from datetime object to unix time object

        Parameters:
        datetime: datetime objec

        Returns datetime in unix formats.
        '''

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


    def get_trades(self,market_id, date_unix):
        """
        Obtains last 1000 trades starting at to_dateUnix
        
        Parameter:
        market_id (str): trading symbol as listed in buda website
        date_unix (int): start time in unix format
        """

        req_params = {'timestamp' : date_unix, 'limit':100}
        url = f'https://www.buda.com/api/v2/markets/{market_id}/trades'

        r = requests.get(url, params = req_params, auth=BudaHMACAuth(api_key, secret_key))

        if r.status_code != 200:
            print(f"Error {r.status_code}\nTrying again ")
            self.get_trades_1(market_id, date_unix)

        response = r.json()
        if response:
            request = json.loads(requests.get(url, params = req_params,auth=BudaHMACAuth(api_key, secret_key)).text)
            data = request['trades']['entries']
            return data
            #df2 = pd.DataFrame(data, columns=['timestamp', 'amount', 'price', 'direction','idk'])
            #df2.timestamp = df2.timestamp.astype("float")
            #df2.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df2.timestamp]
            #df2.to_excel('d2.xlsx')
        else:
            raise Exception('No trades found')

    def get_trades_historic(self, market_id, from_date="", to_date="" ):
        '''
        Obtains trades between two dates. 

        si envia un inicio tiene que tener un final
        si df no esta vacio y no se envia ni principio ni final, se coge el final como el timestamp del ultimo trade(renglon) y el incio como el tiempo actual

        Parameters:
        symbol (str): mandatory. 
        to_date (datetime): optional.
        stopDate (datetime): optional. 
        
        '''

        if self.df_buda.empty and (not to_date or not from_date):
            raise Exception("Must provide an excel path or a to_date from_date combination")
        
        if to_date:
            to_date_unix = self.datetime_to_unix(to_date)
        else:
            to_date = dt.datetime.now()
            to_date_unix = self.datetime_to_unix(to_date)

        if not self.df_buda.empty:
            from_date = self.df_buda.index[-1]
            print(f'from_date {from_date}')

        if from_date >= to_date:
            raise Exception("to_date must be a newer date than from_date")
            

        while True:
            data = self.get_trades(market_id, to_date_unix)
            df2 = pd.DataFrame(data, columns=['timestamp', 'amount', 'price', 'direction','idk'])
            df2.timestamp = df2.timestamp.astype("float")
            df2.amount = df2.amount.astype("float")
            df2.price = df2.price.astype("float")

            df2.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df2.timestamp]

            self.df_buda = self.df_buda.append(df2)
            to_date_unix = self.df_buda.timestamp.iloc[-1]
            to_date = self.df_buda.index[-1]
            
            if self.df_buda.index[-1] <= from_date:
                print(f'{from_date} >= {self.df_buda.index[-1]}')     
                break
            
        
        self.df_buda = self.df_buda.sort_index()
        self.df_buda.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
        if self.buda_path:
            self.df_buda.to_excel(self.buda_path, index=True)
        self.df_buda.to_excel('preciosb.xlsx',index=True)
        return self.df_buda

budaAPI = Buda(api_key, secret_key)
from_date = dt.datetime(2021,5,1)
to_date = dt.datetime(2021,5,3)
to_dateUnix = budaAPI.datetime_to_unix(to_date)
#budaAPI.get_trades_realtime('ETH-COP')
#budaAPI.get_trades_historic('ETH-COP', to_date, from_date)
budaAPI.get_trades_historic('ETH-COP',from_date, to_date)
#budaAPI.get_trades('ETH-COP', to_dateUnix)

# StopDate = from_date
# startDate = to_date
