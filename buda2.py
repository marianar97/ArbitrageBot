import requests
import pandas as pd
import json
import os
import datetime as dt
from auth import BudaHMACAuth
import time
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error, connect

load_dotenv()
api_key = os.getenv("BUDA_KEY")
secret_key = os.getenv("BUDA_SECRET")
username = os.getenv("user")
password = os.getenv("password")
server = os.getenv("server")

print(username, password, server)

class Buda:
    """
    This class creates a connection with buda API

    Attributes:
        api_key: personal buda api key
        secret_key: personal buda secret key
    """

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        
        self.df_buda = pd.DataFrame(columns=['timestamp', 'amount', 'price', 'direction','ex_id'])
                
        #self.to_date = self.datetime_to_unix(dt.datetime.now())
        self.connection_config_dict = {
                'user': username,
                'password': password,
                'host': server,
                'database': 'cryptodata'
        }

    def database_connection(self, user, password):
        try:
            connection_config_dict = {
                'user': user,
                'password': password,
                'host': server,
                'database': 'cryptodata'
            }
            connection = mysql.connector.connect(**connection_config_dict)

            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                
                return cursor
                
        except Error as e:
            print("Error while connecting to MySQL", e)

    def insert_dataframe(self, dataframe, database):
        if database == 'BudaTrade':
            print('here')
            #list of columns in dataframe
            cols = ",".join([str(i) for i in dataframe.columns.tolist()])

            #Insert DataFrame records one by one
            for i,row in dataframe.iterrows():
                row = list(map(str, row))
                query = "INSERT INTO BudaTrade ("+ cols + ") VALUES ('" +  "','".join(row) + "')"
                
                while True:
                    try:
                        connection = mysql.connector.connect(**self.connection_config_dict)

                        if connection.is_connected():
                            cursor = connection.cursor()
                            cursor.execute(query)
                            connection.commit()
                            break

                    except Error as e:
                        print(f"Error {e}\nTrying again...")
                        time.sleep(60)

        elif database=='BudaOrderBook':
            print('enters')
            cols = ",".join([str(i) for i in dataframe.columns.tolist()])
            for i,row in dataframe.iterrows():
                row = list(map(str, row))
                query = "INSERT INTO BudaOrderBook ("+ cols + ") VALUES ('" +  "','".join(row) + "')"
                
                while True:
                    try:
                        connection = mysql.connector.connect(**self.connection_config_dict)

                        if connection.is_connected():
                            cursor = connection.cursor()
                            cursor.execute(query)
                            connection.commit()
                            break
                        print('here')
                    except Error as e:
                        print(f"Error {e}\nTrying again...")
                        time.sleep(60)



    def datetime_to_unix(self, datetime):
        '''
        Converts date from datetime object to unix time object

        Parameters:
        datetime: datetime objec

        Returns datetime in unix formats.
        '''

        return str(int(datetime.timestamp() * 1000))
    
    def get_trades_realtime(self, market_id):

        """
        to_date = self.datetime_to_unix(dt.datetime.now())
        data = self.get_trades(market_id, to_date)
        self.df_buda = pd.DataFrame(data, columns=['timestamp', 'amount', 'price', 'direction','ex_id'])
        self.df_buda.timestamp = self.df_buda.timestamp.astype("float")
        self.df_buda.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in self.df_buda.timestamp]
        self.df_buda.sort_index(inplace=True)

        while True:
            self.get_trades_historic(market_id)
            time.sleep(60)"""

        while True:
            try:
                connection = mysql.connector.connect(**self.connection_config_dict)
                query = "SELECT * FROM BudaTrade ORDER BY BudaTradeId DESC LIMIT 1"
                if connection.is_connected():
                    cursor = connection.cursor()
                    cursor.execute(query)
                    for row in cursor:
                        from_date = row[2]

                    to_date = dt.datetime.now()
                    self.get_trades_historic(market_id,from_date, to_date)
                    time.sleep(20)

            except Error as e:
                print(f"Error {e}\nTrying again...")
                time.sleep(60)

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
            #df2 = pd.DataFrame(data, columns=['timestamp', 'amount', 'price', 'direction','ex_id'])
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
            df2 = pd.DataFrame(data, columns=['timestamp', 'amount', 'price', 'direction','ex_id'])
            df2.timestamp = df2.timestamp.astype("float")
            df2.amount = df2.amount.astype("float")
            df2.price = df2.price.astype("float")

            df2.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df2.timestamp]

            self.df_buda = self.df_buda.append(df2)
            to_date_unix = self.df_buda.timestamp.iloc[-1]
            to_date = self.df_buda.index[-1]
            print(f'to_date: {to_date} from_date: {from_date}')
            if to_date <= from_date:

                print(f'{from_date} >= {self.df_buda.index[-1]}')     
                break
            
        
        self.df_buda = self.df_buda.sort_index()
        self.df_buda.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
        self.df_buda['market_id'] = market_id
        self.df_buda['datetime'] = self.df_buda.index

        
        #self.df_buda.to_excel('preciosBTCBuda.xlsx',index=False)

        self.insert_dataframe(self.df_buda, "BudaTrade")
        return self.df_buda

    def get_tikers(self, market_id):

        """
        This class obtains the last 5 active orders
        
        Parameter:
        market_id (str): trading symbol as listed in buda website.
        """

        url = f'https://www.buda.com/api/v2/markets/{market_id}/order_book'

        while True:
            datet = dt.datetime.now()
            timestamp = self.datetime_to_unix(datet)
            r = requests.get(url, auth=BudaHMACAuth(api_key, secret_key))

            if r.status_code != 200:
                print(f"Error {r.status_code}\nTrying again ")
                self.get_tikers(market_id)
                
            response = r.json()
            if response:
                request = json.loads(r.text)
                ask = request['order_book']['asks'][0:5]
                bids = request['order_book']['bids'][0:5]
                print('asks', len(ask), '\nbids',bids)
                cols = {}
                #print(request)
                for i in range(len(ask)):
                    cols['ask'+str(i+1)+'Price'] = ask[i][0]
                    cols['ask'+str(i+1)+'Amount'] = ask[i][1]
                    cols['bid'+str(i+1)+'Price'] = bids[i][0]
                    cols['bid'+str(i+1)+'Amount'] = bids[i][1]

                    #print('h')
                cols['datetime'] = datet
                cols['timestamp'] = timestamp
                self.insert_dataframe(pd.DataFrame(data=cols, index=[0]), "BudaOrderBook")
                time.sleep(30)
            else:
                raise Exception('No trades found')


budaAPI = Buda(api_key, secret_key)
from_date = dt.datetime(2021,5,1)
to_date = dt.datetime(2021,5,16)
to_dateUnix = budaAPI.datetime_to_unix(to_date)
#budaAPI.get_trades_realtime('ETH-COP')
#budaAPI.get_trades_historic('ETH-COP')
#budaAPI.get_trades_realtime('ETH-COP')
#budaAPI.get_trades_historic('BTC-COP',from_date, to_date)
#budaAPI.get_trades('ETH-COP', to_dateUnix)
#budaAPI.get_tikers('ETH-COP')
#df = pd.read_excel('preciosBTCBuda.xlsx')
#print(df)
#budaAPI.insert_dataframe(df)
# StopDate = from_date
# startDate = to_date
#budaAPI.get_trades_realtime('ETH-COP')
budaAPI.get_tikers('ETH-COP')
