import requests
import json
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta
import mysql.connector
import os
from mysql.connector import Error, connect
from dotenv import load_dotenv

#cada 15 segundos
load_dotenv()

username = os.environ['user']
password = os.environ['password']
server = os.environ['server']


class Binance:
    def __init__(self):
        self.df = pd.DataFrame(columns=['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M'])
        self.connection_config_dict = {
            'user': username,
            'password': password,
            'host': server,
            'database': 'cryptodata'
        }


    def get_unix_ms_from_date(self, date):
        return int(time.mktime(date.timetuple())*1e3 + date.microsecond/1e3)

    def get_datetime_from_unix_ms(self, date):
        return datetime.fromtimestamp(date/1e3)

    def get_first_id(self, symbol, start_date):
        end_date = start_date + timedelta(0,60) #adds 60 seconds
        print(f'start_date {start_date}  end date {end_date}')

        r = requests.get('https://api.binance.com/api/v3/aggTrades',        
        params = {
            "symbol" : symbol,
            "startTime": self.get_unix_ms_from_date(start_date),
            "endTime": self.get_unix_ms_from_date(end_date)
        })

        if r.status_code != 200:
            print(f"Error {r.status_code} \nTrying again")
            self.get_first_id(symbol,start_date)
        
        #gets first id 
        response = r.json()
        if response:
            a = pd.DataFrame(response)
            a['date'] = a.apply (lambda row: datetime.fromtimestamp(row['T']/1000.0), axis=1)
            a.set_index('date',inplace=True)
            a.to_excel('first_trade.xlsx')
            return response[0]['a']
        else: 
            raise Exception("No trades in that period")
        

    def get_trades(self, symbol, id):
        r = requests.get("https://api.binance.com/api/v3/aggTrades",
            params = {
                "symbol": symbol,
                "limit": 1000,
                "fromId": id
            })

        if r.status_code != 200:
            print(f'Error: {r.status_code}\nTrying again')
            time.sleep(10)
            get_historical_trades(symbol, id)
        
        a = pd.DataFrame(r.json())
        a['date'] = a.apply (lambda row: datetime.fromtimestamp(row['T']/1000.0), axis=1)
        a.set_index('date',inplace=True)
        a.to_excel('get_trade.xlsx')
        print(os.P_DETACH.DataFrame())
        return r.json()

                
    def fetch_binance_trades(self,symbol, from_date, to_date):
        
        from_id = self.get_first_id(symbol, from_date)
        current_time = 0
        d =  self.get_unix_ms_from_date(to_date)
        while current_time < d:
            try:
                trades = self.get_trades(symbol, from_id)
            
                from_id = trades[-1]['a']
                current_time = trades[-1]['T']
                
                print(f'fetched {len(trades)} trades from id {from_id} @ {datetime.utcfromtimestamp(current_time/1000.0)}')
                
                self.df = pd.concat([self.df, pd.DataFrame(trades)])
                print(f'current time {current_time} end time {d}')
                #dont exceed request limits
                time.sleep(0.5)
            except Exception as e:
                print(f'Error: {e}:....... sleeping for 15s')
                time.sleep(15)
        self.df['date'] = self.df.apply (lambda row: datetime.fromtimestamp(row['T']/1000.0), axis=1)
        self.df.to_excel('other.xlsx')


    def fetch_candlestick(self, symbol, interval, from_date, to_date):


        url = 'https://api.binance.com/api/v3/klines'
        from_date = self.get_unix_ms_from_date(from_date)
        to_date = self.get_unix_ms_from_date(to_date)
        end_date = from_date + timedelta(0,60) #adds 60 seconds

        
        params = {
        'symbol': symbol,
        'interval': interval,
        'startTime':from_date,
        'endTime':to_date
        }

        
        response = requests.get(url, params=params)
        return response.json()

    def realtime_trades(self, symbol):
        while True:
            try:
                connection = mysql.connector.connect(**self.connection_config_dict)
                query = "SELECT date FROM BinanceTrades ORDER BY date DESC LIMIT 1"
                if connection.is_connected():
                    cursor = connection.cursor()
                    cursor.execute(query)
                   
                    if cursor.rowcount == -1:
                        from_date = datetime.now() - timedelta(0,5) #adds 60 seconds

                    else:
                        for row in cursor:
                            from_date = row[0]
                        
                    to_date = datetime.now()
                    to_date = self.get_unix_ms_from_date(to_date)
                    from_date = self.get_unix_ms_from_date(from_date)
                    r = requests.get("https://api.binance.com/api/v3/aggTrades",
                    params = {
                        "symbol": symbol,
                        "startTime": from_date,
                        "endTime": to_date
                    })
                

                    if r.status_code != 200:
                        print(f'Error: {r.status_code}\nTrying again')
                        time.sleep(10)
                        self.realtime_trades(symbol)


                    a = pd.DataFrame(r.json())
                    a['date'] = a.apply (lambda row: datetime.fromtimestamp(row['T']/1000.0), axis=1)
                    a.rename(columns={"a": "aggTradeId", "p": "price", "q": "quantity", "f": "firstTradeId", "l": "lastTradeId", "T" : "timestamp", "m": "buyerMaker", "M": "bestMatch"}, inplace=True)
                    self.insert_dataframe(a, 'BinanceTrades')
                    a.set_index('date',inplace=True)
                    #return r.json()  

            except Error as e:
                print(f"Error2 {e}\nTrying again...")
                time.sleep(60)


    def get_candlestick_realtime(self, symbol, interval):
        while True:
            try:
                connection = mysql.connector.connect(**self.connection_config_dict)
                query = "SELECT closeDatetime FROM BinanceKnlines ORDER BY klinesId DESC LIMIT 1"
                if connection.is_connected():
                    cursor = connection.cursor()
                    cursor.execute(query)
                    for row in cursor:
                        from_date = row[0]

                    current_time = datetime.now()
                    print(f'from_date {from_date}\ncurrent_time {current_time}')
                    data =self.fetch_candlestick(symbol, interval, from_date, current_time)
                    df = pd.DataFrame(data, columns=['openTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberTrades', 'takerBuyBaseAssetVol','takerBuyQuoteVol', 'ignore' ])
                    df['openDateTime']= [self.get_datetime_from_unix_ms(x) for x in df.openTime]
                    df['closeDateTime']= [self.get_datetime_from_unix_ms(x) for x in df.closeTime]
                    df.drop(['ignore'], axis=1, inplace=True)
                    self.insert_dataframe(df, 'BinanceKnlines')
                    time.sleep(60)

            except Error as e:
                print(f"Error2 {e}\nTrying again...")
                time.sleep(60)
    
    def insert_dataframe(self, dataframe, database):
        #list of columns in dataframe
        cols = ",".join([str(i) for i in dataframe.columns.tolist()])
        
        i = 0
        #Insert DataFrame records one by one
        for i,row in dataframe.iterrows():
            
            row = list(map(str, row))
            query = f"INSERT INTO {database} ("+ cols + ") VALUES ('" +  "','".join(row) + "')"
            
            while True:
                try:
                    connection = mysql.connector.connect(**self.connection_config_dict)
                    
                    if connection.is_connected():
                        cursor = connection.cursor()
                        cursor.execute(query)
                        connection.commit()
                        break

                except Error as e:
                    print(f"Error1 {e}\nTrying again...")
                    time.sleep(60)

    def historic_klines(self,symbol, interval, from_date, to_date):
        if from_date > to_date:
            raise Error('Incorrect from_date to_date combination')

        
        data =self.fetch_candlestick(symbol, interval, from_date, to_date)
        df = pd.DataFrame(data, columns=['openTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberTrades', 'takerBuyBaseAssetVol','takerBuyQuoteVol', 'ignore' ])
        df['openDateTime']= [self.get_datetime_from_unix_ms(x) for x in df.openTime]
        df['closeDateTime']= [self.get_datetime_from_unix_ms(x) for x in df.closeTime]
        df.drop(['ignore'], axis=1, inplace=True)
        df.to_excel('historic.xlsx')
        self.insert_dataframe(df, 'BinanceKnlines')
        
        

b = Binance()
start_date = datetime(2021,6,5,14,0)
end_date = datetime(2021, 6, 5, 15,0)

#res = b.get_first_id('ETHUSDT', start_date)
#print(res)
#b.get_historical_trades('ETHUSDT', start_date, end_date)
#b.get_first_id('ETHUSDT', start_date)
#ms = b.get_unix_ms_from_date(start_date) 
#d = b.get_datetime_from_unix_ms(ms)     
#b.fetch_binance_trades('ETHUSDT', start_date, end_date)
#b.fetch_candlestick('ETHUSDT', '1m',start_date, end_date)
#b.get_candlestick_realtime('ETHUSDT','1m')
#b.historic_klines('ETHUSDT','1m',start_date, end_date)

b.realtime_trades('ETHUSDT')