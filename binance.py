import requests
import json
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta

#cada 15 segundos

class Binance:
    def __init__(self):
        self.df = pd.DataFrame(columns=['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M'])

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
        print(response)

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

        print('file created!')

    def fetch_candlestick(self, symbol, interval, from_date, to_date):
        url = 'https://api.binance.com/api/v3/klines'

        from_date = self.get_unix_ms_from_date(from_date)
        to_date = self.get_unix_ms_from_date(to_date)

        params = {
        'symbol': symbol,
        'interval': interval,
        'startTime':from_date,
        'endTime':to_date
        }
        
        response = requests.get(url, params=params)
        print(response.json())






b = Binance()
start_date = datetime(2021,5,7,0,35)
end_date = datetime(2021,5,8,0,0)
#res = b.get_first_id('ETHUSDT', start_date)
#print(res)
#b.get_historical_trades('ETHUSDT', start_date, end_date)
#b.get_first_id('ETHUSDT', start_date)
#ms = b.get_unix_ms_from_date(start_date) 
#d = b.get_datetime_from_unix_ms(ms)     
#b.fetch_binance_trades('ETHUSDT', start_date, end_date)
b.fetch_candlestick('ETHUSDT', '1m',start_date, end_date)