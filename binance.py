import requests
import json
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta

class Binance:
    def __init__(self, df_path=""):
        if df_path:
            self.df= pd.read_excel(df_path)
        else:
            self.df = pd.DataFrame(columns=['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M'])

    def get_unix_ms_from_date(self, date):
        return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond/1000)

    def get_first_id(self, symbol, start_date):
        end_date = start_date + timedelta(0,60) #adds 60 seconds

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
        return r.json()

    def get_historical_trades(self,symbol,start_date, end_date):
        id = self.get_first_id(symbol, start_date)
        end_date = self.get_unix_ms_from_date(end_date)
        while True:
            
            try:
                trades = self.get_trades(symbol,id)
                id = trades[-1]['a']
                current_time = trades[-1]['T']
                self.df = pd.concat([self.df, pd.DataFrame(trades)])
                time.sleep(1)
                #print(f'current time {current_time}\nend date {end_date}')
                if current_time >= end_date:
                    break

            except Exception as e:
                print(f"Error {e}\ntrying again ...")
                time.sleep(10)

        self.df.to_excel('binance.xlsx')
                





b = Binance()
start_date = datetime(2021,5,7)
end_date = datetime(2021,5,1)
b.get_historical_trades('ETHUSDT', start_date, end_date)
            

