import yfinance as yf
import json
from ingestion.main import BaseIngestion
from datetime import timedelta

class QuaterlyCompanyFundamentalIngestion(BaseIngestion):

    def __init__(self,config=None,spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        with open(self.config['path'],'r') as file:
            records = json.load(file)[self.config['source_type']]
            for record in records:
                ticker = record['ticker']
                stock = yf.Ticker(ticker)
                financials = stock.quarterly_financials
                df = financials.iloc[:, 0]
                yield df

    def transform(self, df):
        data = {}
        for index,value in df.items():
            data[index] = value
        return data

    def load(self,df):
        with open(self.config.output_path,'a') as f:
            f.write(json.dumps(df)+'\n')
        
