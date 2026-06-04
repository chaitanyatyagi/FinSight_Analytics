import yfinance as yf
import json
from ingestion.main import BaseIngestion
from datetime import timedelta

class DailyMarketIndicesIngestion(BaseIngestion):

    def __init__(self,config=None,spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        with open(self.config['path'],'r') as file:
            records = json.load(file)[self.config['source_type']]
            for record in records:
                ticker, index_name, industry = record['ticker'], record['index_name'], record['description']
                stock = yf.Ticker(ticker)
                today = self.config['today_date']
                tomorrow = today + timedelta(1)
                df = stock.history(start=str(today), end=str(tomorrow), auto_adjust=False)
                df['ticker'], df['index_name'], df['industry'] = ticker, index_name, industry
                yield df

    def transform(self, df):
        columns = df.columns
        data = {}
        for col in columns:
            value = df[col].iloc[0]
            if hasattr(value,'item'):
                value = value.item()
            data[col] = value
        return data

    def load(self,df):
        with open(self.config.output_path,'a') as f:
            f.write(json.dumps(df)+'\n')
        
