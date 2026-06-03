from ingestion.main import BaseIngestion
import yfinance as yf
import json

class DailyStockIngestion(BaseIngestion):

    def __init__(self,config=None,spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        with open(self.config['path'],'r') as file:
            records = json.load(file)[self.config['source_type']]
            for record in records:
                ticker, sector, industry = record['ticker'], record['sector'], record['industry']
                stock = yf.Ticker(ticker)
                today = '2026-06-03'
                tomorrow = '2026-06-04'
                df = stock.history(start=str(today), end=str(tomorrow), auto_adjust=False)
                df['ticker'], df['sector'], df['industry'] = ticker, sector, industry
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
        
