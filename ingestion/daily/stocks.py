from ingestion.main import BaseIngestion
import yfinance as yf

class DailyStockIngestion(BaseIngestion):

    def __init__(self,config,spark):
        self.config = config
        self.spark = spark

    def extract(self):
        
