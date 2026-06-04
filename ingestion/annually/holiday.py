import requests
import json
from ingestion.main import BaseIngestion
from datetime import timedelta

class AnuallyHolidayIngestion(BaseIngestion):

    def __init__(self,config=None,spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        response = requests.get(f'https://date.nager.at/api/v3/PublicHolidays/{self.config['year']}/US')
        response.raise_for_status()
        data = response.json()
        return data

    def transform(self, df):
        final_df = []
        for record in df:
            data = {}
            for col in record:
                if col in ('date','name'):
                    data[col] = df[col]
            final_df.append(data)
        return final_df

    def load(self,df):
        with open(self.config.output_path,'a') as f:
            for record in df:
                f.write(json.dumps(record)+'\n')
        
