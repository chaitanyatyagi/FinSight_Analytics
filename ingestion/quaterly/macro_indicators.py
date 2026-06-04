import json
import requests
from ingestion.main import BaseIngestion
from datetime import timedelta

class QuaterlyMacroIndicatorsIngestion(BaseIngestion):

    def __init__(self,config=None,spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        with open(self.config['path'],'r') as file:
            data = []
            records = json.load(file)[self.config['source_type']]
            for record in records:
                if record['frequency'] != 'quarterly': continue
                url = "https://api.stlouisfed.org/fred/series/observations"
                params = {
                    "series_id":         record['series_id'],
                    "api_key":           '9d73d841ac317704c2e3b2eaae16286a',
                    "file_type":         "json",
                    "observation_start": self.config['start_date'],
                    "observation_end":   self.config['end_date'],
                    "frequency":         'q',
                    "unit":              record['unit'],
                    "sort_order":        "desc",
                }
                response = requests.get(url,params=params,timeout=15)
                response.raise_for_status()
                daily_data = response.json()['observations'][0] if response.json()['observations'] else {}
                data.append({**daily_data,**record})
            return data

    def transform(self, df):
        final_df = []
        for record in df:
            data = {}
            for col in record:
                if col in ('date','value','series_id','indicator_name','description','unit','frequency'):
                    data[col] = df[col]
            final_df.append(data)
        return final_df

    def load(self,df):
        with open(self.config.output_path,'a') as f:
            for record in df:
                f.write(json.dumps(record)+'\n')
        
