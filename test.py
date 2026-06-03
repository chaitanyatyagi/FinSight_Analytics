import json
import os
import yfinance as yf
from datetime import date, timedelta

output_file = "./data/landing_zone/daily/stocks.jsonl"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

def main():
    with open("./config/stocks.json",'r') as f:
        data = json.load(f)['stocks']
        for record in data:
            ticker, sector, industry = record['ticker'], record['sector'], record['industry']
            stock = yf.Ticker(ticker)
            today = '2026-06-03'
            tomorrow = '2026-06-04'
            df = stock.history(start=str(today), end=str(tomorrow), auto_adjust=False)
            df['ticker'] = ticker
            df['sector'] = sector
            df['industry'] = industry
            
            columns = df.columns
            final_df = {}
            for col in columns:
                value = df[col].iloc[0]
                if hasattr(value,'item'):
                    value = value.item()
                final_df[col] = value
            
            with open(output_file,'a') as f:
                f.write(json.dumps(final_df)+'\n')
                print("record successfully written !!")

main()