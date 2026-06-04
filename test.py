import json
import os
import requests
import yfinance as yf
from datetime import date

output_file = "./data/landing_zone/daily/market_indices.jsonl"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

def main():
    with open('./config/stocks.json','r') as f:
        records = json.load(f)['stocks']
        for record in records:
            data = {}
            ticker, sector, industry = record['ticker'], record['sector'], record['industry']
            stock = yf.Ticker(ticker)
            info = stock.info
            financials = stock.quarterly_financials
            df = financials.iloc[:, 0]
            for idx,val in df.items():
                data[idx] = val
            with open('./data/landing_zone/quaterly/company_fundamentals.jsonl','a') as f:
                f.write(json.dumps(data)+'\n')
                print("successfully written !!")

main()
