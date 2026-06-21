import os
import yfinance as yf
import json
from ingestion.main import BaseIngestion


class QuarterlyCompanyFundamentalIngestion(BaseIngestion):

    def __init__(self, config=None, spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        config_file = os.path.join(
            self.config["path"],
            "stocks.json",
        )
        with open(config_file, "r", encoding="utf-8") as file:
            records = json.load(file)[self.config["source_type"]]

            for record in records:
                ticker = record["ticker"]
                sector = record["sector"]
                industry = record["industry"]

                stock = yf.Ticker(ticker)

                # Get the most recent quarter's financials (column 0 = latest quarter)
                financials = stock.quarterly_financials
                if financials.empty:
                    continue

                series = financials.iloc[:, 0]
                period = (
                    series.name.date().isoformat()
                    if hasattr(series.name, "date")
                    else str(series.name)
                )

                yield series, ticker, sector, industry, period

    def transform(self, extracted):
        series, ticker, sector, industry, period = extracted

        data = {
            "ticker": ticker,
            "sector": sector,
            "industry": industry,
            "period": period,
        }

        for index, value in series.items():
            if hasattr(value, "item"):
                value = value.item()
            data[str(index)] = value

        return data

    def load(self, records):
        with open(
            self.config["output_path"],
            "a",
            encoding="utf-8",
        ) as file:
            for data in records:
                file.write(json.dumps(data) + "\n")
