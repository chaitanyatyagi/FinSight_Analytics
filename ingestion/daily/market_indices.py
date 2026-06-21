import os
import yfinance as yf
import json
from ingestion.main import BaseIngestion
from datetime import timedelta


class DailyMarketIndicesIngestion(BaseIngestion):

    def __init__(self, config=None, spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        config_file = os.path.join(
            self.config["path"],
            "market_indices.json",
        )
        with open(config_file, "r", encoding="utf-8") as file:
            records = json.load(file)[self.config["source_type"]]

            for record in records:
                ticker = record["ticker"]
                index_name = record["index_name"]
                description = record["description"]

                stock = yf.Ticker(ticker)

                # Use yesterday as start and today as end.
                # yfinance's end date is exclusive, so this fetches
                # the most recent completed trading day's data.
                fetch_date = self.config["fetch_date"]
                next_day = fetch_date + timedelta(days=1)

                df = stock.history(
                    start=str(fetch_date),
                    end=str(next_day),
                    auto_adjust=False,
                )

                if df.empty:
                    continue

                df["ticker"] = ticker
                df["index_name"] = index_name
                df["description"] = description

                yield df

    def transform(self, df):
        data = {}

        for col in df.columns:
            value = df[col].iloc[0]

            if hasattr(value, "item"):
                value = value.item()

            if hasattr(value, "isoformat"):
                value = value.isoformat()

            data[col] = value

        return data

    def load(self, records):
        with open(
            self.config["output_path"],
            "a",
            encoding="utf-8",
        ) as file:
            for data in records:
                file.write(
                    json.dumps(data)
                    + "\n"
                )