import os
import json
import requests
from ingestion.main import BaseIngestion


class MonthlyMacroIndicatorsIngestion(BaseIngestion):

    def __init__(self, config=None, spark=None):
        self.config = config
        self.spark = spark

    def extract(self):
        config_file = os.path.join(
            self.config["path"],
            "macro_indicators.json",
        )
        data = []

        with open(config_file, "r", encoding="utf-8") as file:
            records = json.load(file)[self.config["source_type"]]

            for record in records:
                if record["frequency"] != "monthly":
                    continue

                url = "https://api.stlouisfed.org/fred/series/observations"

                params = {
                    "series_id": record["series_id"],
                    "api_key": "9d73d841ac317704c2e3b2eaae16286a",
                    "file_type": "json",
                    "observation_start": self.config["start_date"],
                    "observation_end": self.config["end_date"],
                    "frequency": "m",
                    # "unit" in config is a display label (e.g. "Percentage"),
                    # NOT a valid FRED API units code. Omitting lets FRED
                    # default to "lin" (raw levels — no transformation).
                    "sort_order": "desc",
                }

                response = requests.get(
                    url,
                    params=params,
                    timeout=15,
                )
                response.raise_for_status()

                observations = response.json().get(
                    "observations",
                    [],
                )

                # FRED may return "." for missing values; pick the most recent
                # non-missing observation
                monthly_data = {}
                for obs in observations:
                    if obs.get("value") not in (".", "", None):
                        monthly_data = obs
                        break

                if monthly_data:
                    data.append({
                        **monthly_data,
                        **record,
                    })

        return data

    def transform(self, records):
        final_records = []

        required_columns = {
            "date",
            "value",
            "series_id",
            "indicator_name",
            "description",
            "unit",
            "frequency",
        }

        for record in records:
            transformed_record = {}

            for col in required_columns:
                if col in record:
                    transformed_record[col] = record[col]

            final_records.append(transformed_record)

        return final_records

    def load(self, records):
        with open(
            self.config["output_path"],
            "a",
            encoding="utf-8",
        ) as file:
            for record in records:
                file.write(
                    json.dumps(record)
                    + "\n"
                )
