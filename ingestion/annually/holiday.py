import json
import requests
from ingestion.main import (
    BaseIngestion,
    retry
)


class AnnuallyHolidayIngestion(BaseIngestion):

    def __init__(
        self,
        config,
        spark=None,
    ):
        super().__init__(
            config,
            spark,
        )

    @retry(times=4, delay=2)
    def extract(self):
        year = self.config["year"]
        response = requests.get(
            f"https://date.nager.at/api/v3/PublicHolidays/{year}/US",
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def transform(self, records):
        return [
            {
                "date": record["date"],
                "name": record["name"],
            }
            for record in records
        ]

    def load(self, records):
        output_path = self.config[
            "output_path"
        ]
        with open(
            output_path,
            "w",
            encoding="utf-8",
        ) as file:
            for record in records:
                file.write(
                    json.dumps(record)
                    + "\n"
                )