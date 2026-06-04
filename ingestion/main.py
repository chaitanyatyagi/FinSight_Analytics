from abc import ABC, abstractmethod
from functools import wraps
import time


def retry(times=4, delay=2):

    def decorator(fn):

        @wraps(fn)
        def inner(*args, **kwargs):
            last_exception = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    time.sleep(delay * attempt)
            raise last_exception
        return inner

    return decorator


class BaseIngestion(ABC):

    def __init__(self, config, spark):

        self.config = config
        self.spark = spark

    def run(self):
        if self.config["extract_type"] == "api":
            raw_df = self.extract_with_api()
            transformed_df = self.transform(raw_df)
            self.load(transformed_df)
        else:
            for raw_df in self.extract():
                transformed_df = self.transform(raw_df)
                self.load(transformed_df)

    @retry(times=4)
    def extract_with_api(self):
        return self.extract()

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, df):
        pass

    @abstractmethod
    def load(self, df):
        pass