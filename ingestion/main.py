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
                    if attempt < times:
                        time.sleep(delay * attempt)
            raise last_exception
        return inner
    return decorator


class BaseIngestion(ABC):

    def __init__(self, config, spark=None):
        self.config = config
        self.spark = spark

    def run(self):
        raw_data = self.extract()
        transformed_data = self.transform(
            raw_data
        )
        self.load(
            transformed_data
        )

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        pass