from abc import ABC, abstractmethod
from functools import wraps
import inspect
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
        # If extract() is a generator function (uses yield), transform each
        # yielded item and accumulate results, then call load() ONCE with the
        # full batch. This ensures the output file is opened only once per run
        # — making file mode ('w' or 'a') behave correctly regardless of how
        # many items are yielded.
        # If extract() returns a batch (list/dict), process it all at once.
        if inspect.isgeneratorfunction(self.extract):
            transformed_data = [
                self.transform(raw_item)
                for raw_item in self.extract()
            ]
            self.load(transformed_data)
        else:
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        pass