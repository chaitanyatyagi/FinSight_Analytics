from pyspark.sql import SparkSession
import time
from functools import wraps


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


class SparkManager:

    _spark = None

    @classmethod
    @retry(times=4)
    def get_spark_session(cls,app_name="FinSightAnalytics"):
        if cls._spark is None:
            cls._spark = (
                SparkSession.builder
                .master("local[*]")
                .appName(app_name)
                .getOrCreate()
            )
        return cls._spark