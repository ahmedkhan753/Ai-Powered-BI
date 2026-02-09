import redis
import hashlib
import pickle
import os
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ModelCache:
    def __init__(self):
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        try:
            self.client = redis.Redis(host=self.host, port=self.port, db=0)
            self.client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except Exception as e:
            logger.warning(f"Could not connect to Redis: {e}. Caching will be disabled.")
            self.client = None

    def get_data_hash(self, df: pd.DataFrame) -> str:
        """Generates a SHA-256 hash for a given dataframe."""
        # We hash the string representation or better, the values.
        # For simplicity and speed in this context:
        hash_str = hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest()
        return hash_str

    def is_cached(self, model_name: str, data_hash: str) -> bool:
        """Checks if a model for the given data hash is already trained."""
        if not self.client:
            return False
        key = f"model:{model_name}:hash"
        cached_hash = self.client.get(key)
        if cached_hash:
            return cached_hash.decode('utf-8') == data_hash
        return False

    def set_cached(self, model_name: str, data_hash: str):
        """Sets the cache for a model name and its data hash."""
        if not self.client:
            return
        key = f"model:{model_name}:hash"
        self.client.set(key, data_hash)
        logger.info(f"Cached hash for {model_name}")

model_cache = ModelCache()
