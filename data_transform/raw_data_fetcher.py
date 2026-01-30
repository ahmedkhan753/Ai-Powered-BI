import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

database_url = "postgresql://admin:password123@postgres:5432/bi_warehouse"

def raw_data_fetcher():
    pass