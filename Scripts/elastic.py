import pandas as pd
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")


df = pd.read_csv("Data/2023_Jan_Data.csv", low_memory=False)
    
print(df.isna().sum())
