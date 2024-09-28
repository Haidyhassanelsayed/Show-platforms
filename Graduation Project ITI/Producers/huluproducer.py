import csv
import json
import time
from confluent_kafka import Producer
import pandas as pd
producer = Producer({'bootstrap.servers': 'localhost:9092'})
time_limit = 60
start_time = time.time()
elapsed_time = time.time() - start_time


while elapsed_time < time_limit:
    with open('hulu_titles.csv','r' ,encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row  = json.dumps(row)
            producer.produce('movies', key='hulu', value=row)
            print(row)
            producer.flush()
            time.sleep(3)

