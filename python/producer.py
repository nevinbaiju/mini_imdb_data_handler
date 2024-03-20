import pandas as pd
import numpy as np

from pykafka import KafkaClient

import time
import random
import json

client = KafkaClient(hosts="127.0.0.1:9092")

movie_reviews_topic = client.topics['movie_reviews']

avg_ratings = pd.read_csv('data/static_data/avg_ratings.csv')
movie_ids = avg_ratings['movie_id'].values

with movie_reviews_topic.get_sync_producer() as producer:
    while True:
    # for i in range(10):
        movie_id = random.choice(movie_ids)
        movie_id, title, avg_rating, rating_std, count = avg_ratings[avg_ratings['movie_id'] == movie_id].values[0]

        rating_dict = {}
        rating_dict['rating'] = np.ceil(max(min(np.random.normal(avg_rating, rating_std), 5), 0))
        rating_dict['old_rating'] = round(avg_rating, 2)
        rating_dict['movie_id'] = movie_id
        rating_dict['title'] = title

        message = json.dumps(rating_dict)
        producer.produce(message.encode())
        
        # time.sleep(0.1)
