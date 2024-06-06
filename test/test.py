import os
from datetime import datetime as DT

import dotenv
from dotenv import load_dotenv
from dataclutch import MongoClutch

assert os.path.exists('.env')
load_dotenv()

mongo_clutch = MongoClutch(
                        f"mongodb://{os.environ.get('CLUTCH_MONGO_ADR', '127.0.0.1')}:{os.environ.get('CLUTCH_MONGO_PORT', 27017)}/", 
                        username=os.environ.get('MONGO_USERNAME'), password=os.environ.get('MONGO_PASSWORD'))
mongo_table = mongo_clutch.table("example_table")

res = mongo_table.insert({'name': 'Alice', 'age': 29, 'updated_at':DT.now()})
print(res)
res = mongo_table.insert({'name': 'Bob', 'age': 29, 'updated_at':DT.now()})
print(res)
res = mongo_table.insert({'name': 'Charie', 'age': 29, 'updated_at':DT.now()})
print(res)
