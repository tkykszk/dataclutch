""" DB抽象化レイヤー


"""
import os
import sys
import operator as op
import yaml
import logging
from abc import ABC, abstractmethod
from typing import Tuple
from pprint import *
import datetime

# pip
import boto3
import pymongo
from pymongo import MongoClient

#sys.path.append(os.path.split(__file__)[0])
#pprint(sys.path)
#from . import querybuilder
from .querybuilder import QueryBuilder

class Table(object):
    @abstractmethod
    def insert(self, data):
        pass

    @abstractmethod
    def find(self, conditions):
        pass

    @abstractmethod
    def find_one(self, conditions):
        pass

    @abstractmethod
    def update(self, query, data):
        pass

    @abstractmethod
    def delete(self, query):
        pass

    @abstractmethod
    def upsert(self, conditions, data):
        pass

# MongoDBのコマンド
comamnds = set(['$set', '$unset', '$inc', '$push', '$pull', '$addToSet', '$rename', '$min', '$max', '$currentDate'])

class MongoTable:

    def __init__(self, clutch, table_name, collection):
        self.clutch = clutch
        self.collection = collection
        self.table = table_name

    def __del__(self):
        # detach from the manager class.
        # sensitive memory handling.
        del self.clutch.tables[self.table]

    def insert(self, data):
        return self.collection.insert_one(data)

    def find_one(self, conditions: Tuple):
        """
        {"name": "John"}
        """
        return self.collection.find_one(QueryBuilder(conditions).getquery())

    def find(self, conditions):
        """
        {"city": "New York"}        
        """
        q = QueryBuilder(conditions).getquery()
        return self.collection.find(q)

    def update(self, query, command):
        """

        result:
        	update_result.matched_countは、条件に一致したドキュメントの数を示します。
	    	update_result.modified_countは、実際に更新されたドキュメントの数を示します。

        $set: {"name": "John"}
        $unset: {"name": ""}
        $inc: {"age": 1}
        $push: {"hobbies": "hiking"}
        $pull: {"hobbies": "hiking"} #条件に一致するものを削除
        $addToSet: {"hobbies": "hiking"} #重複を許さない
        $rename: {"old_name": "new_name"}
        $min: {"score": 90}
        $max: {"score": 90}
        $currentDate: {"lastModified": True}   # 
        """
        return self.collection.update_many(query, command)

    def upsert(self, query, command):
        """
        一旦抽象化なしでmongodb直接
        """
        system_command = [key for key in command.keys() if key[0] == '$']  # $なんちゃらがあるか
        if len(system_command) == 0:
            # commandが存在しない.
            command = {"$set": command}
        return self.collection.update_many(query, command, upsert=True)

    def delete(self, query):
        q = QueryBuilder(query).getquery()
        return self.collection.delete_many(q)

class MongoClutch(object):

    def __init__(self, uri, username, password):
        self.client = MongoClient(uri, username=username, password=password)
        self.db = self.client.database
        #self.collection = self.db[self.table_name]
        self.tables = {}

    def table(self, table_name) -> MongoTable:
        tbl = MongoTable(self, table_name, self.db[table_name])
        self.tables[table_name] = tbl
        return tbl

class DynamoClutch(object):
    def __init__(self, table, region):
        self.dynamodb = boto3.resource('dynamodb', region_name=config.get('CLUTCH_DYNAMO_REGION'))
        self.table = self.dynamodb.Table(table)

    def insert(self, data):
        return self.table.put_item(Item=data)

    def find(self, conditions):
        key_conditions = {}
        value_conditions = []
        logical_op = 'and'
        for condition in conditions:
            if isinstance(condition, list):
                key, op_func, value = condition
                if key in self.table.key_schema:
                    key_conditions[f'{key}__op'] = op_func.__name__
                    key_conditions[key] = value
                else:
                    value_conditions.append(f'{key} {op_func.__name__} {value}')
            else:
                logical_op = condition.__name__
        key_expression = ' and '.join([f'{key} {op}' for key, op in key_conditions.items() if key.endswith('__op')])
        filter_expression = f' {logical_op} '.join(value_conditions)
        response = self.table.query(KeyConditionExpression=key_expression, FilterExpression=filter_expression)
        return response['Items']

    def update(self, query, data):
        key = {k: query[k] for k in self.table.key_schema}
        return self.table.update_item(Key=key, AttributeUpdates=data)

    def delete(self, query):
        key = {k: query[k] for k in self.table.key_schema}
        return self.table.delete_item(Key=key)

config_default_yaml = """\
CLUTCH_MONGO_REGION: ap-northeast-1
CLUTCH_MONGO_ADR: 127.0.0.1
CLUTCH_MONGO_PORT: 27017
CLUTCH_DYNAMO_REGION: ap-northeast-1
CLUTCH_DYNAMO_ADR: ''
CLUTCH_DYNAMO_PORT: None
MONGO_USERNAME: root
MONGO_PASSWORD: b7deb5ddbcaeb3bcdeac
"""

config = yaml.safe_load(config_default_yaml)

if __name__ == '__main__':
    
    from datetime import timezone, datetime as DT
    from pymongo import MongoClient
    import boto3

    def NOW(msec=0):
        return DT.now(tz=timezone.utc).replace(microsecond=msec)

    mongo_clutch = MongoClutch(
                               f"mongodb://{config.get('CLUTCH_MONGO_ADR', '127.0.0.1')}:{config.get('CLUTCH_MONGO_PORT', 27017)}/", 
                               username=config.get('MONGO_USERNAME'), password=config.get('MONGO_PASSWORD'))
    mongo_table = mongo_clutch.table("example_table")

    if False:
        dynamo_clutch = DynamoClutch(table="example_table", region={config.get('CLUTCH_MONGO_REGION')})

    # 全部
    print("🔵find all")
    for doc in mongo_table.find(()):
        print(doc)
    print("🔵🔚")

    # MongoDB に挿入
    mongo_table.insert({'name': 'Alice', 'age': 29, 'updated_at':NOW()})
    mongo_table.insert({'name': 'Bob', 'age': 30, 'updated_at':NOW()})
    mongo_table.insert({'name': 'Charie', 'age': 31, 'updated_at':NOW()})
    mongo_table.insert({'name': 'John', 'age': 32, 'updated_at':NOW()})

    result = mongo_table.upsert({'name': 'David'}, {'name': 'David', 'age': 32, 'updated_at':NOW()})
    print(result)

    if False:
        # DynamoDB に挿入
        dynamo_clutch.insert({'name': 'Alice', 'age': 55})
        dynamo_clutch.insert({'name': 'Bob', 'age': 56})
        dynamo_clutch.insert({'name': 'Charie', 'age': 57})

    # MongoDB から検索 
    for doc in mongo_table.find(('==', 'age', 30)):
        print(doc)

    if False:
        # DynamoDB から検索
        for item in dynamo_clutch.find(['age', op.le, 56]):
            print(item)

    # MongoDB から特定条件削除
    ## result = mongo_table.delete(('<=', 'age', 40))
    ## print(result)

    del mongo_table

    print("🔵bye")
