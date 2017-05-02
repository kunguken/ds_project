import csv
import pprint
import pymongo

from pymongo import MongoClient


def create_db(dbname, drop_on_exist=False):
    client = MongoClient()
    if dbname in client.database_names():
        if not drop_on_exist:
            raise Exception("database {} already exists!".format(dbname))
        client.drop_database(dbname)
    db = client[dbname]
    return db


def read_from_csvfile(fname):
    with open(fname) as f:
        reader = csv.DictReader(f)
        return list(reader)


def insert_to_collection(db, coll_name, items):
    if isinstance(items, list):
        db[coll_name].insert_many(items)
    else:
        db[coll_name].insert_one(items)


def print_docs(docs):
    if isinstance(docs, list):
        for doc in docs:
            pprint.pprint(doc)
    else:
        pprint.pprint(docs)


def read_from_collection(db, coll_name):
    return db[coll_name].find({}, {'_id': False})
