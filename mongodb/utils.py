import csv
import pprint
import pymongo
import pandas as pd

from pymongo import MongoClient
from collections import defaultdict


def create_db(dbname, drop_on_exist=False):
    client = MongoClient()
    if dbname in client.database_names():
        if not drop_on_exist:
            raise Exception("database {} already exists!".format(dbname))
        client.drop_database(dbname)
    db = client[dbname]
    return db


# def read_from_csvfile(fname):
#     with open(fname) as f:
#         reader = csv.DictReader(f)
#         return list(reader)


def read_from_csvfile(fname, types, header=True):
    """
    """
    num_columns = len(types)
    parsed = []
    with open(fname) as f:
        reader = csv.reader(f)
        if header:
            columns = next(reader)
        else:
            columns = ['col '+str(i) for i in range(num_columns)]

        for row in reader:
            parsed.append([types[i](row[i]) if row[i] else None for i in range(num_columns)])

    return [{k:v for k,v in zip(columns, row)} for row in parsed]


def insert_to_collection(db, coll_name, items):
    if isinstance(items, list):
        db[coll_name].insert_many(items)
    else:
        db[coll_name].insert_one(items)


def read_from_collection(db, coll_name):
    return db[coll_name].find({}, {'_id': False})


def pretty_print_stats(stats_dict):
    d = stats_dict.copy()
    num_elements_to_print = 10
    for field in d:
        print
        print "Field: {}".format(field)
        stats = d[field]
        for stat_name, stat in stats.items():
            if isinstance(stat, list) and len(stat) > num_elements_to_print * 2:
                stats[stat_name] = stat[:num_elements_to_print] + ['...'] + stat[-num_elements_to_print:]
        pprint.pprint(d[field])


def create_pandas_dataframe(data, index, columns):
    return pd.DataFrame(data, index, columns)
