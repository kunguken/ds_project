import csv
import pprint
import pymongo
import pandas as pd
import matplotlib.pyplot as plt

from pymongo import MongoClient
from collections import defaultdict

numeric_fields = ['Survived', 'Age', 'Pclass', 'SibSp', 'Parch', 'Fare']

def get_or_create_db(dbname, drop_on_exist=False):
    """ Create a MongoDB database or get the database if it exists already
    Args:
        dname (str): database name
        drop_on_exist (bool): If True, drop the database if it exists already
    Returns:
        db: the created or existing database
    """
    client = MongoClient()
    if dbname in client.database_names():
        if drop_on_exist:
            client.drop_database(dbname)
    db = client[dbname]
    return db


# def read_from_csvfile(fname):
#     with open(fname) as f:
#         reader = csv.DictReader(f)
#         return list(reader)


def read_from_csvfile(fname, types, header=True):
    """ Read from a csvfile with data types of the columns specified
    Args:
        fname (str): csvfile name
        types (list of type): a list of data types
    Returns:
        [dictionaries]: a list of dictionaries where each dictionary corresponds
                        to a row in the input csvfile
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


def insert_to_collection(db, coll_name, docs):
    """ Insert items into a collection
    Args:
        db: MongoDB database
        coll_name (str): collection name
        docs ([dictionaries]): a list of dictionaries to be inserted
    Returns:
        None
    """
    if isinstance(docs, list):
        db[coll_name].insert_many(docs)
    else:
        db[coll_name].insert_one(docs)


def read_from_collection(db, coll_name):
    """ Read all data from a collection
    Args:
        db: MongoDB database
        coll_name (str): collection name
    Returns:
        [dictionaries]: a list of dictionaries
    """
    return db[coll_name].find({}, {'_id': False})


def pretty_print_stats(stats_dict):
    """ Print statistics
    Args:
        stats_dict (dict): dictionary of statistics data
    Returns:
        None
    """
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
    """ Create a pandas DataFrame from a numpy array
    Args:
        data: a numpy array of statistics
        index ([str]): a list of index names
        columns ([str]): a list of column names
    Returns:
        a pandas DataFrame
    """
    return pd.DataFrame(data, index, columns)

def get_histogram_text(ax, data, keys, col):

    X = range(0, len(keys))
    ax.bar(X, data, align='center', width=0.5)
    ax.set_xticks(X)
    ax.set_xticklabels(keys)
    ax.set_title(col)
    return ax

def get_histogram_numeric(ax, data, bin, col):
    ax.hist(data, bins=bin)
    ax.set_title(col)
    return ax

def get_histogram(df, columns, bins=10):

    plt.rc('figure', figsize=(12,5))

    if type(columns) is str:
        fig, axe = plt.subplots(1,1)
        columns, axe, bins = [columns], [axe], [bins]
    else:

        fig, axe = plt.subplots(1, len(columns))
        if type(bins) is int:
            bins = [bins] * len(columns)


    for col, ax, bin in zip(columns, axe, bins):
        if col in numeric_fields:
            data = df.select(col).dropna().rdd.flatMap(lambda x :x).collect()
            get_histogram_numeric(ax, data, bin, col)

        else:
            keys = df.select(col).dropna().rdd.flatMap(lambda x :x).countByValue().keys()
            data = df.select(col).dropna().rdd.flatMap(lambda x :x).countByValue().values()
            get_histogram_text(ax, data, keys, col)

    plt.tight_layout()
    plt.show()
