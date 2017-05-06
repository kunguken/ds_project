from collections import OrderedDict, defaultdict
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import pyspark as ps
import numpy as np


class SparkObj(object):
    def join_dataframes(self, df1, df2, df1_key, df2_key):
        return df1.join(df2, df1[df1_key]==df2[df2_key])

    def get_stats_summary_numeric_fields(self, df, columns):
        stats = defaultdict(dict)
        for col in columns:
            stats[col]['mean'] = df.select([F.mean(col)]).first()[0]
            stats[col]['max'] = df.select([F.max(col)]).first()[0]
            stats[col]['min'] = df.select([F.min(col)]).first()[0]
            stats[col]['median'] = df.approxQuantile(col, [0.5], 0.10)[0]
        return stats

    def get_stats_summary_text_fields(self, df, columns):
        stats = defaultdict(dict)
        for col in columns:
            stats[col]['word_counts'] = (df.select(col)
                                        .rdd
                                        .flatMap(lambda line: line[0].split() if line[0] else ['MissingValue'])
                                        .map(lambda word: (word, 1))
                                        .reduceByKey(lambda a, b: a + b)
                                        .sortBy(lambda a: a[1], ascending=False).collect())
            stats[col]['max'] = stats[col]['word_counts'][0]
            stats[col]['min'] = stats[col]['word_counts'][-1]
        return stats

    def get_correlation_matrix(self, df, columns):
        dim = len(columns)
        corr = np.ones((dim, dim))
        for i in range(dim):
            for j in range(dim):
                if j > i:
                    corr[i, j] = df.stat.corr(columns[i], columns[j])
                    corr[j, i] = corr[i, j]
        return corr

    def get_covariance_matrix(self, df, columns):
        dim = len(columns)
        cov = np.ones((dim, dim))
        for i in range(dim):
            for j in range(dim):
                if j > i:
                    cov[i, j] = df.stat.cov(columns[i], columns[j])
                    cov[j, i] = cov[i, j]
        return cov


class SparkMongoDB(SparkObj):
    def __init__(self, dbname, collection, app_name='test', master_url='local[4]'):
        uri = dbname + '.' + collection
        self._spark = (SparkSession
                        .builder
                        .appName(app_name)
                        .master(master_url)
                        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/{}".format(uri))
                        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/{}".format(uri))
                        .getOrCreate())

    def create_dataframe(self):
        df = self._spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
        return df


class SparkLocal(SparkObj):
    def __init__(self, app_name='test', master_url='local[4]'):
        self._spark = (SparkSession.builder
                                  .appName(app_name)
                                  .master(master_url)
                                  .getOrCreate())

    def create_dataframe(self, l_dicts):
        df = (self._spark.sparkContext.parallelize(l_dicts)
                                     .map(lambda d: Row(**OrderedDict(sorted(d.items()))))
                                     .toDF())
        return df
