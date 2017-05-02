from collections import OrderedDict
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import pyspark as ps

import utils_db as udb


# spark = SparkSession \
#     .builder \
#     .appName("myApp") \
#     .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/titanic.train") \
#     .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/titanic.train") \
#     .getOrCreate()
#
# df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
# print df.count()

class Spark(object):
    def __init__(self, appName='test', num_cores_str='local[4]'):
        self._spark = ps.sql.SparkSession.builder \
                                         .appName(appName) \
                                         .master(num_cores_str) \
                                         .getOrCreate()

    # def convert_to_row(self, d):
    #     return Row(**OrderedDict(sorted(d.items())))

    def create_dataframe_from_list(self, items):
        # schema = StructType( [
        #     StructField('Age',IntegerType(),True),
        #     StructField('Cabin',StringType(),True),
        #     StructField('Embarked',StringType(),True),
        #     StructField('Fair',DoubleType(),True),
        #     StructField('Name',StringType(),True),
        #     StructField('Parch',IntegerType(),True),
        #     StructField('PassengerId',StringType(),True),
        #     StructField('Pclass',IntegerType(),True),
        #     StructField('Sex',StringType(),True),
        #     StructField('Survived',IntegerType(),True),
        #     StructField('Ticket',StringType(),True) ] )

        df = self._spark.sparkContext.parallelize(items) \
                                     .map(lambda d: Row(**OrderedDict(sorted(d.items())))) \
                                     .toDF()

        # rdd = self._spark.sparkContext.parallelize(items) \
        #                               .map(lambda d: Row(**OrderedDict(sorted(d.items()))))
        # df = self._spark.createDataFrame(rdd, schema)


        return df

    def join_dataframes(self, df1, df2, df1_key, df2_key):
        return df1.join(df2, df1[df1_key]==df2[df2_key])

    def get_dataframe_stats(self, df):
        pass
