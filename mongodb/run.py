from pyspark.sql.types import *
import pprint

from spark_obj import SparkMongoDB, SparkLocal
import utils


def load_all_data(db, coll_name):
    # read all items from collection 'train', which is a list of dictionaries
    train_docs = utils.read_from_collection(db, coll_name)

    spark = SparkLocal()
    df = spark.create_dataframe(train_docs)
    return spark, df

def load_with_mongodb_connector(dbname, coll_name):
    spark = SparkMongoDB(dbname, coll_name)
    df = spark.create_dataframe()
    return spark, df


if __name__ == '__main__':
    dbname = 'titanic'
    f_train = 'data/train.csv'
    f_test = 'data/test.csv'
    numeric_fields = ['Survived', 'Age', 'Pclass', 'SibSp', 'Parch', 'Fare']
    text_fields = ['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked']

    db = utils.create_db(dbname, drop_on_exist=True)

    # PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
    col_types_train = [str, int, int, str, str, float, int, int, str,float, str, str]
    # PassengerId,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
    col_types_test = [str, int, str, str, float, int, int, str,float, str, str]

    utils.insert_to_collection(db, 'train', utils.read_from_csvfile(f_train, col_types_train))
    utils.insert_to_collection(db, 'test', utils.read_from_csvfile(f_test, col_types_test))

    coll_name = 'train'
    # command line: spark-submit run.py
    spark, df = load_all_data(db, coll_name)

    # command line: spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 run.py
    # spark, df = load_with_mongodb_connector(dbname, coll_name)

    print 'dataframe count: {}'. format(df.count())
    print df.take(2)
    df.printSchema()
    df.describe().toPandas().transpose()

    stats_numeric = spark.get_stats_summary_numeric_fields(df, numeric_fields)
    utils.pretty_print_stats(stats_numeric)
    stats_text = spark.get_stats_summary_text_fields(df, text_fields)
    utils.pretty_print_stats(stats_text)

    print
    corr_matrix = spark.get_correlation_matrix(df, numeric_fields)
    df_corr = utils.create_pandas_dataframe(corr_matrix, numeric_fields, numeric_fields)
    print df_corr
    print
    cov_matrix = spark.get_covariance_matrix(df, numeric_fields)
    df_cov = utils.create_pandas_dataframe(cov_matrix, numeric_fields, numeric_fields)
    print df_cov
