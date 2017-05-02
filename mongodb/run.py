from spark_obj import Spark
import utils_db as udb
import utils_spark as usp

dbname = 'titanic'
f_train = 'data/train.csv'
f_test = 'data/test.csv'

db = udb.create_db(dbname, drop_on_exist=True)

udb.insert_to_collection(db, 'train', udb.read_from_csvfile(f_train))
udb.insert_to_collection(db, 'test', udb.read_from_csvfile(f_test))

train_doc = db.train.find_one()
udb.print_docs(train_doc)

test_doc = db.test.find_one()
udb.print_docs(test_doc)

# read all items from collection 'train', which is a list of dictionaries
train_docs = udb.read_from_collection(db, 'train')

# create spark dataframe from a list of dictionaries
spark = Spark()
df = spark.create_dataframe_from_list(train_docs)

df_ = df.select(df.Age.cast(IntegerType()).alias('Age'),
                df.Cabin.cast(StringType()).alias('Cabin'),
                df.Embarked.cast(StringType()).alias('Embark'),
                df.Fare.cast(DoubleType()).alias('Fare'),
                df.Name.cast(StringType()).alias('Name'),
                df.Parch.cast(IntegerType()).alias('Parch'),
                df.PassengerId.cast(StringType()).alias('PassengerId'),
                df.Pclass.cast(IntegerType()).alias('Pclass'),
                df.Sex.cast(StringType()).alias('Sex'),
                df.Survived.cast(IntegerType()).alias('Survived'),
                df.Ticket.cast(StringType()).alias('Ticket'))

print 'dataframe count: {}'. format(df_.count())
print df_.head()
df_.printSchema()
df_.describe().show()
