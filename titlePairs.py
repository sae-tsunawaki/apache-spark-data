import pyspark
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType 
from pyspark.sql.functions import col

# Required for spark-submit
sc = SparkContext("local", "TitlePairs")
spark = SparkSession(sc)

# A regular expression for parsing data from part1.dat
TUPLE = re.compile(r'\(\((\d+), (\d+)\), (\d+)\)')

titles_schema = StructType() \
    .add("book_id", IntegerType(), True) \
    .add("title", StringType(), True)

# A function that creates DataFrame rows from a line of text from
# part1.dat.
def parse_tuple(t: str) -> pyspark.sql.Row:
    book1, book2, frequency = TUPLE.search(t).groups()
    return pyspark.sql.Row(book1=int(book1),
              book2=int(book2),
              frequency=int(frequency))

# Read in the tab-delimited goodreads_titles.dat file.
titles_df = spark.read.format("csv") \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .schema(titles_schema) \
        .load("/home/cs143/data/goodreads_titles.dat")
titles_df.show()


# Read in the file into an RDD and apply parse_tuple over the entire
# RDD.
part1_df = spark.createDataFrame(sc.textFile("/home/cs143/data/part1.dat").map(parse_tuple))

titlePair = (part1_df.join(titles_df, part1_df.book1 == titles_df.book_id) \
                        .withColumnRenamed("book_id", "book1_id") \
                        .withColumnRenamed("title", "book1_title") \
                        .join(titles_df, part1_df.book2 == titles_df.book_id) \
                        .withColumnRenamed("title", "book2_title") \
                        .select("book1_title", "book2_title", "frequency") \
                        .orderBy(col("frequency").desc(), col("book1_title"), col("book2_title")))

titlePair.show()

titlePair.write.option("header", False).option("delimiter","\t").csv("/home/cs143/output2")
