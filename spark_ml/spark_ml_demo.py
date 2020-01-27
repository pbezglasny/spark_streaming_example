from pyspark.sql import *


def main():
    spark = SparkSession.builder.master("local[2]").getOrCreate()


if __name__ == '__main__':
    main()
