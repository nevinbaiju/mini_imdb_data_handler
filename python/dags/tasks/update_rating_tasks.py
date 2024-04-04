from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, mean, stddev, count, round, col, rank, row_number
from pyspark.sql.window import Window
import mysql.connector
import os

def spark_aggregate():
    spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "../jars/mysql-connector-j-8.3.0.jar") \
           .getOrCreate()
    files = os.listdir('../data/dumps/')
    files = [os.path.join('../data/dumps/', file) for file in files]

    df = spark.read.csv(files, header=None).toDF(*['movie_id','title','rating','rating_old'])
    new_avg_df = df.groupBy('movie_id', 'title').agg(
                        round(mean('rating'), 2).alias('mean_rating'),
                        round(stddev('rating'), 2).alias('std_rating'),
                        count('rating').alias('rating_count')
                        )
    in_clause = ",".join([str(movie_id['movie_id']) for movie_id in new_avg_df.select('movie_id').collect()])
    mysql_df = spark.read.format("jdbc") \
                    .option("url", "jdbc:mysql://mysql:3306/movie_db") \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", "(SELECT * FROM avg_ratings WHERE movie_id IN (" + in_clause + ")) as avg_ratings_filtered") \
                    .option("user", "root") \
                    .option("password", "password") \
                    .load()
    df = new_avg_df.join(mysql_df, on=['movie_id', 'title'])
    result_df = df.withColumn('new_count', col('rating_count') + col('count'))
    result_df = result_df.withColumn('new_avg_rating',
                               round(
                                   ((col('avg_rating') * col('count')) +
                                    (col('mean_rating') * col('rating_count'))) /
                                   (col('new_count')), 2)
                              )
    rows = result_df.collect()
    row_list = [(str(row['movie_id']), str(row['title']), str(row['avg_rating']), 
                 str(row['rating_std']), str(row['count']), str(row['release_year'])) for row in rows]
    spark.stop()

    with open(f"/opt/airflow/hello_task.txt", "a") as file:
        file.write(f"{str(row_list[:4])} \n\n")