from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, mean, stddev, count, round, col, rank, row_number
from pyspark.sql.window import Window
import mysql.connector
import os

def write_avg_ratings(rows):
    connection = mysql.connector.connect(
        host="mysql",
        port=3306,
        user="root",
        password="password",
        database="movie_db"
    )
    cursor = connection.cursor()
    for data in rows:
        query = "UPDATE avg_ratings SET avg_rating = %s, count = %s WHERE movie_id = %s"
        cursor.execute(query, (data[2], data[4], data[0]))
    connection.commit()
    connection.close()  

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
    write_avg_ratings(row_list)

def update_ranks():
    spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "../jars/mysql-connector-j-8.3.0.jar") \
           .getOrCreate()
    top_movies = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://mysql:3306/movie_db") \
                        .option("driver", "com.mysql.cj.jdbc.Driver") \
                        .option("dbtable", "(SELECT * FROM avg_ratings WHERE count > 5000) as top_avg_ratings") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
    
    sorted_df = top_movies.orderBy(col("avg_rating").desc())
    windowSpec = Window.orderBy(col("avg_rating").desc())
    sorted_df = sorted_df.withColumn("movie_rank", row_number().over(windowSpec))
    rank_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://mysql:3306/movie_db") \
                        .option("driver", "com.mysql.cj.jdbc.Driver") \
                        .option("dbtable", "(SELECT movie_id, movie_rank as old_rank, rank_diff FROM movie_ranks) as movie_ranks") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
    rank_df = sorted_df.join(rank_df, on=['movie_id'], how='left')
    rank_df = rank_df.withColumn("rank_diff", col("old_rank") - col("movie_rank"))
    connection = mysql.connector.connect(
        host="mysql",
        port=3306,
        user="root",
        password="password",
        database="movie_db"
    )
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE movie_ranks")
    connection.commit()

    rank_df = rank_df.select('movie_id', 'movie_rank', 'rank_diff')
    rank_df.write.format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/movie_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "movie_ranks") \
        .option("user", "root") \
        .option("password", "password") \
        .mode("append") \
        .save()