from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, mean, stddev, count, round, col, rank, row_number
from pyspark.sql.window import Window

import pandas as pd

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

def get_connection():
    return mysql.connector.connect(
            host='mysql',
            user='root',
            password='password',
            database='movie_db'
        )

def spark_aggregate(**kwargs):
    spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "../jars/mysql-connector-j-8.3.0.jar") \
           .getOrCreate()
    files = os.listdir('../data/dumps/')
    if files:
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
                        .option("dbtable", "(SELECT * FROM avg_ratings WHERE movie_id IN (" + in_clause + ")) AS avg_ratings_filtered") \
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
    kwargs['ti'].xcom_push(key='files_list', value=files)

def delete_files(**kwargs):
    ti = kwargs['ti']
    files_list = ti.xcom_pull(task_ids='spark_aggregate', key='files_list')
    with open('outputs/files', 'w') as file:
        file.write("\n".join(files_list))
    for file in files_list:
        os.remove(file)

def update_ranks():
    spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "../jars/mysql-connector-j-8.3.0.jar") \
           .getOrCreate()
    top_movies = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://mysql:3306/movie_db") \
                        .option("driver", "com.mysql.cj.jdbc.Driver") \
                        .option("dbtable", "(SELECT * FROM avg_ratings WHERE count > 5000) AS top_avg_ratings") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
    
    sorted_df = top_movies.orderBy(col("avg_rating").desc())
    windowSpec = Window.orderBy(col("avg_rating").desc())
    sorted_df = sorted_df.withColumn("movie_rank", row_number().over(windowSpec))
    rank_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://mysql:3306/movie_db") \
                        .option("driver", "com.mysql.cj.jdbc.Driver") \
                        .option("dbtable", "(SELECT movie_id, movie_rank AS old_rank, rank_diff FROM movie_ranks) AS movie_ranks") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
    rank_df = sorted_df.join(rank_df, on=['movie_id'], how='left')
    rank_df = rank_df.withColumn("rank_diff", col("old_rank") - col("movie_rank"))
    rank_df = rank_df.na.fill(0)
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

def find_top_10():
    top_10_query = "SELECT t1.title, t1.avg_rating, t1.release_year \
                    FROM avg_ratings AS t1 join movie_ranks AS t2 \
                    ON t1.movie_id = t2.movie_id\
                    ORDER BY t2.movie_rank LIMIT 10"
    top_10_trending_query = "SELECT t1.title, t1.avg_rating, t1.release_year, t2.rank_diff \
                             FROM avg_ratings AS t1 join movie_ranks AS t2 \
                             ON t1.movie_id = t2.movie_id \
                             ORDER BY t2.rank_diff DESC LIMIT 10"
    bot_10_trending_query = "SELECT t1.title, t1.avg_rating, t1.release_year, t2.rank_diff \
                             FROM avg_ratings AS t1 join movie_ranks AS t2 \
                             ON t1.movie_id = t2.movie_id \
                             ORDER BY t2.rank_diff LIMIT 10"
    conn = get_connection()
    
    top_10_df = pd.read_sql(top_10_query, conn)
    top_10_trending_df = pd.read_sql(top_10_trending_query, conn)
    bot_10_trending_df = pd.read_sql(bot_10_trending_query, conn)

    top_10_df.to_csv('outputs/top_10_movies.csv', index=None)
    top_10_trending_df.to_csv('outputs/top_10_trending.csv', index=None)
    bot_10_trending_df.to_csv('outputs/bot_10_trending.csv', index=None)