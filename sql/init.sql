CREATE DATABASE IF NOT EXISTS movie_db;

USE movie_db;

CREATE TABLE IF NOT EXISTS avg_ratings (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    avg_rating DECIMAL(3, 2),
    rating_std DECIMAL(3, 2),
    count INT
);

LOAD DATA INFILE '/var/lib/mysql-files/avg_ratings.csv'
INTO TABLE avg_ratings
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(movie_id, title, avg_rating, rating_std, count);