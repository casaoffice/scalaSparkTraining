//package com.cei.training.scala

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.{SparkSession, types, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
import org.apache.log4j.{Level, Logger}

class SparkAnalyticsTest extends AnyFunSuite with SparkSessionTestWrapper {
  test("Test the Highest Favorites  ") {
    val ratings_columns = Seq("user_id", "item_id", "rating", "timestamp")
    val movies_columns = Seq("movie_id",
      "movie_title",
      "release_date",
      "video_release_date",
      "IMDb_URL",
      "unknown",
      "Action",
      "Adventure",
      "Animation",
      "Children\'s",
      "Comedy",
      "Crime",
      "Documentary",
      "Drama",
      "Fantasy",
      "Film-Noir",
      "Horror",
      "Musical",
      "Mystery",
      "Romance",
      "Sci-Fi",
      "Thriller",
      "War",
      "Western"
    )


    // user data set
    val path = "file//C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/"
    val ratings = spark.read.format("csv").option("header", "false")
      .option("sep", "\t")
      .option("escape", "\t")
      .csv("file:///C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/u.data")
      .toDF(ratings_columns: _*)

    val movies = spark.read.format("csv").option("header", "false")
      .option("sep", "|")
      .option("escape", "\t")
      .csv("file:///C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/u.item")
      .toDF(movies_columns: _*)

    val ratings_date = ratings.select(col("user_id"), col("item_id"), col("rating"), col("timestamp"),
      (from_unixtime(col("timestamp"))).as("timestamp5")
      , (year(from_unixtime(col("timestamp"))).as("year"))
      , (month(from_unixtime(col("timestamp"))).as("month"))
    )


    val ratings_movie = ratings_date.join(movies, ratings_date("item_id") === movies("movie_id"), "inner")
    //print(ratings_movie.head())
    //ratings_movie.show()
    val ratings_all = ratings_movie.groupBy("item_id").agg(count("movie_id").alias("counter")).sort(desc("counter"))
    val ratings_year = ratings_movie.groupBy("item_id", "year").count().sort()
    val ratings_year_month = ratings_movie.groupBy("item_id", "year", "month").count().sort()



    val auxDF= SparkAnalytics.getTop("Adventure",ratings_movie)
    assert (auxDF.count()== 13753)
    val test2 = SparkAnalytics.getTop("Drama", ratings_movie)
    print(test2.count())
    assert(test2.count() == 8055)
    //assert(ss == 1)
  }
}
