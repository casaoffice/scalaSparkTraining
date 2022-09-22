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
    Logger.getLogger("org").setLevel(Level.OFF)
    //Read data

    val rating = SparkAnalytics.readDataTab("file:///C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/u.data")
    val ratings = rating.toDF(ratings_columns: _*)
    val movie = SparkAnalytics.readDataVertical("file:///C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/u.item")
    val movies = movie.toDF(movies_columns: _*)

    // Transfor Data Frame

    val ratings_movie = SparkAnalytics.getTransform(ratings, movies)

    assert(ratings_movie.count() == 100000)


    val auxDF = SparkAnalytics.getTop("Adventure", ratings_movie)
    assert(auxDF.count() == 13753)
    val summary2 = SparkAnalytics.getTop("Drama", ratings_movie)

    assert(summary2.count() == 39895)

    spark.stop()
  }
}
