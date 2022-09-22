//package com.cei.training.scala

import org.apache.spark.sql._
import org.apache.spark.sql.{SparkSession, types, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
import org.apache.log4j.{Level, Logger}

//import org.apache.spark.sql.functions._
//import sqlContext.implicits._

// This program was created to compute some analytics for a data set that shows movies and users data sets.
// Gets informations about moviews, users, and ratings.


object SparkAnalytics extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val logFile = "file//C:/Users/ojuarezespinosa/Documents/projects/spark.txt"
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    // Define columns for data sets
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
    val user_columns = Seq("user_id", "age", "gender", "occupation", "zip_code")
    // Movies Genres
    val occupations_columns = Seq("SparkAnalytics")
    // Geners for the movies
    val genres_columns = Seq("genre_name")
    // Generes Names
    val genres_names = Seq("unknown", "Action", "Adventure", "Animation", "Children's", "Comedy",
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


    Logger.getLogger("org").setLevel(Level.OFF)

    //Read data

    val rating = readDataTab("file:///C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/u.data")
    val ratings = rating.toDF(ratings_columns: _*)
    val movie = readDataVertical("file:///C:/Users/ojuarezespinosa/Documents/projects/spark-analytics/data/MovieLens/u.item")
    val movies = movie.toDF(movies_columns: _*)

    // Transfor Data Frame

    val ratings_movie = getTransform(ratings, movies)

    // Get top ten and the top low movies by Gener

    getTop("Action", ratings_movie)

    getTop("Adventure", ratings_movie)

    getTop("Animation", ratings_movie)

    getTop("Children's", ratings_movie)
    getTop("Crime", ratings_movie)
    getTop("Drama", ratings_movie)
    getTop("Fantasy", ratings_movie)
    getTop("Film-Noir", ratings_movie)
    getTop("Horror", ratings_movie)
    getTop("Musical", ratings_movie)
    getTop("Romance", ratings_movie)
    getTop("Sci-Fi", ratings_movie)


    spark.stop()
  }


  def readDataTab(path: String): DataFrame = {

    val sqlContext = spark.sqlContext


    val df = sqlContext.read.format("csv").option("header", "false")
      .option("sep", "\t")
      .option("escape", "\t")
      .csv(path)
    df

  }

  def readDataVertical(path: String): DataFrame = {

    val sqlContext = spark.sqlContext


    val df = sqlContext.read.format("csv").option("header", "false")
      .option("sep", "|")
      .option("escape", "\t")
      .csv(path)
    df

  }

  def getTransform(ratings: DataFrame, movies: DataFrame): DataFrame = {

    val ratings_date = ratings.select(col("user_id"), col("item_id"), col("rating"), col("timestamp"),
      (from_unixtime(col("timestamp"))).as("timestamp5")
      , (year(from_unixtime(col("timestamp"))).as("year"))
      , (month(from_unixtime(col("timestamp"))).as("month"))
    )
    val ratings_movie = ratings_date.join(movies, ratings_date("item_id") === movies("movie_id"), "inner")
    ratings_movie
  }


  def getTop(category: String, ratings_movie: DataFrame): DataFrame = {

    //print("Printing   " + category + "\n")
    val auxDF = ratings_movie.where(ratings_movie(category) === 1)
    print("Gener " + category + "\n")
    print("Most Popular Movies \n")
    auxDF.groupBy("movie_title", "year").agg(avg("rating").alias("Mean")).sort(desc("Mean")).show(10)
    print("Least Popular Movies \n")
    auxDF.groupBy("movie_title", "year").agg(avg("rating").alias("Mean")).sort(asc("Mean")).show(10)
    print(auxDF.count())
    auxDF


  }
}