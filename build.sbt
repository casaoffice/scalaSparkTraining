name := "spark-Analytics"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1",
  //scalaTest % Test,
  "org.apache.spark" %% "spark-sql" % "2.0.1",
  "org.scalactic" %% "scalactic" % "3.2.13",
  //"org.scalatest" %% "scalatest" % "3.2.13" % "test"
  "org.scalatest" %% "scalatest" % "3.2.11" % Test
)
// Could add other dependencies here e.g.
// libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.1"
