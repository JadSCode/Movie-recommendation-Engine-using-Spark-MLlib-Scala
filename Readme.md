# Movie recommendation Engine using Spark MLlib & Scala

Use MovieLens dataset to build a movie recommender engine using collaborative filtering with Spark's Alternating Least Saqures implementation.

The project is organized in two parts :

 1. Parsing movies & ratings data into Spark DataFrames
 2. Train , test and validate the model using MLlib and predict recommendations based on a personal ratings list of movies



### Prerequisites

 1. Download [MovieLens DataSet](https://grouplens.org/datasets/movielens/)
 2. Move ratings.csv and movies.csv to `src/main/resources/`

```
scalaVersion := "2.11.4"
```

### build.sbt

```
name := "RecommendationSystem"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies ++= {
  val sparkVer = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % "2.1.0"
  )
}
```
