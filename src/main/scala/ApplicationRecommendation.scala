
import MagicML.movies
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}


object Context {

  def getCtx() : SparkSession = {
    SparkSession.builder.
    master("local[*]")
    .appName("spark session example")
    .getOrCreate()

  }

}

object ApplicationRecommendation extends App {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = Context.getCtx()
  import sc.implicits._





  var ratings  = sc.read
                 .format("csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .load("src/main/resources/ratings.csv")
                 .drop(col("timestamp"))




  var movies  = sc.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/movies.csv")
    .withColumn("genres", split(col("genres"), "\\|"))


  //movies.show(5)



  val personalRatings = Seq(
    ("Toy Story (1995)", 5.0),
    ("Saving Private Ryan (1998)", 1.0),
    ("Sixth Sense, The (1999)", 1.0),
    ("Ace Ventura: When Nature Calls (1995)", 1.0),
    ("Aladdin (1992)", 5.0),
    ("Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)", 1.0),
    ("Jumanji (1995)", 4.0),
    ("Mortal Kombat (1995)", 1.0),
    ("Inferno (1980)", 1.0),
    ("Pocahontas (1995)", 5.0),
    ("Wrong Trousers, The (1993)", 1.0),
    ("Balto (1995)", 5.0),
    ("Godfather, The (1972)", 1.0),
    ("Silence of the Lambs, The (1991)", 2.0),
    ("Indiana Jones and the Last Crusade (1989)", 1.0),
    ("Heat (1995)", 1.0),
    ("Fugitive, The (1993)", 1.0),
    ("Man of the Year (1995)", 5.0)
  ).toDF("title", "rating")




  // Convert ratings above into model-friendly form
  // User ID will have special value of "0"

  var userId = 0

  var normlizedPersonalRatings = personalRatings.
    join(movies, "title").
    select(lit(userId).as("user"), col("movieId").as("product") , col("rating"))

 // normlizedPersonalRatings.show(5)


  MagicML.userId = userId
  MagicML.movies = movies
  MagicML.ratings = ratings
  MagicML.personalNormalizedRatings = normlizedPersonalRatings

  val model = MagicML.train()

  val usersProducts = movies.select(lit(userId), col("movieId")).map{
    row => (row.getInt(0), row.getInt(1))
  }

  model.predict(usersProducts.rdd).toDS()

  val result = MagicML.predict(userId, model)
  val df = result.filter(r => r.user == userId)

  val recommendationList = df.toDF().sort(col("rating").desc).join(movies, movies("movieId") === df("product"), "inner")
  recommendationList.select("movieId", "title", "genres").show()




}
