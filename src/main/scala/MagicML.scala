import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset}
 object MagicML {


  var  userId: Int = 0
  var  movies: DataFrame = null
  var  ratings: DataFrame = null
  var  personalNormalizedRatings: DataFrame = null

  def train(): MatrixFactorizationModel ={


    val sc = Context.getCtx()
    import sc.implicits._


    //Splitting training data 90% for training & 10% for testing
    val set = ratings.randomSplit(Array(0.9, 0.1), seed = 12345)
    val training = personalNormalizedRatings.union(set(0)).cache()
    val test = set(1).cache()


    println(s"Training: ${training.count()}, test: ${test.count()}")



    val trainRDD = training.as[Rating].rdd
    val rank = 10
    val numIterations = 100

    //Training the recommendation model using ALS
    ALS.train(trainRDD, rank, numIterations, 0.01)

  }



   // predict a user recommendations
  def predict(userId: Int, model : MatrixFactorizationModel): Dataset[Rating] = {
    val sc = Context.getCtx()
    import sc.implicits._

    val usersProducts = movies.select(lit(userId), col("movieId")).map{
      row => (row.getInt(0), row.getInt(1))
    }

    model.predict(usersProducts.toJavaRDD).toDS()


  }


}
