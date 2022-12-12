package ind.phyrrid.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class MongoConfig(uri:String, db:String)

object StatisticsRecommender {
  object Tasks {
    //    结果表
    val RES_ANNUAL_TOP_ALBUMS = "AnnualTopAlbums" // 年度最佳人气专辑
    val RES_TOP_ALBUMS = "TopAlbums" //最佳人气专辑
    val RES_ANNUAL_FAVE_SINGERS = "AnnualFaveSingers" // 年度最受听众喜爱歌手
    val RES_FAVE_SINGERS = "FaveSingers" //最受听众喜爱歌手
    val RES_ANNUAL_FAVE_GENRES = "AnnualFaveGenres" // 年度最受听众喜爱音乐类型
    val RES_FAVE_GENRES = "AnnualFaveGenres" //最受听众喜爱音乐类型

    //   输入表

    //    配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster(Tasks.config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(Tasks.config("mongo.uri"), Tasks.config("mongo.db"))

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

  }
}
