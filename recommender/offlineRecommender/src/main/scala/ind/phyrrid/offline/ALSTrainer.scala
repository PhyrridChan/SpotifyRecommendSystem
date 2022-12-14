package ind.phyrrid.offline

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MongoConfig(uri: String, db: String)

object ALSTrainer {
  object Tasks {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
  }

  private val logger = Logger.getLogger("ALSTrainer")

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster(Tasks.config("spark.cores")).setAppName("ALSTrainer")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig: MongoConfig = MongoConfig(Tasks.config("mongo.uri"), Tasks.config("mongo.db"))

    val track_artist_genreDF = loadRequiredData(spark)

    val source = scala.io.Source.fromURL(s"https://api.day.app/ZCs6nE83mAH4yLjn7YriUL/test/done")
    source.close()
    spark.stop()
  }

  def loadRequiredData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val tracksDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "tracks")
      .format("com.mongodb.spark.sql")
      .load()

    val audio_featuresDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "audio_features")
      .format("com.mongodb.spark.sql")
      .load()

    val tracks_audio_featuresDF = audio_featuresDF.join(tracksDF, audio_featuresDF.col("id") ===
      tracksDF.col("audio_feature_id")).select(
      tracksDF.col("id") as "track_id", $"audio_feature_id", $"popularity",
      $"acousticness", $"danceability", audio_featuresDF.col("duration"), $"energy", $"instrumentalness", $"key", $"liveness", $"loudness", $"mode", $"speechiness", $"tempo", $"time_signature", $"valence",
    )

    val artistsDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "artists")
      .format("com.mongodb.spark.sql")
      .load()

    val r_artist_genreDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "r_artist_genre")
      .format("com.mongodb.spark.sql")
      .load()

    val artist_genreDF = r_artist_genreDF.join(artistsDF, artistsDF.col("id") ===
      r_artist_genreDF.col("artist_id")).select(
      $"artist_id", $"genre_id".as("genre"))


    val r_track_artistDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "r_track_artist")
      .format("com.mongodb.spark.sql")
      .load()

    val track_to_artist_genreDF = r_track_artistDF.join(artist_genreDF, artist_genreDF.col("artist_id") ===
      r_track_artistDF.col("artist_id")).select(
      r_track_artistDF.col("artist_id"), $"track_id", $"genre")

    val track_artist_genreDF = track_to_artist_genreDF.join(tracks_audio_featuresDF, track_to_artist_genreDF.col("track_id") ===
      tracks_audio_featuresDF.col("track_id")).select(
      $"artist_id", tracks_audio_featuresDF.col("track_id"), $"genre",
      $"popularity",
      $"acousticness", $"danceability", $"duration", $"energy", $"instrumentalness", $"key", $"liveness", $"loudness", $"mode", $"speechiness", $"tempo", $"time_signature", $"valence",
    )

    //    21677838
    println(track_artist_genreDF.count())
    track_artist_genreDF.show()

    track_artist_genreDF
  }

  def storeDFInMongoDB(df: DataFrame, name: String, indexes: Array[(String, Int)])(implicit mongoConfig: MongoConfig, mongoClient: MongoClient): Unit = {
    logger.info(s"=====================${name}=====================")
    val num = df.count()
    logger.info(num)
    println(num)
    df.show(5)

    mongoClient(mongoConfig.db)(name).dropCollection()
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    if (indexes != null) {
      for (i <- indexes) {
        mongoClient(mongoConfig.db)(name).createIndex(MongoDBObject(i))
      }
    }
  }

}
