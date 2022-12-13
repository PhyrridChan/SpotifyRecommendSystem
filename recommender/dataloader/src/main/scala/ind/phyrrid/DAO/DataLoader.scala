package ind.phyrrid.DAO

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.spark.sql.toMongoDataFrameWriterFunctions
import ind.phyrrid.DAO.OriginalDataMapper.loadAllDataWithIndex
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable


case class MongoConfig(uri: String, db: String)

case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String, docName: String)

case class albums_record(id: String, name: String, album_group: String, album_type: String, release_date: String, popularity: String)

case class artists_record(name: String, id: String, popularity: String, followers: String)

case class audio_features_record(id: String, acousticness: Double, analysis_url: String, danceability: Double, duration: Int, energy: Double, instrumentalness: Double, key: Int, liveness: Double, loudness: Double, mode: Int, speechiness: Double, tempo: Double, time_signature: Int, valence: Double)

case class genres_record(id: String)

case class r_albums_artists_record(album_id: String, artist_id: String)

case class r_albums_tracks_record(album_id: String, track_id: String)

case class r_artist_genre_record(genre_id: String, artist_id: String)

case class r_track_artist_record(track_id: String, artist_id: String)

case class tracks_record(id: String, disc_number: Int, duration: Int, explicit: Int, audio_feature_id: String, name: String, preview_url: String, track_number: String, popularity: String, is_playable: String)

object DataLoader {
  private val logger = Logger.getLogger("DataLoader")

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch_phyrridchan",
      "es.doc.name" -> "Spotify"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val (allData, indexInfo) = loadAllDataWithIndex(spark)
    val allDataRefined = MongoDataHandler(spark, allData)
    logger.info(allDataRefined)
    storeDataInMongoDB(allDataRefined, indexInfo)

    implicit val eSConfig: ESConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"), config("es.doc.name"))
    storeDataInES(ESDataHandler(spark, allDataRefined))

    spark.stop()
  }

  def MongoDataHandler(spark: SparkSession, allData: mutable.Map[String, DataFrame]): mutable.Map[String, DataFrame] = {
    import spark.implicits._

    for ((k, v) <- allData) {
      k match {
        case "tracks" =>
          allData.put(k, v.as[tracks_record].filter(record =>
            record.popularity != null && record.popularity.matches("^\\d+"))
            .withColumn("popularity", $"popularity".cast(IntegerType))
            .toDF())
        case "albums" =>
          allData.put(k, v.as[albums_record].filter(record =>
            (record.popularity != null && record.popularity.matches("^\\d+")) &&
              (record.release_date != null && record.release_date.matches("^-?\\d+")))
            .withColumn("popularity", $"popularity".cast(IntegerType))
            .withColumn("release_date", $"release_date".cast(LongType))
            .toDF())
        case "artists" =>
          allData.put(k, v.as[artists_record].filter(record =>
            (record.popularity != null && record.popularity.matches("^\\d+")) &&
              (record.followers != null && record.followers.matches("^\\d+")))
            .withColumn("popularity", $"popularity".cast(IntegerType))
            .withColumn("followers", $"followers".cast(LongType))
            .toDF())
        case _ =>
      }

      logger.info(k)
      logger.info(allData(k).printSchema())
      allData(k).persist()
    }

    allData
  }

  def storeDataInMongoDB(allData: mutable.Map[String, DataFrame], indexInfo: mutable.Map[String, Array[(String, Int)]])(implicit mongoConfig: MongoConfig): Unit = {
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    for ((k, v) <- allData) {
      mongoClient(mongoConfig.db)(k).dropCollection()
      v.write
        .option("uri", mongoConfig.uri)
        .option("collection", k)
        .mode(SaveMode.Overwrite)
        .mongo()

      val indexes: Array[(String, Int)] = indexInfo.getOrElse(k, Array[(String, Int)]())
      for (i <- indexes) {
        mongoClient(mongoConfig.db)(k).createIndex(MongoDBObject(i))
      }
    }

    mongoClient.close()
  }


  def ESDataHandler(spark: SparkSession, allData: mutable.Map[String, DataFrame]): DataFrame = {
    import spark.implicits._
    val tracks = allData("tracks").select($"id" as "track_id", $"name" as "track_name")
    val albums = allData("albums").select($"id" as "album_id", $"name" as "album_name")
    val artists = allData("artists").select($"id" as "artist_id", $"name" as "artist_name")
    val r_albums_artists = allData("r_albums_artists")
    val r_artist_genre = allData("r_artist_genre")
    val r_albums_tracks = allData("r_albums_tracks")

    var joinExpression = artists.col("artist_id") ===
      r_albums_artists.col("artist_id")
    val artists_to_albums = artists.join(r_albums_artists, joinExpression).
      select(artists.col("artist_id"), $"artist_name", $"album_id")

    joinExpression = artists_to_albums.col("album_id") ===
      albums.col("album_id")
    val artists__albums = artists_to_albums.join(albums, joinExpression).
      select(artists_to_albums.
        col("album_id"), $"album_name", $"artist_id", $"artist_name")

    joinExpression = artists__albums.col("album_id") ===
      r_albums_tracks.col("album_id")
    val artists__albums_to_tracks = artists__albums.join(r_albums_tracks, joinExpression).
      select(artists__albums.
        col("album_id"), $"album_name", $"artist_id", $"artist_name", $"track_id")

    joinExpression = artists__albums_to_tracks.col("track_id") ===
      tracks.col("track_id")
    val artists__albums__tracks = artists__albums_to_tracks.join(tracks, joinExpression).
      select(artists__albums_to_tracks.col("track_id"),
        $"track_name", $"album_id", $"album_name", $"artist_id", $"artist_name"
      )

    val artist_genres = r_artist_genre.groupBy($"artist_id").agg(
      concat_ws("|", collect_set($"genre_id")).as("genres")
    ).select($"artist_id", $"genres")

    joinExpression = artists__albums__tracks.col("artist_id") ===
      artist_genres.col("artist_id")
    val artists__albums__tracks__genre = artists__albums__tracks.join(artist_genres, joinExpression).
      select(
        $"track_id", $"track_name", $"album_id", $"album_name",
        artists__albums__tracks.col("artist_id"), $"artist_name",
        $"genres"
      ).distinct()

    artists__albums__tracks__genre
  }

  def storeDataInES(dataFrame: DataFrame)(implicit eSConfig: ESConfig): Unit = {
    val option_map: mutable.Map[String, String] = mutable.Map()
    option_map.put("es.index.auto.create", "true")
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.httpHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) =>
        option_map.put("es.nodes", host)
        option_map.put("es.port", port);
    }

    dataFrame.write.format("org.elasticsearch.spark.sql")
      .options(option_map)
      .mode(SaveMode.Overwrite)
      .save(eSConfig.index + "/" + eSConfig.docName)
  }
}
