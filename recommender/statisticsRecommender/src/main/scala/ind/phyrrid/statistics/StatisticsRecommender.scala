package ind.phyrrid.statistics

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import scala.language.postfixOps

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {
  object Tasks {
    //    结果表
    val RES_ANNUAL_TOP_ALBUMS = "AnnualTopAlbums" // 年度最佳人气专辑
    val RES_TOP_ALBUMS = "TopAlbums" //最佳人气专辑
    val RES_ANNUAL_TOP_TRACKS = "AnnualTopTracks" // 年度最佳人气单曲
    val RES_TOP_TRACKS = "TopTracks" //最佳人气单曲
    val RES_FAVE_SINGERS = "FaveSingers" //最受听众喜爱歌手
    val RES_TOP_SINGERS = "TopSingers" //热门歌手
    val RES_ANNUAL_FAVE_GENRES = "AnnualFaveGenres" // 年度最受听众喜爱音乐类型
    val RES_FAVE_GENRES = "FaveGenres" //最受听众喜爱音乐类型

    //    配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
  }

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster(Tasks.config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig: MongoConfig = MongoConfig(Tasks.config("mongo.uri"), Tasks.config("mongo.db"))

    implicit val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //    tracks
    val tracksDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "tracks")
      .format("com.mongodb.spark.sql")
      .load()
    tracksDF.createTempView("tracks")

    val sortedTracks = spark.sql(
      """
        |select id, name, audio_feature_id, popularity, preview_url
        |from tracks where popularity > 10
        |order by popularity desc
        |""".stripMargin)
    storeDFInMongoDB(sortedTracks, Tasks.RES_TOP_TRACKS)

    //    albums
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x: Long) => simpleDateFormat.format(new Date(x)).toInt)

    val albumsDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "albums")
      .format("com.mongodb.spark.sql")
      .load()
    albumsDF.createTempView("albums")
    val sortedAlbums = spark.sql(
      """
        |select id, name, changeDate(release_date) as releaseDate, popularity
        |from albums where popularity > 10
        |order by popularity desc
        |""".stripMargin)
    sortedAlbums.createTempView("sortedAlbums")

    val annualSortedAlbums = spark.sql(
      """
        |select id, name, releaseDate, popularity
        |from sortedAlbums where releaseDate > 202000
        |order by releaseDate desc, popularity desc
        |""".stripMargin)

    val releaseDateIndex: Array[(String, Int)] = Array("releaseDate" -> -1, "popularity" -> -1)
    storeDFInMongoDB(sortedAlbums, Tasks.RES_TOP_ALBUMS, releaseDateIndex)
    storeDFInMongoDB(annualSortedAlbums, Tasks.RES_ANNUAL_TOP_ALBUMS, releaseDateIndex)

    //    r_albums_tracks
    import spark.implicits._
    val r_albums_tracksDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "r_albums_tracks")
      .format("com.mongodb.spark.sql")
      .load()
    val tracks_to_albums = r_albums_tracksDF.join(sortedTracks, sortedTracks.col("id") ===
      r_albums_tracksDF.col("track_id")).select(
      $"album_id", $"track_id", $"name".as("track_name"),
      $"audio_feature_id", $"popularity".as("track_popularity")
    )
    val tracks_albums = tracks_to_albums.join(sortedAlbums, tracks_to_albums.col("album_id") ===
      sortedAlbums.col("id")).select(
      $"album_id", $"track_id",
      $"name".as("album_name"), $"track_name",
      $"audio_feature_id", $"track_popularity", $"releaseDate"
    ).orderBy($"track_popularity" desc)
    tracks_albums.createTempView("tracks_albums")

    val annualSortedTracks = spark.sql(
      """
        |select album_id, track_id, album_name, track_name, audio_feature_id, track_popularity, releaseDate
        |from tracks_albums where releaseDate > 202000
        |order by releaseDate desc
        |""".stripMargin)
    storeDFInMongoDB(annualSortedTracks, Tasks.RES_ANNUAL_TOP_TRACKS, releaseDateIndex)

    //    artists
    val artistsDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "artists")
      .format("com.mongodb.spark.sql")
      .load()
    artistsDF.createTempView("artists")
    val sortedArtists_popularity = spark.sql(
      """
        |select name, id, popularity, followers
        |from artists order by popularity desc limit 200
        |""".stripMargin)
    val sortedArtists_followers = spark.sql(
      """
        |select name, id, popularity, followers
        |from artists order by followers desc limit 200
        |""".stripMargin)

    storeDFInMongoDB(sortedArtists_popularity, Tasks.RES_FAVE_SINGERS)
    storeDFInMongoDB(sortedArtists_followers, Tasks.RES_TOP_SINGERS)

    //    r_artist_genre
    val r_artist_genreDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "r_artist_genre")
      .format("com.mongodb.spark.sql")
      .load()
    val artist_genreDF = r_artist_genreDF.join(artistsDF, artistsDF.col("id") ===
      r_artist_genreDF.col("artist_id")).select(
      $"artist_id", $"name", $"popularity", $"followers", $"genre_id".as("genre")
    )
    artist_genreDF.createTempView("artist_genre")

    val sortedGenre = spark.sql(
      """
        |select sum(popularity) as sum_popularity, sum(followers) as sum_followers,
        |count(*) as count, genre
        |from artist_genre where popularity > 10 and followers > 1000 group by genre
        |order by count desc, sum_popularity/count desc , sum_followers/count desc
        |""".stripMargin)
    storeDFInMongoDB(sortedGenre, Tasks.RES_FAVE_GENRES)

    //    r_albums_artists
    val r_albums_artistsDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "r_albums_artists")
      .format("com.mongodb.spark.sql")
      .load()
    val albums_to_artists = r_albums_artistsDF.join(artist_genreDF, artist_genreDF.col("artist_id") ===
      r_albums_artistsDF.col("artist_id")
    ).select(
      artist_genreDF.col("artist_id"), $"album_id", $"genre"
    )
    val albums_artists = albums_to_artists.join(sortedAlbums, albums_to_artists.col("album_id") ===
      sortedAlbums.col("id")).select(
      $"artist_id", $"album_id", $"genre",
      $"popularity", $"releaseDate"
    )
    albums_artists.createTempView("albums_artists_genre")
    val annualSortedGenre = spark.sql(
      """
        |select  sum(popularity) as sum_popularity,count(*) as count, genre, releaseDate
        |from albums_artists_genre where popularity > 10 and releaseDate > 202000 group by genre, releaseDate
        |order by releaseDate desc, count desc, sum_popularity/count desc
        |""".stripMargin)
    storeDFInMongoDB(annualSortedGenre, Tasks.RES_ANNUAL_FAVE_GENRES, releaseDateIndex)

    bark_notify("StatisticsRecommender", "Done", 2)
    mongoClient.close()
    spark.stop()
  }

  def bark_notify(title: String, msg: String, retry: Int): Unit = {
    for (i <- 0 until retry) {
      val result_source = scala.io.Source.fromURL(s"https://api.day.app/ZCs6nE83mAH4yLjn7YriUL/$title/$msg$i")
      if (result_source.mkString.contains("\"code\":200")) {
        result_source.close()
        return
      }
      result_source.close()
    }
  }

  def storeDFInMongoDB(df: DataFrame, name: String, indexes: Array[(String, Int)] = null)(implicit mongoConfig: MongoConfig, mongoClient: MongoClient): Unit = {
    println(s"=====================${name}=====================")
    println(df.count())
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
