package ind.phyrrid.offline

import breeze.linalg.{DenseVector, norm}
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{PCA, StandardScaler, StringIndexer, StringIndexerModel, VectorAssembler, Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.awt.Toolkit
import java.io.File


object SimTrack {
  case class MongoConfig(uri: String, db: String)

  case class track_artist_genre(
                                 artist_id: String, track_id: String,
                                 genre: String, popularity: Int,
                                 acousticness: Double, danceability: Double, duration: Int, energy: Double,
                                 instrumentalness: Double, key: Int, liveness: Double, loudness: Double,
                                 mode: Int, speechiness: Double, tempo: Double, time_signature: Int, valence: Double
                               )

  case class genres_record(id: String)

  case class track_artistIndex_genre(
                                      artist_id: String, track_id: String,
                                      artist_idIndex: Double,
                                      genre: String, popularity: Int,
                                      acousticness: Double, danceability: Double, duration: Int, energy: Double,
                                      instrumentalness: Double, key: Int, liveness: Double, loudness: Double,
                                      mode: Int, speechiness: Double, tempo: Double, time_signature: Int, valence: Double
                                    )

  case class track_artistIndex_genreArr(
                                         artist_id: String, track_id: String,
                                         artist_idIndex: Double,
                                         genre: String, genreArr: Array[String],
                                         popularity: Int,
                                         acousticness: Double, danceability: Double, duration: Int, energy: Double,
                                         instrumentalness: Double, key: Int, liveness: Double, loudness: Double,
                                         mode: Int, speechiness: Double, tempo: Double, time_signature: Int, valence: Double
                                       )

  case class track_recod(
                          track_id: String,
                          scaledPCAFeatures: Vector
                        )


  object Tasks {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
  }

  private val logger = Logger.getLogger("SimTrack")

  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val sparkConf: SparkConf = new SparkConf().setMaster(Tasks.config("spark.cores")).setAppName("ALSTrainer")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig: MongoConfig = MongoConfig(Tasks.config("mongo.uri"), Tasks.config("mongo.db"))

    val (track_artist_genreDF, artist_genreDF) = loadRequiredData(spark)
    import spark.implicits._

    // track_artistIndex_genreDF
    val artist_indexer_model = artistStringIndexerModel(artist_genreDF)
    var track_artistIndex_genreDF: DataFrame = null
    val track_artistIndex_genreDF_o = checkDFInLocal("track_artistIndex_genreDF", spark)
    if (track_artistIndex_genreDF_o == null) {
      track_artistIndex_genreDF = artist_indexer_model.transform(track_artist_genreDF)
      storeDFInLocal(track_artistIndex_genreDF, "track_artistIndex_genreDF")
    } else {
      track_artistIndex_genreDF = track_artistIndex_genreDF_o
    }

    // track_artistIndex_genreVecDF
    val genresDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "genres")
      .format("com.mongodb.spark.sql")
      .load().as[genres_record].rdd.map(v => v.id.split(' ')).map(Tuple1.apply).toDF("genreArr")
    val genre_vec_model = genreVecModel(genresDF)
    val track_artistIndex_genreArrDF = track_artistIndex_genreDF.as[track_artistIndex_genre].rdd.map(
      v => track_artistIndex_genreArr(v.artist_id, v.track_id, v.artist_idIndex, v.genre, v.genre.split(' '), v.popularity,
        v.acousticness, v.danceability, v.duration, v.energy, v.instrumentalness, v.key, v.liveness, v.loudness, v.mode, v.speechiness, v.tempo, v.time_signature, v.valence)
    ).toDF()
    val track_artistIndex_genreVecDF = genre_vec_model.transform(track_artistIndex_genreArrDF)

    // scaled_pca_TrackFeaturesDF
    val assembler = new VectorAssembler().setInputCols(
      Array(
        "artist_idIndex",
        "genreVec",
        "popularity",
        "acousticness",
        "danceability",
        "duration",
        "energy",
        "instrumentalness",
        "key",
        "liveness",
        "loudness",
        "mode",
        "speechiness",
        "tempo",
        "time_signature",
        "valence"
      )
    ).setOutputCol("features")
    val TrackFeaturesDF = assembler.transform(track_artistIndex_genreVecDF)
    val pca_TrackFeaturesDF_model = new PCA()
      .setInputCol("features")
      .setOutputCol("pcafeatures")
      .setK(8)
      .fit(TrackFeaturesDF)
    val pca_TrackFeaturesDF = pca_TrackFeaturesDF_model.transform(TrackFeaturesDF)
    val scaler = new StandardScaler()
      .setInputCol("pcafeatures")
      .setOutputCol("scaledPCAFeatures")
      .setWithStd(true)
      .setWithMean(false)
      .fit(pca_TrackFeaturesDF)
    val scaled_pca_TrackFeaturesDF = scaler.transform(pca_TrackFeaturesDF)


    // track_df(cross joined)
    val track_df_o = scaled_pca_TrackFeaturesDF.select($"track_id", $"scaledPCAFeatures")
      .as[track_recod]
      .rdd
      .map(v => ("k", (v.track_id, DenseVector(v.scaledPCAFeatures.toArray))))
    val track_df:
      RDD[(String, ((String, DenseVector[Double]), (String, DenseVector[Double])))] =
      track_df_o.join(track_df_o).filter(
        v => v._2._1._1 != v._2._2._1
      )

    // res_df
    val res_df = track_df.map(
      { v => (v._2._1._1, v._2._2._1, norm(v._2._1._2 -:- v._2._2._2)) }
    ).toDF("ori_track_id", "sea_track_id", "norm")
    res_df.show(false)

    implicit val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    storeDFInMongoDB(res_df, "SimTrack", Array("norm" -> 1, "ori_track_id" -> 1))

    //    val source = scala.io.Source.fromURL(s"https://api.day.app/ZCs6nE83mAH4yLjn7YriUL/test/done")
    //    source.close()

    val toolkit = {
      Toolkit.getDefaultToolkit
    }
    toolkit.beep()
    val end_time = System.currentTimeMillis()
    println(s"===================================${end_time - start_time}===================================")
    spark.stop()
  }

  def loadRequiredData(spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._

    val track_artist_genreDF_o = checkDFInLocal("track_artist_genreDF", spark)
    val artist_genreDF_o = checkDFInLocal("artist_genreDF", spark)

    var artist_genreDF: DataFrame = null

    if (artist_genreDF_o == null) {
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

      artist_genreDF = r_artist_genreDF.join(artistsDF, artistsDF.col("id") ===
        r_artist_genreDF.col("artist_id")).select(
        $"artist_id", $"genre_id".as("genre")).distinct()

      storeDFInLocal(artist_genreDF, "artist_genreDF")
    } else {
      artist_genreDF = artist_genreDF_o
    }

    val tracksDF = spark.read
      .option("uri", "mongodb://localhost:27017/recommender")
      .option("collection", "tracks")
      .format("com.mongodb.spark.sql")
      .load().distinct().where($"popularity" > 10)
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

    var track_artist_genreDF: DataFrame = null
    if (track_artist_genreDF_o == null) {
      val r_track_artistDF = spark.read
        .option("uri", "mongodb://localhost:27017/recommender")
        .option("collection", "r_track_artist")
        .format("com.mongodb.spark.sql")
        .load()

      val track_to_artist_genreDF = r_track_artistDF.join(artist_genreDF, artist_genreDF.col("artist_id") ===
        r_track_artistDF.col("artist_id")).select(
        r_track_artistDF.col("artist_id"), $"track_id", $"genre")

      track_artist_genreDF = track_to_artist_genreDF.join(tracks_audio_featuresDF, track_to_artist_genreDF.col("track_id") ===
        tracks_audio_featuresDF.col("track_id")).select(
        $"artist_id", tracks_audio_featuresDF.col("track_id"), $"genre",
        $"popularity",
        $"acousticness", $"danceability", $"duration", $"energy", $"instrumentalness", $"key", $"liveness", $"loudness", $"mode", $"speechiness", $"tempo", $"time_signature", $"valence",
      ).distinct().as[track_artist_genre].toDF()

      storeDFInLocal(track_artist_genreDF, "track_artist_genreDF")
    } else {
      track_artist_genreDF = track_artist_genreDF_o
    }

    (track_artist_genreDF, artist_genreDF)
  }

  def genreVecModel(genresDF: DataFrame): Word2VecModel = {
    val word2vec = new Word2Vec()
      .setInputCol("genreArr")
      .setOutputCol("genreVec")
      .setVectorSize(1)
      .setMinCount(0)

    val model = word2vec.fit(genresDF)
    model
  }

  def artistStringIndexerModel(artistDF: DataFrame): StringIndexerModel = {
    val indexer = new StringIndexer()
      .setInputCol("artist_id")
      .setOutputCol("artist_idIndex")
      .fit(artistDF)
    indexer
  }

  def trackStringIndexerModel(trackDF: DataFrame): StringIndexerModel = {
    val indexer = new StringIndexer()
      .setInputCol("track_id")
      .setOutputCol("track_idIndex")
      .fit(trackDF)
    indexer
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

  def storeDFInLocal(df: DataFrame, name: String, path: String = "/tmp/checkpoint/ALSTrainer/"): Unit = {
    df.write.option("header", "true").mode(SaveMode.Overwrite).csv(s"$path$name.csv")
  }

  def checkDFInLocal(name: String, spark: SparkSession, path: String = "/tmp/checkpoint/ALSTrainer/"): DataFrame = {
    val filePath = s"$path$name.csv"
    val file = new File(filePath)
    if (file.exists) {
      println("==========================read form local===========================================")
      logger.info(s"read form $filePath")
      spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
    }
    else {
      logger.warn(s"file $filePath not exists")
      null
    }
  }
}
