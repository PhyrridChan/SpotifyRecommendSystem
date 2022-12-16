package ind.phyrrid.offline

import breeze.linalg.{DenseVector, norm}
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import ind.phyrrid.offline.Utils._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimTrack {
  def main(args: Array[String]): Unit = {
    Utils.logger = Logger.getLogger("SimTrack")
    Utils.basePath = "/tmp/checkpoint/SimTrack/"

    val start_time = System.currentTimeMillis()
    var end_time: Long = 0
    var dur: Double = 0
    val sparkConf: SparkConf = new SparkConf().setMaster(Tasks.config("spark.cores")).setAppName("SimTrack")
    implicit val mongoConfig: MongoConfig = MongoConfig(Tasks.config("mongo.uri"), Tasks.config("mongo.db"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    var track_df = checkRDDInLocal[((String, DenseVector[Double]), (String, DenseVector[Double]))]("track_df", sc)
    var track_df_o = checkRDDInLocal[(String, DenseVector[Double])]("track_df_o", sc)
    import spark.implicits._
    if (track_df == null) {
      if (track_df_o == null) {
        var track_artistIndex_genreDF: DataFrame = null
        val track_artistIndex_genreDF_o = checkDFInLocal("track_artistIndex_genreDF", spark)
        if (track_artistIndex_genreDF_o == null) {
          val (track_artist_genreDF, artist_genreDF) = loadRequiredData(spark)

          // track_artistIndex_genreDF
          val artist_indexer_model = artistStringIndexerModel(artist_genreDF)

          track_artistIndex_genreDF = artist_indexer_model.transform(track_artist_genreDF)
          storeDFInLocal(track_artistIndex_genreDF, "track_artistIndex_genreDF")
        } else {
          track_artistIndex_genreDF = track_artistIndex_genreDF_o
        }

        // track_artistIndex_genreVecDF
        val track_artistIndex_genreArrDF = track_artistIndex_genreDF.as[track_artistIndex_genre].rdd.map(
          v => track_artistIndex_genreArr(v.artist_id, v.track_id, v.artist_idIndex, v.genre, v.genre.split("[ |]"), v.popularity,
            v.acousticness, v.danceability, v.duration, v.energy, v.instrumentalness, v.key, v.liveness, v.loudness, v.mode, v.speechiness, v.tempo, v.time_signature, v.valence)
        ).toDF()
        val genre_vec_model = genreVecModel(track_artistIndex_genreArrDF)
        val track_artistIndex_genreVecDF = genre_vec_model.transform(track_artistIndex_genreArrDF).where(
          $"popularity" > 60
        )

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
        val scalar = new StandardScaler()
          .setInputCol("pcafeatures")
          .setOutputCol("scaledPCAFeatures")
          .setWithStd(true)
          .setWithMean(false)
          .fit(pca_TrackFeaturesDF)
        val scaled_pca_TrackFeaturesDF = scalar.transform(pca_TrackFeaturesDF)

        track_df_o = scaled_pca_TrackFeaturesDF.select($"track_id", $"scaledPCAFeatures")
          .as[track_recod]
          .rdd
          .map(v => (v.track_id, DenseVector(v.scaledPCAFeatures.toArray)))
        storeRDDInLocal[(String, DenseVector[Double])](track_df_o, "track_df_o")
        end_time = System.currentTimeMillis()
        dur = (end_time - start_time).toDouble / 60000
        println(s"track_df_o saved===${track_df_o.count()}================================${dur}===================================")
      }
      // track_df(cross joined)
      track_df =
        (track_df_o cartesian track_df_o).filter(
          v => v._1._1 != v._2._1
        )
      storeRDDInLocal(track_df, "track_df")
      end_time = System.currentTimeMillis()
      dur = (end_time - start_time).toDouble / 60000
      println(s"track_df saved===================================${dur}===================================")
    }

    // res_df
    val res_df = track_df.map(
      { v => (v._1._1, v._2._1, norm(v._1._2 -:- v._2._2)) }
    ).toDF("ori_track_id", "sea_track_id", "norm")
    res_df.show(false)

    implicit val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    storeDFInMongoDB(res_df, "SimTrack", Array("norm" -> 1, "ori_track_id" -> 1))
    end_time = System.currentTimeMillis()
    dur = (end_time - start_time).toDouble / 60000
    println(s"res_df saved===================================${dur}===================================")

    end_time = System.currentTimeMillis()
    dur = (end_time - start_time).toDouble / 60000
    bark_notify("SimTrack", s"Done, it takes ${dur} mins")
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
        .load().where($"followers" > 1000 and $"popularity" > 50).distinct()

      val r_artist_genreDF = spark.read
        .option("uri", "mongodb://localhost:27017/recommender")
        .option("collection", "r_artist_genre")
        .format("com.mongodb.spark.sql")
        .load().distinct()

      artist_genreDF = r_artist_genreDF.join(artistsDF, artistsDF.col("id") ===
        r_artist_genreDF.col("artist_id")).select(
        $"artist_id", $"genre_id".as("genre")).distinct().groupBy($"artist_id").agg(
        concat_ws("|", collect_set($"genre")).as("genre")).select($"artist_id", $"genre")

      storeDFInLocal(artist_genreDF, "artist_genreDF")
    } else {
      artist_genreDF = artist_genreDF_o
    }

    var track_artist_genreDF: DataFrame = null
    if (track_artist_genreDF_o == null) {
      val tracksDF = spark.read
        .option("uri", "mongodb://localhost:27017/recommender")
        .option("collection", "tracks")
        .format("com.mongodb.spark.sql")
        .load().distinct().where($"popularity" > 60)
      val audio_featuresDF = spark.read
        .option("uri", "mongodb://localhost:27017/recommender")
        .option("collection", "audio_features")
        .format("com.mongodb.spark.sql")
        .load().distinct()
      val tracks_audio_featuresDF = audio_featuresDF.join(tracksDF, audio_featuresDF.col("id") ===
        tracksDF.col("audio_feature_id")).select(
        tracksDF.col("id") as "track_id", $"audio_feature_id", $"popularity",
        $"acousticness", $"danceability", audio_featuresDF.col("duration"), $"energy", $"instrumentalness", $"key", $"liveness", $"loudness", $"mode", $"speechiness", $"tempo", $"time_signature", $"valence",
      )

      val r_track_artistDF = spark.read
        .option("uri", "mongodb://localhost:27017/recommender")
        .option("collection", "r_track_artist")
        .format("com.mongodb.spark.sql")
        .load().distinct()

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
}
