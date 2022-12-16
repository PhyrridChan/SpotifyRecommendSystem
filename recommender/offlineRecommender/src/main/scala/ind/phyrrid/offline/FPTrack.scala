package ind.phyrrid.offline

import ind.phyrrid.offline.Utils.{MongoConfig, Tasks, bark_notify, checkDFInLocal, checkRDDInLocal, storeDFInLocal, storeRDDInLocal}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FPTrack {
  def main(args: Array[String]): Unit = {
    Utils.logger = Logger.getLogger("FPTrack")
    Utils.basePath = "/tmp/checkpoint/FPTrack/"

    val start_time = System.currentTimeMillis()
    var end_time: Long = 0
    var dur: Double = 0
    val sparkConf: SparkConf = new SparkConf().setMaster(Tasks.config("spark.cores")).setAppName("FPTrack")
    implicit val mongoConfig: MongoConfig = MongoConfig(Tasks.config("mongo.uri"), Tasks.config("mongo.db"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    var FPSet: RDD[FPGrowth.FreqItemset[String]] = checkRDDInLocal[FPGrowth.FreqItemset[String]]("FP set", sc)
    var ARSet: RDD[AssociationRules.Rule[String]] = checkRDDInLocal[AssociationRules.Rule[String]]("AR set", sc)
    if (FPSet == null || ARSet == null) {
      val tracks_id_set = loadRequiredData(spark)

      end_time = System.currentTimeMillis()
      dur = (end_time - start_time).toDouble / 60000
      println(s"read tracks_id_set ===================================${dur}===================================")

      val tracks_id_arr: RDD[Array[String]] = tracks_id_set.rdd.map(v => v.getAs[String]("tracks_id_set").split('|'))
      tracks_id_arr.persist()

      val res_tuple: (RDD[FPGrowth.FreqItemset[String]], RDD[AssociationRules.Rule[String]]) = FPM__ARM(tracks_id_arr)
      FPSet = res_tuple._1
      ARSet = res_tuple._2
    }

    val FPArr = FPSet.take(20)
    val ARArr = ARSet.take(20)

    println("\n==================FPM Result==================")
    println(s"Number of frequent itemsets: ${FPArr.length}")
    FPArr.foreach(println)
    println("\n==================ARM Result==================")
    println(s"Number of Association Rules: ${ARArr.length}")
    ARArr.foreach(println)

    bark_notify("FPTrack", s"Done, it takes ${dur} mins")
    spark.stop()
    sc.stop()
  }

  def loadRequiredData(spark: SparkSession): DataFrame = {
    import spark.implicits._

    var tracks_id_set = checkDFInLocal("tracks_id_set", spark)
    if (tracks_id_set == null) {
      var track_artistDF = checkDFInLocal("track_artistDF", spark)
      if (track_artistDF == null) {
        val r_track_artistDF = spark.read
          .option("uri", "mongodb://localhost:27017/recommender")
          .option("collection", "r_track_artist")
          .format("com.mongodb.spark.sql")
          .load().distinct()
        val artistsDF = spark.read
          .option("uri", "mongodb://localhost:27017/recommender")
          .option("collection", "artists")
          .format("com.mongodb.spark.sql")
          .load().distinct().select($"name", $"id")
        val track_to_artistDF = r_track_artistDF.join(artistsDF, r_track_artistDF.col("artist_id") === artistsDF.col("id"))
          .select($"artist_id", $"name".as("artist_name"), $"track_id")

        val tracksDF = spark.read
          .option("uri", "mongodb://localhost:27017/recommender")
          .option("collection", "tracks")
          .format("com.mongodb.spark.sql")
          .load().distinct().select($"id", $"name")
        track_artistDF = tracksDF.join(track_to_artistDF, track_to_artistDF.col("track_id") === tracksDF.col("id"))
          .select($"artist_id", $"artist_name", $"track_id", $"name".as("track_name"))

        storeDFInLocal(track_artistDF, "track_artistDF")
      }
      val playlistDF = spark.read
        .option("uri", "mongodb://localhost:27017/recommender")
        .option("collection", "playlist")
        .format("com.mongodb.spark.sql")
        .load()

      tracks_id_set = playlistDF.join(track_artistDF, playlistDF.col("trackname") ===
        track_artistDF.col("track_name") and playlistDF.col("artistname") ===
        track_artistDF.col("artist_name")).select($"track_id", $"user_id", $"playlistname").distinct()
        .groupBy($"user_id", $"playlistname").agg(
        concat_ws("|", collect_set($"track_id")).as("tracks_id_set")
      ).select("tracks_id_set")
      storeDFInLocal(tracks_id_set, "tracks_id_set")
    }
    tracks_id_set
  }

  def FPM__ARM(tracks_id_arr: RDD[Array[String]],
               miniSupport: Double = 0.005,
               FPM: Boolean = true,
               ARM: Boolean = true,
               minConfidence: Double = 0.8
              ): (RDD[FPGrowth.FreqItemset[String]], RDD[AssociationRules.Rule[String]]) = {
    val model: FPGrowthModel[String] = new FPGrowth()
      .setMinSupport(miniSupport)
      .run(tracks_id_arr)

    var FPSet: RDD[FPGrowth.FreqItemset[String]] = null
    var ARSet: RDD[AssociationRules.Rule[String]] = null
    if (FPM) {
      FPSet = model.freqItemsets
      println(s"Number of frequent itemsets: ${FPSet.count()}")
      storeRDDInLocal[FPGrowth.FreqItemset[String]](FPSet, "FP set")
    }

    if (ARM) {
      ARSet = model.generateAssociationRules(minConfidence)
      println(s"Number of Association Rules: ${ARSet.count()}")
      storeRDDInLocal[AssociationRules.Rule[String]](ARSet, "AR set")
    }

    (FPSet, ARSet)
  }
}
