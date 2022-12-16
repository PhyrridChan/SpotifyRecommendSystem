package ind.phyrrid.offline

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.reflect.ClassTag

object Utils {
  var logger: Logger = Logger.getLogger("Utils")
  var basePath = "/tmp/checkpoint/"

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

  def storeDFInLocal(df: DataFrame, name: String, path: String = basePath): Unit = {
    val filePath = s"$path$name.csv"
    println(s"==========================save to local $filePath===========================================")
    logger.info(s"save to $filePath")
    df.write.option("header", "true").mode(SaveMode.Overwrite).csv(filePath)
  }

  def checkDFInLocal(name: String, spark: SparkSession, path: String = basePath): DataFrame = {
    val filePath = s"$path$name.csv"
    val file = new File(filePath)
    if (file.exists) {
      println(s"==========================read form local $filePath===========================================")
      logger.info(s"read form $filePath")
      spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
    }
    else {
      logger.warn(s"file $filePath not exists")
      null
    }
  }

  def storeRDDInLocal[T](df: RDD[T], name: String, path: String = basePath): Unit = {
    val filePath = s"$path$name"
    val file = new File(filePath)
    FileUtils.deleteDirectory(file)
    println(s"==========================save to local $filePath===========================================")
    logger.info(s"save to $filePath")
    df.saveAsObjectFile(filePath)
  }

  def checkRDDInLocal[T: ClassTag](name: String, sc: SparkContext, path: String = basePath): RDD[T] = {
    val filePath = s"$path$name"
    val file = new File(filePath)
    if (file.exists) {
      println(s"==========================read form local $filePath===========================================")
      logger.info(s"read form $filePath")
      sc.objectFile[T](filePath)
    }
    else {
      logger.warn(s"file $filePath not exists")
      null
    }
  }

  def storeObjInLocal[T](obj: T, name: String, path: String = basePath): Unit = {
    val filePath = s"$path$name.ser"
    val file = new File(filePath)
    FileUtils.deleteDirectory(file)
    val fos = new FileOutputStream(filePath)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(obj)
    println(s"==========================save to local $filePath===========================================")
    logger.info(s"save to $filePath")
    oos.close()
    fos.close()
  }

  def checkObjInLocal[T](name: String, path: String = basePath): T = {
    val filePath = s"$path$name.ser"
    println(s"==========================read form local $filePath===========================================")
    logger.info(s"read form $filePath")
    val fis = new FileInputStream(filePath)
    val ois = new ObjectInputStream(fis)
    ois.readObject().asInstanceOf[T]
  }

  def bark_notify(title: String, msg: String, retry: Int = 1): Unit = {
    for (i <- 0 until retry) {
      val result_source = scala.io.Source.fromURL(s"https://api.day.app/ZCs6nE83mAH4yLjn7YriUL/$title/$msg$i")
      if (result_source.mkString.contains("\"code\":200")) {
        result_source.close()
        return
      }
      result_source.close()
    }
  }
}
