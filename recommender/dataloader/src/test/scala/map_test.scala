import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.spark.sql.toMongoDataFrameWriterFunctions
import ind.phyrrid.DAO.MongoConfig
import ind.phyrrid.utils.FileHandler.JsonFileHandler.{Json2Map, readJsonFile}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import test.sex_test

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable


object map_test {
  def main(args: Array[String]): Unit = {
    val json = readJsonFile("/Users/phyrridchan/IdeaProjects/SpotifyRecommendSystem/recommender/dataloader/src/main/resources/DataFileMapper.json")
    val nObject = JSON.parseObject(json)
    val pathMap = scala.collection.mutable.HashMap[String, String]()
    val indexMap = scala.collection.mutable.Map[String, Array[(String, Int)]]()
    nObject.keySet().asScala.foreach(
      key => {
        pathMap.put(key, nObject.getJSONObject(key).getString("path"))
        val moduleArray = index_handler(nObject, key)
        if (moduleArray != null & moduleArray.length > 0) indexMap.put(key, moduleArray)
      }
    )


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val frame = spark.createDataFrame(
      Seq(("shit", 25, 18, "16m", "7373", 18),
        ("asda", 18, 17, "18m", "7272", 14))
    ).toDF("name", "age", "length", "time", "id", "popularity").as[sex_test].toDF()

    val allData = mutable.Map(
      "albums" -> frame
    )
    val indexInfo = indexMap

    val config: Map[String, String] = Map(
      "mongo.uri" -> "mongodb://localhost:27017/test_recommender",
      "mongo.db" -> "test_recommender",
    )

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    for ((k, v) <- allData) {
      mongoClient(mongoConfig.db)(k).dropCollection()
      v.write
        .option("uri", mongoConfig.uri)
        .option("collection", k)
        .mode("overwrite")
        .mongo()

      val indexes: Array[(String, Int)] = indexInfo.getOrElse(k, Array[(String, Int)]())
      for (i <- indexes) {
        mongoClient(mongoConfig.db)(k).createIndex(MongoDBObject(i))
      }
    }
    mongoClient.close()
  }

  def index_handler(nObject: JSONObject, key: String): Array[(String, Int)] = {
    val index_array: JSONArray = nObject.getJSONObject(key).getJSONArray("index")
    if (index_array == null) return Array()
    val index_len = index_array.size()
    val moduleArray: Array[(String, Int)] = new Array(index_len)
    for (i <- 0 until index_len) {
      val index_obj = index_array.getJSONObject(i)
      index_obj.keySet().asScala.foreach(
        key => {
          moduleArray(i) = (key -> index_obj.getIntValue(key))
        }
      )
    }
    moduleArray
  }
}


