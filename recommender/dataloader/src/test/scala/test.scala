import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import ind.phyrrid.DAO.MongoConfig
import ind.phyrrid.DAO.OriginalDataMapper.{csv2DataFrame, dataFileMap, loadAllDataWithIndex}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.sql.toMongoDataFrameWriterFunctions
import ind.phyrrid.DAO.DataLoader.MongoDataHandler

import scala.collection.mutable

object test {
  case class sex_test(name: String, age: Int, length: Int, time: String)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val (allData, indexInfo) = loadAllDataWithIndex(spark)

    import spark.implicits._
    allData("tracks").where($"id" === "5edi1NCQfGQqjbRFdpgIM0").show()
  }
}
