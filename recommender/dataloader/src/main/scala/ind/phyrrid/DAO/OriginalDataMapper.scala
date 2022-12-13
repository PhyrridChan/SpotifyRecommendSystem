package ind.phyrrid.DAO

import org.apache.spark.sql.{DataFrame, SparkSession}
import ind.phyrrid.utils.FileHandler.JsonFileHandler.{JsonFile2Map, JsonFile2MapWithIndex}

import scala.collection.mutable

object OriginalDataMapper {
  def dataFileMap(dataFile: String = "DataFileMapper.json"): mutable.Map[String, String] = {
    val path = this.getClass.getClassLoader.getResource(dataFile).getPath
    JsonFile2Map(path)
  }

  def dataFileMapWithIndex(dataFile: String = "DataFileMapper.json"): (mutable.HashMap[String, String], mutable.Map[String, Array[(String, Int)]]) = {
    val path = this.getClass.getClassLoader.getResource(dataFile).getPath
    JsonFile2MapWithIndex(path)
  }

  def csv2DataFrame(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").option("escape", "\"").csv(path)

  def loadAllData(spark: SparkSession): mutable.Map[String, DataFrame] = {
    val path_map = dataFileMap()

    val dataframe_map: mutable.Map[String, DataFrame] = mutable.Map()
    path_map.foreach(k => dataframe_map += (k._1 -> csv2DataFrame(spark, k._2)))

    dataframe_map
  }

  def loadAllDataWithIndex(spark: SparkSession): (mutable.Map[String, DataFrame], mutable.Map[String, Array[(String, Int)]]) = {
    val (path_map, index_map) = dataFileMapWithIndex()

    val dataframe_map: mutable.Map[String, DataFrame] = mutable.Map()
    path_map.foreach(k => dataframe_map += (k._1 -> csv2DataFrame(spark, k._2)))

    (dataframe_map, index_map)
  }
}
