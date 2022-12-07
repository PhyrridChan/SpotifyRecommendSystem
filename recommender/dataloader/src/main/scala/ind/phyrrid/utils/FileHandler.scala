package ind.phyrrid.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.mapred.InvalidFileTypeException

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable
import scala.io.Source

object FileHandler {
  def readFile(path: String): String = {
    val src = Source.fromFile(path)
    val res = src.mkString
    src.close()
    res
  }

  @throws(classOf[InvalidFileTypeException])
  def checkFileType(path: String, ft: String): Boolean = {
    val suffix = path.substring(path.lastIndexOf(".")+1, path.length())
    val check = suffix.toLowerCase().equals(ft)
    if (!check) {
      throw new InvalidFileTypeException(s"文件类型不为$ft")
    }
    check
  }

  object JsonFileHandler {
    def Map2Json(map: Map[String, _]): String = {
      val json = new JSONObject()
      map.foreach(x => {
        json.put(x._1, x._2)
      })
      json.toJSONString
    }

    def Json2Map(json: String): mutable.Map[String, String] = {
      val resMap = scala.collection.mutable.HashMap[String, String]()
      val jObject = JSON.parseObject(json)
      jObject.keySet().asScala.foreach(key => {
        resMap.put(key, jObject.getString(key))
      })
      resMap
    }

    @throws(classOf[InvalidFileTypeException])
    def readJsonFile(path: String): String = {
      checkFileType(path, "json")
      readFile(path)
    }

    def JsonFile2Map(path: String): mutable.Map[String, String] = Json2Map(readJsonFile(path))
  }

  object CSVFileHandler{

  }
}
