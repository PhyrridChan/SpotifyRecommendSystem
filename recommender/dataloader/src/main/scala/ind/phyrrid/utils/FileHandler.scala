package ind.phyrrid.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
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
    val suffix = path.substring(path.lastIndexOf(".") + 1, path.length())
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
      val jObject = JSON.parseObject(json)
      JSONObject2Map(jObject)
    }

    def Json2MapWithIndex(json: String): (mutable.HashMap[String, String], mutable.Map[String, Array[(String, Int)]]) = {
      val jObject = JSON.parseObject(json)
      JSONObject2MapWithIndex(jObject)
    }

    def JSONObject2Map(jObject: JSONObject): mutable.Map[String, String] = {
      val resMap = scala.collection.mutable.HashMap[String, String]()
      jObject.keySet().asScala.foreach(key => {
        resMap.put(key, jObject.getString(key))
      })
      resMap
    }

    def JSONObject2MapWithIndex(nObject: JSONObject): (mutable.HashMap[String, String], mutable.Map[String, Array[(String, Int)]]) = {
      val pathMap = scala.collection.mutable.HashMap[String, String]()
      val indexMap = scala.collection.mutable.Map[String, Array[(String, Int)]]()
      nObject.keySet().asScala.foreach(
        key => {
          pathMap.put(key, nObject.getJSONObject(key).getString("path"))
          val moduleArray = index_handler(nObject, key)
          if (moduleArray != null & moduleArray.length > 0) indexMap.put(key, moduleArray)
        }
      )
      (pathMap, indexMap)
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

    @throws(classOf[InvalidFileTypeException])
    def readJsonFile(path: String): String = {
      checkFileType(path, "json")
      readFile(path)
    }

    def JsonFile2Map(path: String): mutable.Map[String, String] = Json2Map(readJsonFile(path))

    def JsonFile2MapWithIndex(path: String): (mutable.HashMap[String, String], mutable.Map[String, Array[(String, Int)]]) = Json2MapWithIndex(readJsonFile(path))
  }

  object CSVFileHandler {

  }
}
