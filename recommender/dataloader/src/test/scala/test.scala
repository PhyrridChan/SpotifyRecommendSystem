import ind.phyrrid.DAO.OriginalDataMapper.dataFileMap
import org.apache.log4j.Logger

object test {
  private val logger = Logger.getLogger("test")
  def main(args: Array[String]): Unit = {
    for( i <- (0 to 10)){
      logger.info(s"infoTest$i")
      logger.error(s"errTest$i")
    }
  }
}
