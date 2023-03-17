import org.apache.spark.sql.DataFrame

trait Reader {
  def readJson(): DataFrame
  def readKafka(): DataFrame
}
