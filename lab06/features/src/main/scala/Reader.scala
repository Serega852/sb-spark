import org.apache.spark.sql.DataFrame

trait Reader {
  def readWebLog(): DataFrame
  def readUserItems(): DataFrame
}
