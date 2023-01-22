import org.apache.spark.sql.DataFrame

trait Reader {
  def cassandra: DataFrame
  def elasticsearch: DataFrame
  def json: DataFrame
  def postgres: DataFrame
}
