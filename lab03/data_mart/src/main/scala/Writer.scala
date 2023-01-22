import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame): Unit
}
