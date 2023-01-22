import org.apache.spark.sql.DataFrame

class ShowWriter extends Writer {
  override def write(df: DataFrame): Unit = df.show(false)
}
