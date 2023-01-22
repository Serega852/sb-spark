import org.apache.spark.sql.DataFrame

class ShowWriter extends Writer {
  override def write(df: DataFrame): Unit = df.groupBy("gender", "age_cat").count().show(false)
}
