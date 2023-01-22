import org.apache.spark.sql.DataFrame

class ShowWriter extends Writer {
  override def write(df: DataFrame): Unit = {
    df.show()
    println(df.count())
    println(df.select("uid").distinct().count())
    df.groupBy("gender", "age_cat")
      .count()
      .orderBy("gender", "age_cat")
      .show(false)
  }
}
