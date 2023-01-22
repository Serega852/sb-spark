import org.apache.spark.sql.DataFrame

class PostgresWriter extends Writer {

  @Override
  def write(df: DataFrame) = {
    df.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/sergey_maslakov")
      .option("dbtable", "clients")
      .option("user", "sergey_maslakov")
      .option("password", "HP1LNBFQ")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()
  }
}
