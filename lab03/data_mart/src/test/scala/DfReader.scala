import org.apache.spark.sql.{DataFrame, SparkSession}

class DfReader(session: SparkSession) extends Reader {
  override def cassandra: DataFrame = {
    parquet("cassandra")
  }
  override def elasticsearch: DataFrame = {
    parquet("elastic")
  }

  override def json: DataFrame = {
    session.read.json("../data/laba03/hdfs")
  }

  override def postgres: DataFrame = {
    parquet("postgres")
  }

  private def parquet(dfName: String) = {
    session.read.parquet(s"../data/laba03/${dfName}")
  }

}
