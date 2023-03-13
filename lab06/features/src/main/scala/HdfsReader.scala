import org.apache.spark.sql.{DataFrame, SparkSession}
class HdfsReader(spark: SparkSession) extends Reader {
  override def readWebLog(): DataFrame = {
    spark.read.json("/labs/laba03/weblogs.json")
  }

  override def readUserItems(): DataFrame = {
    spark.read.parquet("/user/sergey.maslakov/users-items/20200429")
  }
}
