import org.apache.spark.sql.{DataFrame, SparkSession}
class FileReader(spark: SparkSession) extends Reader {
  override def readWebLog(): DataFrame = {
    spark.read.json("../../../data/laba03/hdfs/weblogs.json")
  }

  override def readUserItems(): DataFrame = {
    spark.read.parquet("../../../data/laba05/40200429")
  }
}
