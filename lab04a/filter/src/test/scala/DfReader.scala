import org.apache.spark.sql.{DataFrame, SparkSession}
class DfReader(session: SparkSession) extends Reader {
  override def read: DataFrame = {
    session.read.parquet("../../../data/laba04/lab04_kafka")
  }
}
