import org.apache.spark.sql.{DataFrame, SparkSession}
class ReaderImpl(spark: SparkSession, jsonPath: String, inputTopic: String) extends Reader {
  override def readJson(): DataFrame = {
    spark.read.json(jsonPath)
  }

  override def readKafka(): DataFrame = {
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "spark-master-1.newprolab.com:6667")
         .option("subscribe", inputTopic)
         .load
  }
}
