import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaReader(session: SparkSession, kafkaTopicName: String, kafkaOffset: String) extends Reader {
  override def read: DataFrame = {
    session.read
           .format("kafka")
           .option("kafka.bootstrap.servers", "spark-master-1.newprolab.com:6667")
           .option("subscribe", kafkaTopicName)
           .option("startingOffsets",
                   if (kafkaOffset.contains("earliest")) {
                     kafkaOffset
                   } else {
                     s"""{"$kafkaTopicName":{"0":$kafkaOffset}}"""
                   })
           .load
  }
}
