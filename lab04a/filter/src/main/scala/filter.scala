import org.apache.spark.sql.SparkSession

object filter extends App {
  val session = SparkSession.builder()
                                 .appName("sergey_maslakov_lab04a")
                                 .getOrCreate()
  session.conf.set("spark.sql.session.timeZone", "UTC")
  private val outputDir = session.sparkContext.getConf.get("spark.filter.output_dir_prefix")
  val kafkaTopic = session.sparkContext.getConf.get("spark.filter.topic_name")
  val kafkaOffset = session.sparkContext.getConf.get("spark.filter.offset")
  new Processor(session, new KafkaReader(session, kafkaTopic, kafkaOffset), outputDir).run
}
