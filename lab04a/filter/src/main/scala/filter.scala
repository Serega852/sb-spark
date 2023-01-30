import org.apache.spark.sql.SparkSession

object filter extends App {
  val session = SparkSession.builder()
                                 .appName("sergey_maslakov_lab04a")
                                 .getOrCreate()
  private val outputDir = session.sparkContext.getConf.get("spark.filter.output_dir_prefix")
  new Processor(session, new KafkaReader(session), outputDir).run
}
