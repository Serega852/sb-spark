import org.apache.spark.sql.SparkSession

object test extends App {
  val session = SparkSession.builder()
                            .appName("sergey_maslakov_lab07")
                            .getOrCreate()
  session.conf.set("spark.sql.session.timeZone", "UTC")

  val inputTopic = session.sparkContext.getConf.get("spark.test.input_topic", "sergey_maslakov")
  val outputTopic = session.sparkContext.getConf.get("spark.test.output_topic", "sergey_maslakov_lab07_out")
  val modelPath = session.sparkContext.getConf.get("spark.test.model_dir", "model")

  new MlTest(session, new ReaderImpl(session, "", inputTopic), modelPath, outputTopic).run()
}
