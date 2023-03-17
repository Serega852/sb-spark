import org.apache.spark.sql.SparkSession

object train extends App {
  val session = SparkSession.builder()
                            .appName("sergey_maslakov_lab07")
                            .getOrCreate()
  session.conf.set("spark.sql.session.timeZone", "UTC")

  val jsonPath = session.sparkContext.getConf.get("spark.test.json")
  val modelPath = session.sparkContext.getConf.get("spark.test.model_dir")

  new MlTrainer(session, new ReaderImpl(session, jsonPath, ""), modelPath).run()
}
