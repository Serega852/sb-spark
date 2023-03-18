import org.apache.spark.sql.SparkSession

object dashboard extends App {
  val session = SparkSession.builder()
                            .appName("sergey_maslakov_lab08")
                            .getOrCreate()
  session.conf.set("spark.sql.session.timeZone", "UTC")

  val jsonPath = session.sparkContext.getConf.get("spark.dashboard.json_path")
  val modelPath = session.sparkContext.getConf.get("spark.dashboard.model_path")

  new DashboardPublisher(session, jsonPath, modelPath).run()
}
