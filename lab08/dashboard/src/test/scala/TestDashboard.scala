import org.apache.spark.sql.SparkSession

object TestDashboard extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab08")
                                    .master("local[*]")
                                    .config("spark.executor.memory", "8g")
                                    .config("spark.sql.session.timeZone", "UTC")
                                    .getOrCreate()
  new DashboardPublisher(session,
                         "../../../data/laba08/laba08.json",
                         "../../../data/laba07/model")
    .run()
}
