import org.apache.spark.sql.SparkSession

object MlTrainerTest extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab07")
                                    .master("local[*]")
                                    .config("spark.executor.memory", "8g")
                                    .config("spark.sql.session.timeZone", "UTC")
                                    .getOrCreate()
  session.sparkContext.setLogLevel("WARN")
  new MlTrainer(session, new LocalReader(session), "target/result/model").run()
//  new MlTest(session, new LocalReader(session), "target/result/model", "sergey_maslakov_lab07_out").run()
}
