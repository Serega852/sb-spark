import org.apache.spark.sql.SparkSession

object Main extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab06")
                                    .master("local[*]")
                                    .config("spark.executor.memory", "8g")
                                    .config("spark.sql.session.timeZone", "UTC")
                                    .getOrCreate()
  session.sparkContext.setLogLevel("WARN")
  new DataTransformation(session, new FileReader(session), "target/result").transform()
}
