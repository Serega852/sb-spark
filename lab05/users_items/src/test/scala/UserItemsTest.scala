import org.apache.spark.sql.{SaveMode, SparkSession}

object UserItemsTest extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab05")
                                    .master("local[*]")
                                    .config("spark.sql.session.timeZone", "UTC")
                                    .getOrCreate()
  session.sparkContext.setLogLevel("WARN")
  new Matrix(session, "../../../data/laba04/result", "target/result", true).run
}
