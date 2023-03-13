import org.apache.spark.sql.SparkSession

object features extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab06")
                                    .master("local[*]")
                                    .config("spark.executor.memory", "8g")
                                    .config("spark.sql.session.timeZone", "UTC")
                                    .getOrCreate()
  new DataTransformation(session, new HdfsReader(session), "/user/sergey.maslakov/features").transform()
}
