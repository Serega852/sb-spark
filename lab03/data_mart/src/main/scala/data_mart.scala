import org.apache.spark.sql.SparkSession

object data_mart extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab03")
                                    .getOrCreate()
  new DataMarketProcessor(session, new DataReader(session), new PostgresWriter()).writeMarketInfo()
}
