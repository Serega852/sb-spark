import org.apache.spark.sql.SparkSession

object MainTest extends App {
  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab03")
                                    .master("local[*]")
                                    .getOrCreate()
  session.sparkContext.setLogLevel("WARN")
  new DataMarketProcessor(session, new DfReader(session), new ShowWriter()).writeMarketInfo()
}
