import org.apache.spark.sql.SparkSession

object FilterTest extends App {
  val session = SparkSession.builder()
                            .master("local[*]")
                            .config("spark.sql.session.timeZone", "UTC")
                            .config("spark.filter.output_dir_prefix", "target/result")
                            .getOrCreate()
  session.sparkContext.setLogLevel("WARN")
  val outputDirPrefix = session.sparkContext.getConf.get("spark.filter.output_dir_prefix")
  new Processor(session, new DfReader(session), outputDirPrefix).run
}
