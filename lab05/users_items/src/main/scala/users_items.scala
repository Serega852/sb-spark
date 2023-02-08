import org.apache.spark.sql.{SaveMode, SparkSession}

object users_items extends App {
  private def isUpdate = {
    session.sparkContext.getConf.get("spark.users_items.update", "1") == "1"
  }

  private val session = SparkSession.builder()
                                    .appName("sergey_maslakov_lab05")
                                    .config("spark.sql.session.timeZone", "UTC")
                                    .getOrCreate()
  val inputDir = session.sparkContext.getConf.get("spark.users_items.input_dir")
  val outputDir = session.sparkContext.getConf.get("spark.users_items.output_dir")

  new Matrix(session, inputDir, outputDir, isUpdate).run
}
