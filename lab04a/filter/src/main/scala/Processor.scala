import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.nio.file.Paths

class Processor(session: SparkSession, reader: Reader, outputDir: String) {
  private val basePath = Paths.get(outputDir)
  import session.implicits._

  def run() = {
    val raw = reader.read
                    .select($"value".cast("string"))
                    .as[String]
                    .cache()
    write(raw, "buy")
    write(raw, "view")
  }

  private def write(df: Dataset[String], eventType: String): Unit = {
    getFilteredDf(df, eventType)
      .select($"*", $"date".as("p_date"))
      .write
      .partitionBy("p_date")
      .mode(SaveMode.Overwrite)
      .json(basePath.resolve(eventType).toString)
  }

  private def getFilteredDf(raw: Dataset[String], eventType: String) = {
    session.read
           .json(raw)
           .withColumn("date", date_format(($"timestamp" / 1000).cast("timestamp"), "YYYYMMdd"))
           .filter(s"event_type == '${eventType}'")
  }
}
