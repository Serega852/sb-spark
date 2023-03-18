import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.collection.mutable
import scala.util.Try

class DashboardPublisher(spark: SparkSession, jsonPath: String, modelPath: String) {

  import spark.implicits._

  def run() = {
    val model = PipelineModel.load(modelPath)

    val df = spark.read
                  .json(jsonPath)
                  .select($"uid", $"date", decodeUrlAndGetDomain($"visits.url").as("domains"))
    model.transform(df)
         .select($"uid", $"date", $"gender_age_tmp".as("gender_age"))
         .write
         .format("org.elasticsearch.spark.sql")
         .option("es.nodes", "10.0.0.31")
         .option("es.port", "9200")
         .option("es.read.metadata", value = true)
         .option("es.nodes.wan.only", value = true)
         .option("es.net.ssl", value = false)
         .mode(SaveMode.Overwrite)
         .save("sergey_maslakov_lab08/_doc")

  }

  private def decodeUrlAndGetDomain: UserDefinedFunction = udf((urls: mutable.WrappedArray[String]) => {
    urls.map(url => Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.replaceAll("^www\\.", "")
    }.getOrElse(""))
        .filter(v => v.nonEmpty)
  })
}
