import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{from_json, lit, struct, to_json, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import java.net.{URL, URLDecoder}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

class MlTest(spark: SparkSession, reader: Reader, modelPath: String, outputTopic: String) {

  import spark.implicits._

  def run() = {
    val jsonSchema = new StructType()
      .add("uid", StringType)
      .add("visits", ArrayType(new StructType().add("url", StringType)))

    val model = PipelineModel.load(modelPath)

    reader.readKafka()
          .select($"value".cast("string"))
          .as[String]
          .select(from_json($"value", jsonSchema).as("value"))
          .select($"value.uid".as("uid"), $"value.visits.url".as("url"))
          .select($"uid", decodeUrlAndGetDomain($"url").as("domains"))
          .writeStream
          .format("console")
          .option("truncate", "false")
          .foreachBatch((df, id) => model.transform(df)
                                         .select($"uid", $"gender_age_tmp".as("gender_age"))
                                         .select(to_json(struct($"uid", $"gender_age")).as("value"))
                                         .withColumn("topic", lit(outputTopic))
                                         .select("topic", "value")
                                         .write
                                         .format("kafka")
                                         .option("kafka.bootstrap.servers", "spark-master-1.newprolab.com:6667")
                                         .save())
          .start()
          .awaitTermination(10.minutes.toMillis)
  }

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((urls: mutable.WrappedArray[String]) => {
    urls.map(url => Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.replaceAll("^www\\.", "")
    }.getOrElse(""))
        .filter(v => v.nonEmpty)
  })
}
