import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{from_json, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import java.net.{URL, URLDecoder}
import scala.collection.mutable
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
                                         .write
                                         .format("kafka")
                                         .option("kafka.bootstrap.servers", "spark-master-1.newprolab.com:6667")
                                         .option("subscribe", outputTopic)
                                         .save())
          .start()
          .awaitTermination()

    //    val test = spark.read.parquet("../../../data/laba07/kafka")
    //                    .select($"value".cast("string"))
    //                    .as[String]
    //                    .select(from_json($"value", jsonSchema).as("value"))
    //                    .select($"value.uid".as("uid"), $"value.visits.url".as("url"))
    //                    .select($"uid", decodeUrlAndGetDomain($"url").as("domains"))
    //
    //    test.show()
    //    model.transform(test).select($"uid", $"gender_age_tmp".as("gender_age")).show()
  }

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((urls: mutable.WrappedArray[String]) => {
    urls.map(url => Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.replaceAll("^www\\.", "")
    }.getOrElse(""))
        .filter(v => v.nonEmpty)
  })
}
