import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.collection.mutable
import scala.util.Try

class MlTrainer(spark: SparkSession, reader: Reader, resultPath: String) {

  import spark.implicits._

  def run() = {
    val training = reader.readJson()
                         .select("uid", "visits.url", "gender_age")
                         .select($"uid", decodeUrlAndGetDomain($"url").as("domains"), $"gender_age")

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val labels = indexer.fit(training.select($"gender_age")).labels

    val ItoS = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("gender_age_tmp")
      .setLabels(labels)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, ItoS))

    pipeline.fit(training).write.overwrite().save(resultPath)
  }

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((urls: mutable.WrappedArray[String]) => {
    urls.map(url => Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.replaceAll("^www\\.", "")
    }.getOrElse(""))
        .filter(v => v.nonEmpty)
  })


}
