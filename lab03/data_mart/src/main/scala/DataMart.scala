import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.net.{URL, URLDecoder}
import scala.util.Try

class DataMart(session: SparkSession, reader: Reader) {

  import session.implicits._

  def writeData(): Unit = {
    val clients = reader.cassandra.where("uid is not null")
    val visits = reader.elasticsearch.where("uid is not null")
    val logs = reader.json.where("uid is not null")
    val cats = reader.postgres

    val clientsAgeCat = clients
      .withColumn("age_cat",
                  when($"age" >= 18 && $"age" <= 24, "18-24")
                    .when($"age" >= 25 && $"age" <= 34, "25-34")
                    .when($"age" >= 35 && $"age" <= 44, "35-44")
                    .when($"age" >= 45 && $"age" <= 54, "45-54")
                    .otherwise(">=55"))
      .drop("age")

    val visitsCategories = visits
      .withColumn("category", concat(lit("shop_"), lower(regexp_replace($"category", "[ -]", "_"))))
      .groupBy("uid", "category")
      .pivot("category")
      .count()
      .na
      .fill(0)
      .drop("category")

    val preparedWebCategories = cats.withColumn("category", concat(lit("web_"), $"category"))

    val logsWebCategories = logs
      .withColumn("visits", explode($"visits"))
      .select($"uid", $"visits.url")
      .select($"uid", decodeUrlAndGetDomain($"url").as("domain"))
      .join(preparedWebCategories, "domain")
      .groupBy("uid", "category")
      .pivot("category")
      .count()
      .na
      .fill(0)
      .drop("category")

    clientsAgeCat
      .join(visitsCategories, "uid")
      .join(logsWebCategories, "uid")
      .show()
  }

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.replaceAll("^www\\.", "")
    }.getOrElse("")
  })
}
