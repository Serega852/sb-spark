import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, callUDF, col, concat, count, date_format, desc, explode, from_unixtime, lit, lower, regexp_replace, when}

class DataTransformation(spark: SparkSession, reader: Reader, resultPath: String) {

  import spark.implicits._

  def transform() = {
    val sites = reader.readWebLog()
                      .withColumn("visit", explode($"visits"))
                      .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
                      .withColumn("domain", regexp_replace($"host", "www.", ""))
                      .select("uid", "visit.timestamp", "domain")
                      .alias("sites")
                      .cache()

    val webDays = sites
      .withColumn("day", lower(date_format(($"sites.timestamp" / 1000).cast("timestamp"), "E")))
      .withColumn("day", concat(lit("web_day_"), $"day"))
      .select($"sites.uid".as("uid"), $"sites.domain".as("domain"), $"day")
      .groupBy("uid")
      .pivot("day")
      .count().na.fill(0)
      .drop("day")
      .drop("domain")

    val webHour = sites
      .withColumn("time", lower(date_format(($"sites.timestamp" / 1000).cast("timestamp"), "H")))
      .select($"uid", $"sites.domain".as("domain"), $"time")
      .withColumn("time", concat(lit("web_hour_"), $"time"))
      .groupBy("uid")
      .pivot("time")
      .count().na.fill(0)
      .drop("time")
      .drop("domain")

    val work = sites
      .withColumn("time", lower(date_format(($"sites.timestamp" / 1000).cast("timestamp"), "H")))
      .withColumn("web_fraction_work_hours", when($"time" >= 9 && $"time" < 18, "work"))
      .groupBy("uid")
      .count().na.fill(0)
      .drop("web_fraction_work_hours")
      .select($"uid", $"count".as("web_fraction_work_hours"))
      .drop("time")

    val evening = sites
      .withColumn("time", lower(date_format(($"sites.timestamp" / 1000).cast("timestamp"), "H")))
      .withColumn("web_fraction_evening_hours", when(($"time" >= 18 && $"time" < 23) || $"time" === 0, "evening"))
      .groupBy("uid")
      .count().na.fill(0)
      .drop("web_fraction_evening_hours")
      .select($"uid", $"count".as("web_fraction_evening_hours"))
      .drop("time")
      .drop("domain")

    val top = sites
      .where("domain is not null")
      .groupBy("domain")
      .count().as("count")
      .orderBy(desc("count"))
      .limit(1000)
      //            .limit(10)
      .select($"count.domain".as("domain"))

    val columnSites = sites.join(top.alias("top"), $"sites.domain" === $"top.domain", "left")
                           .groupBy("uid")
                           .pivot("top.domain")
                           .count().na.fill(0)
                           .drop("null")
                           .cache()

    val sitesColumnNames = columnSites.schema.fields.map(v => v.name)
                                      .filter(v => !v.equals("uid") && !v.equals("domain"))
                                      .sorted
                                      .map(v => s"`${v}`")
                                      .map(col)

    val domainsFeatures = columnSites.withColumn("domains_features", array(sitesColumnNames: _*))
                                     .select("uid", "domains_features")

    val result = webDays.join(webHour, "uid")
                        .join(work, "uid")
                        .join(evening, "uid")
                        .join(domainsFeatures, "uid")

    reader.readUserItems().as("parquet")
          .join(result.as("result"), $"parquet.uid" === $"result.uid", "left")
          .write
          .parquet(resultPath)


    //          .where("uid = 'bb31cbb5-78c6-4980-92b9-781a370bc781'")
    //          .show(false)
  }
}
