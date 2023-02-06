import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{concat, desc, lit, lower, regexp_replace}

class Matrix(spark: SparkSession, inputDir: String, outputDir: String, saveMode: SaveMode) {
  import spark.implicits._

  def run = {
    val view = spark.read.json(s"${inputDir}/view")
    val buy = spark.read.json(s"${inputDir}/buy")

    val maxDate: Int = view.select("p_date")
                           .union(buy.select("p_date"))
                           .agg(functions.max($"p_date").as("date"))
                           .first()
                           .getAs("date")


    prepareDf(view, "view")
      .union(prepareDf(buy, "buy"))
      .groupBy("uid")
      .pivot("item_id")
      .count()
      .na
      .fill(0)
      .drop("item_id")
      .write
      .mode(saveMode)
      .parquet(s"${outputDir}/${maxDate}")
  }

  private def prepareDf(df: DataFrame, prefix: String) = {
    df.withColumn("item_id", concat(lit(s"${prefix}_"), lower(regexp_replace($"item_id", "[ -]", "_"))))
      .select("uid", "item_id")
      .filter("uid is not null")
      .cache()
  }
}
