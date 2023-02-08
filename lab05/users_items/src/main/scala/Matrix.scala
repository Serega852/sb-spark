import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, concat, lit, lower, regexp_replace}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

class Matrix(spark: SparkSession, inputDir: String, outputDir: String, isUpdate: Boolean) {

  import spark.implicits._

  def run = {
    val view = spark.read.json(s"${inputDir}/view")
    val buy = spark.read.json(s"${inputDir}/buy")

    val maxDate: Int = view.select("p_date")
                           .union(buy.select("p_date"))
                           .agg(functions.max($"p_date").as("date"))
                           .first()
                           .getAs("date")

    val readingCondition = if (isUpdate) s"p_date == ${maxDate}" else "true"

    val calculatedMatrix = prepareDf(view, "view", readingCondition)
      .unionByName(prepareDf(buy, "buy", readingCondition))
      .groupBy("uid")
      .pivot("item_id")
      .count()
      .na
      .fill(0)
      .drop("item_id")

    if (isUpdate) {
      val outputDf = spark
        .read
        .parquet(s"${outputDir}/*")

      val mergedCols = outputDf.columns.toSet ++ calculatedMatrix.columns.toSet
      outputDf
        .select(getNewColumns(outputDf.columns.toSet, mergedCols): _*)
        .union(calculatedMatrix.select(getNewColumns(calculatedMatrix.columns.toSet, mergedCols): _*))
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"${outputDir}/${maxDate}")
    } else {
      calculatedMatrix
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"${outputDir}/${maxDate}")
    }
  }

  private def prepareDf(df: DataFrame, prefix: String, readingCondition: String) = {
    df.filter(readingCondition)
      .select("uid", "item_id")
      .filter("uid is not null")
      .withColumn("item_id",
                  concat(lit(s"${prefix}_"),
                         lower(regexp_replace($"item_id", "[ -]", "_"))))
      .cache()
  }

  def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
    merged_cols.toList.map {
      case x if column.contains(x) => col(x)
      case x => lit(0).as(x)
    }
  }
}
