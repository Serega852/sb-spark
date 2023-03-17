import org.apache.spark.sql.{DataFrame, SparkSession}

class LocalReader(spark: SparkSession) extends Reader {
  override def readJson(): DataFrame = {
    spark.read.json("../../../data/laba07/laba07.json")
  }

  override def readKafka(): DataFrame = {
    val schema = spark.read.parquet("../../../data/laba07/kafka").schema
    spark.readStream
         .schema(schema)
         .parquet("../../../data/laba07/kafka")
         .limit(1)
  }
}
