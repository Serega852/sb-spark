import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReader(session: SparkSession) extends Reader {
  @Override
  def cassandra: DataFrame = {
    session.read
           .format("org.apache.spark.sql.cassandra")
           .option("spark.cassandra.connection.host", "10.0.0.31")
           .option("spark.cassandra.connection.port", "9042")
           .option("keyspace", "labdata")
           .option("table", "clients")
           .load
  }

  @Override
  def elasticsearch: DataFrame = {
    session.read
           .format("org.elasticsearch.spark.sql")
           .option("es.read.metadata", value = true)
           .option("es.nodes.wan.only", value = true)
           .option("es.net.ssl", value = false)
           .option("es.nodes", "10.0.0.31")
           .option("es.port", "9200")
           .load("visits")
  }

  @Override
  def json: DataFrame = {
    session.read.json("/labs/laba03/weblogs.json")
  }

  @Override
  def postgres: DataFrame = {
    session.read
           .format("jdbc")
           .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
           .option("dbtable", "domain_cats")
           .option("user", "sergey_maslakov")
           .option("password", "HP1LNBFQ")
           .option("driver", "org.postgresql.Driver")
           .load()
  }
}
