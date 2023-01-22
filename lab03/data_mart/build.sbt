name := "data_mart"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  )

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.22"
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"

libraryDependencies += "io.netty" % "netty-all" % "4.1.68.Final"
libraryDependencies += "io.netty" % "netty-buffer" % "4.1.68.Final"