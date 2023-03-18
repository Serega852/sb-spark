name := "dashboard"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided
  )

libraryDependencies += "io.netty" % "netty-all" % "4.1.68.Final" % Provided
libraryDependencies += "io.netty" % "netty-buffer" % "4.1.68.Final" % Provided
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.8.22" % Provided
