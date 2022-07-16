ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.4"

lazy val root = (project in file("."))
  .settings(
    name := "diamine"
  )



resolvers += "spark-packages" at "https://repos.spark-packages.org"

libraryDependencies ++=Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "2.4.4",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
  "org.apache.spark" %% "spark-graphx" % "2.4.4",
  // https://mvnrepository.com/artifact/graphframes/graphframes
  "graphframes" % "graphframes" % "0.7.0-spark2.3-s_2.11",
  // https://mvnrepository.com/artifact/org.neo4j.driver/neo4j-java-driver
  "org.neo4j.driver" % "neo4j-java-driver" % "4.1.1",
  // https://mvnrepository.com/artifact/neo4j-contrib/neo4j-connector-apache-spark
  "neo4j-contrib" %% "neo4j-connector-apache-spark" % "4.0.0"
)
