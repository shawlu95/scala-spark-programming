version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.15"

name := "scala-spark-programming"

val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "joda-time" % "joda-time" % "2.10.14"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-programming"
  )
