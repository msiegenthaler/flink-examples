import sbt.Keys._
name := "Example1"

version := "0.1-SNAPSHOT"

organization := "ch.inventsoft"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "1.0.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
    libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.9" % flinkVersion,
    libraryDependencies += "org.apache.flink" %% "flink-connector-wikiedits" % flinkVersion,
    libraryDependencies += "com.dataartisans" % "flink-training-exercises" % "0.3.1"
  )


mainClass in assembly := Some("ch.inventsoft.WikipediaEdits")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
