ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

val flinkVersion = "1.14.6"

val flinkDependencies = Seq("org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
)
lazy val root = (project in file("."))
  .settings(
    name := "scala",
    libraryDependencies ++= flinkDependencies
  )


