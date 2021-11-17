lazy val root = project
  .in(file("."))
  .settings(
    name := "sensor-stats",
    description := "A command line program that calculates statistics from humidity sensor data",
    version := "0.1.0",
    scalaVersion := "3.1.0"
  )
libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "3.2.9",
  "co.fs2" %% "fs2-core" % "3.2.2",
  "co.fs2" %% "fs2-io" % "3.2.2",
  "org.typelevel" %% "munit-cats-effect-3" % "1.0.6" % Test,
  "org.scalameta" %% "munit" % "0.7.29" % Test
)
