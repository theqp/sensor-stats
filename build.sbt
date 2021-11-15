lazy val root = project
  .in(file("."))
  .settings(
    name := "sensor-stats",
    description := "A command line program that calculates statistics from humidity sensor data",
    version := "0.1.0",
    scalaVersion := "3.1.0"
  )
libraryDependencies ++= Seq("org.typelevel" %% "cats-effect" % "3.2.9")
