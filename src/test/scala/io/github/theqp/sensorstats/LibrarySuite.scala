package io.github.theqp.sensorstats

import cats.effect.IO
import cats.effect.SyncIO
import cats.effect.kernel.Resource.Pure
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Path
import munit.CatsEffectSuite

import scala.collection.immutable.TreeMap

class LibrarySuite extends CatsEffectSuite:
  test("reads correctly the example csvs") {
    reportFromPath[IO](Path("example_csvs")).assertEquals(
      Report(
        files = 2,
        fileReport = FileReport(
          failedMeasurements = 2,
          sensorStats = TreeMap(
            "s1" -> SensorStat.Processed(
              min = 10,
              max = 98,
              measurementCount = 2,
              measurementSum = 108
            ),
            "s2" -> SensorStat.Processed(
              min = 78,
              max = 88,
              measurementCount = 3,
              measurementSum = 246
            ),
            "s3" -> SensorStat.OnlyFailed
          )
        )
      )
    )
  }
  test("can read csvs from lines") {
    reportFromFileLines[IO](
      Stream
        .emit(Stream("sensor-id,humidity", "s1,10"))
        .repeatN(2)
    ).assertEquals(
      Report(
        files = 2,
        fileReport = FileReport(
          failedMeasurements = 0,
          sensorStats = TreeMap(
            "s1" -> SensorStat.Processed(
              min = 10,
              max = 10,
              measurementCount = 2,
              measurementSum = 20
            )
          )
        )
      )
    )
  }
  test("validates header") {
    reportFromFileLines[IO](Stream.emit(Stream("sensorid,humidity")))
      .map(Right.apply)
      .handleError(Left.apply)
      .assertEquals(Left(InvalidCsvRow("sensorid,humidity")))
  }
  test("rows can only consist of two columns") {
    reportFromFileLines[IO](
      Stream
        .emit(Stream("sensor-id,humidity", "foo,1,2"))
    ).map(Right.apply)
      .handleError(Left.apply)
      .assertEquals(Left(InvalidCsvRow("foo,1,2")))
  }
  test("second column only can be an integer") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "foo,1.0"))
    ).map(Right.apply)
      .handleError(Left.apply)
      .assertEquals(Left(InvalidCsvRow("foo,1.0")))
  }
  test("second column must not be larger than 100") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "foo,101"))
    ).map(Right.apply)
      .handleError(Left.apply)
      .assertEquals(Left(InvalidCsvRow("foo,101")))
  }
  test("second column must not be smaller than 0") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "foo,-1"))
    ).map(Right.apply)
      .handleError(Left.apply)
      .assertEquals(Left(InvalidCsvRow("foo,-1")))
  }
  test("second column can be 0") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,0"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 0,
          sensorStats = TreeMap(
            "s" -> SensorStat.Processed(
              min = 0,
              max = 0,
              measurementCount = 1,
              measurementSum = 0
            )
          )
        )
      )
    )
  }
  test("second column can be 100") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,100"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 0,
          sensorStats = TreeMap(
            "s" -> SensorStat.Processed(
              min = 100,
              max = 100,
              measurementCount = 1,
              measurementSum = 100
            )
          )
        )
      )
    )
  }
  test("second column can be NaN") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,NaN"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 1,
          sensorStats = TreeMap("s" -> SensorStat.OnlyFailed)
        )
      )
    )
  }
  test("failed measurements do increment") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,NaN", "s,NaN"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 2,
          sensorStats = TreeMap("s" -> SensorStat.OnlyFailed)
        )
      )
    )
  }
  test("failed measurements do increment through files") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,NaN")).repeatN(2)
    ).assertEquals(
      Report(
        files = 2,
        fileReport = FileReport(
          failedMeasurements = 2,
          sensorStats = TreeMap("s" -> SensorStat.OnlyFailed)
        )
      )
    )
  }
  test("failed measurements do increment through sensors") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,NaN", "s2,NaN"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 2,
          sensorStats =
            TreeMap("s" -> SensorStat.OnlyFailed, "s2" -> SensorStat.OnlyFailed)
        )
      )
    )
  }
  test("failed measurements are overrided by succesful ones") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,NaN", "s,1"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 1,
          sensorStats = TreeMap("s" -> SensorStat.Processed(1, 1, 1, 1))
        )
      )
    )
  }
  test("succesful measurements are not overrided by failed ones") {
    reportFromFileLines[IO](
      Stream.emit(Stream("sensor-id,humidity", "s,1", "s,NaN"))
    ).assertEquals(
      Report(
        files = 1,
        fileReport = FileReport(
          failedMeasurements = 1,
          sensorStats = TreeMap("s" -> SensorStat.Processed(1, 1, 1, 1))
        )
      )
    )
  }
  test("averages are calculated precisely") {
    IO.pure(SensorStat.Processed(1, 5, 3, 7).avg)
      .assertEquals(BigDecimal(7) / 3)
  }
  test("generates report in the format specified in example") {
    Report(
      files = 2,
      fileReport = FileReport(
        failedMeasurements = 2,
        sensorStats = TreeMap(
          "s1" -> SensorStat.Processed(
            min = 10,
            max = 98,
            measurementCount = 2,
            measurementSum = 108
          ),
          "s2" -> SensorStat.Processed(
            min = 78,
            max = 88,
            measurementCount = 3,
            measurementSum = 246
          ),
          "s3" -> SensorStat.OnlyFailed
        )
      )
    ).toStat[IO]()
      .compile
      .fold("")(_ + _)
      .assertEquals("""Num of processed files: 2
Num of processed measurements: 7
Num of failed measurements: 2

Sensors with highest avg humidity:

sensor-id,min,avg,max
s2,78,82,88
s1,10,54,98
s3,NaN,NaN,NaN
""")
  }

  test("averages are show with 2 precisions when decimals are available") {
    Report(
      files = 1,
      fileReport = FileReport(
        failedMeasurements = 0,
        sensorStats = TreeMap(
          "s" -> SensorStat.Processed(1, 5, 3, 7)
        )
      )
    ).toStat[IO]()
      .compile
      .fold("")(_ + _)
      .assertEquals("""Num of processed files: 1
Num of processed measurements: 3
Num of failed measurements: 0

Sensors with highest avg humidity:

sensor-id,min,avg,max
s,1,2.33,5
""")
  }
