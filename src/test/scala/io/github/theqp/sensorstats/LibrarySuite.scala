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
  test("can read csvs from the provided path") {
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
