package io.github.theqp.sensorstats

import cats.effect.{IO, SyncIO}
import munit.CatsEffectSuite
import fs2.io.file.Path
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
