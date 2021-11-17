package io.github.theqp.sensorstats

import cats.*
import cats.data.*
import cats.effect.kernel.Async
import cats.implicits.*
import cats.kernel.CommutativeMonoid
import cats.kernel.CommutativeSemigroup
import fs2.Pipe
import fs2.Pull
import fs2.RaiseThrowable
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Path
import fs2.text

import scala.collection.immutable.ArraySeq

import util.chaining.scalaUtilChainingOps
import scala.collection.immutable.SortedMap

final case class InvalidCsvRow(row: String)
    extends Exception(
      s"Invalid row found in a csv: '$row'."
    )

/** a report created after processing all files */
final case class Report(files: Long, fileReport: FileReport):
  def processedMeasurements: BigInt =
    fileReport.failedMeasurements +
      fileReport.sensorStats.valuesIterator
        .map {
          case p: SensorStat.Processed =>
            p.measurementCount
          case SensorStat.OnlyFailed =>
            0
        }
        .fold(0.toLong)(_ + _)
  /* this could be implemented doing one iteration,
   * but would require storing measurementCount for all sensors
   */
  def toStat[F[_]](): Stream[F, String] = Stream(s"""
Num of processed files: $files
Num of processed measurements: ${processedMeasurements}
Num of failed measurements: ${fileReport.failedMeasurements}

Sensors with highest avg humidity:

sensor-id,min,avg,max
""").append(
    Stream
      .emits(fileReport.sortedStats)
      .map((id, stat) =>
        s"$id,${stat match
          case SensorStat.OnlyFailed =>
            "NaN,NaN,NaN" // Batman!
          case p: SensorStat.Processed =>
            s"${p.min},${p.avg.setScale(2).bigDecimal.stripTrailingZeros},${p.max}"
        }\n"
      )
  )

given CommutativeMonoid[Report] with
  val empty = Report(files = 0, fileReport = Monoid[FileReport].empty)
  def combine(x: Report, y: Report): Report =
    Report(
      files = x.files + y.files,
      fileReport = x.fileReport.combine(y.fileReport)
    )

private type SensorName = String
private final case class FileReport(
    failedMeasurements: Long,
    sensorStats: SortedMap[SensorName, SensorStat]
):
  def sortedStats: ArraySeq[(SensorName, SensorStat)] =
    // sort by average, if they are the same then key ordering applies
    sensorStats
      .to(ArraySeq)
      .sorted(Ordering.by((_, stat) => stat))

private given CommutativeMonoid[FileReport] with
  val empty = FileReport(0, SortedMap.empty)
  def combine(x: FileReport, y: FileReport): FileReport =
    FileReport(
      failedMeasurements = x.failedMeasurements + y.failedMeasurements,
      sensorStats = x.sensorStats.combine(y.sensorStats)
    )

/** Reduce memory usage by avoiding Either/Option. */
private enum SensorStat:
  case OnlyFailed
  case Processed(
      min: Byte,
      max: Byte,
      measurementCount: Long,
      measurementSum: Long
  )
extension (s: SensorStat.Processed)
  def avg: BigDecimal = BigDecimal(s.measurementSum) / s.measurementCount
private given CommutativeSemigroup[SensorStat] with
  def combine(x: SensorStat, y: SensorStat): SensorStat =
    (x, y) match
      case (SensorStat.OnlyFailed, y) => y
      case (x, SensorStat.OnlyFailed) => x
      case (x: SensorStat.Processed, y: SensorStat.Processed) =>
        SensorStat.Processed(
          min = x.min.min(y.min),
          max = x.max.max(y.max),
          measurementCount = x.measurementCount + y.measurementCount,
          measurementSum = x.measurementSum + y.measurementSum
        )
private given Ordering[SensorStat] with
  def compare(x: SensorStat, y: SensorStat): Int =
    import SensorStat.*
    x match
      case x: Processed =>
        y match
          case y: Processed => y.avg.compare(x.avg)
          case OnlyFailed   => -1
      case OnlyFailed =>
        y match
          case _: Processed => 1
          case OnlyFailed   => 0

private enum Humidity:
  case Processed(value: Byte)
  case Failed
private object Humidity:
  def apply(s: String): Option[Humidity] =
    s match
      case "NaN" => Some(Humidity.Failed)
      case s =>
        s.toByteOption.filter((0 to 100).contains).map(Humidity.Processed.apply)

private final case class CsvRow(sensorName: String, humidity: Humidity)

/** see reportFromFileLines for raised errors */
def reportFromPath[F[_]: RaiseThrowable: Async](
    folderPath: Path
): F[Report] =
  Files[F]
    .list(folderPath)
    .filter(_.extName == ".csv")
    .map(path =>
      Files[F]
        .readAll(path)
        .through(text.utf8.decode)
        .through(text.lines)
    )
    .pipe(reportFromFileLines)

/** raises InvalidCsvFileLine in case of invalid input */
def reportFromFileLines[F[_]: RaiseThrowable](
    files: Stream[F, Stream[F, String]]
)(using fs2.Compiler[F, F]): F[Report] =
  files
    .evalMap(readFileReport[F])
    .map(Report(files = 1, _))
    .compile
    .foldMonoid

private def readFileReport[F[_]: RaiseThrowable](
    lines: Stream[F, String]
)(using fs2.Compiler[F, F]): F[FileReport] =
  lines
    .through(csvRows)
    .map {
      case CsvRow(name, Humidity.Processed(value)) =>
        FileReport(
          failedMeasurements = 0,
          sensorStats = SortedMap(
            name ->
              SensorStat.Processed(
                min = value,
                max = value,
                measurementCount = 1,
                measurementSum = value
              )
          )
        )
      case CsvRow(name, Humidity.Failed) =>
        FileReport(
          failedMeasurements = 1,
          sensorStats = SortedMap(name -> SensorStat.OnlyFailed)
        )
    }
    .compile
    .foldMonoid

private def csvRows[F[_]: RaiseThrowable]: Pipe[F, String, CsvRow] =
  def parseCsvRow(row: String) =
    row
      .split(',')
      .pipe {
        case Array(sensorName, humidity) =>
          Humidity(humidity).map(CsvRow(sensorName, _))
        case _ => None
      }
      .toRight(InvalidCsvRow(row))
  _.pull.uncons1
    .flatMap {
      case Some(header, rest) =>
        Option
          .when(header != "sensor-id,humidity")(
            Left(InvalidCsvRow(header))
          )
          .pipe(Stream.fromOption(_))
          .append(rest.map(parseCsvRow))
          .pull
          .echo
      case None => Pull.done
    }
    .stream
    .rethrow
