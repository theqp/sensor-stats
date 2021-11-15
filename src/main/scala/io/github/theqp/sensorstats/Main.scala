package io.github.theqp.sensorstats

import cats.effect.{IO, IOApp}
import cats.effect.kernel.Resource
import cats.effect.ExitCode
import cats.effect.std.Console
import fs2.io.file.Files
import fs2.io.file.Path

sealed abstract class SensorStatError(message: String)
    extends Exception(message)
final case class IllegalArgs(args: List[String])
    extends SensorStatError(
      s"Illegal args provided '${args.mkString(" ")}', expected a single arguement which is a path."
    )

final case class SensorStatReport(processedFiles: Long):
  def show(): String = s"Num of processed files: $processedFiles"

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    (args match
      case path :: Nil =>
        run(path)
          .map(_.show())
          .flatMap(IO.println)
          .as(ExitCode.Success)
      case args =>
        IO.raiseError(IllegalArgs(args))
    )
      .handleErrorWith {
        case e: SensorStatError =>
          Console[IO]
            .errorln(s"Error: ${e.getMessage}")
            .as(ExitCode.Error)
        case e => IO.raiseError(e)
      }

  def run(path: String): IO[SensorStatReport] =
    Files[IO]
      .list(Path(path))
      .filter(_.extName == ".csv")
      .compile
      .count
      .map(count => SensorStatReport(count))
