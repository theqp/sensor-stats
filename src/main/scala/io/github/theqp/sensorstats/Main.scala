package io.github.theqp.sensorstats

import cats.effect.{IO, IOApp}
import cats.effect.kernel.Resource
import cats.effect.ExitCode
import cats.effect.std.Console

sealed abstract class SensorStatError(message: String)
    extends Exception(message)
final case class IllegalArgs(args: List[String])
    extends SensorStatError(
      s"Illegal args provided '${args.mkString(" ")}', expected a single arguement which is a path."
    )

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    (args match
      case path :: Nil => IO.pure(path)
      case args        => IO.raiseError(IllegalArgs(args))
    )
      .as(ExitCode.Success)
      .handleErrorWith {
        case e: SensorStatError =>
          Console[IO]
            .errorln(s"Error: ${e.getMessage}")
            .as(ExitCode.Error)
        case e => IO.raiseError(e)
      }
