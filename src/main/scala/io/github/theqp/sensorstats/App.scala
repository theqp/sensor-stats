package io.github.theqp.sensorstats

import cats.effect.IOApp
import cats.effect.IO
import cats.effect.std.Console
import cats.effect.ExitCode
import fs2.io.file.Path
import java.nio.file.InvalidPathException
import cats.effect.SyncIO.syncForSyncIO

final case class IllegalArgs(args: List[String])
    extends Exception(
      s"Illegal args provided '${args.mkString(" ")}', expected a single arguement which is a path."
    )
object App extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    (args match
      case path :: Nil =>
        reportFromPath[IO](Path(path))
          .flatMap(report =>
            report.toStat[IO]().through(fs2.io.stdoutLines()).compile.drain
          )
          .as(ExitCode.Success)
      case args =>
        IO.raiseError(IllegalArgs(args))
    )
      .handleErrorWith { e =>
        e match
          case _: InvalidPathException | _: InvalidCsvRow =>
            Console[IO]
              .errorln(e.getMessage)
              .as(ExitCode.Error)
          case e => IO.raiseError(e)
      }
