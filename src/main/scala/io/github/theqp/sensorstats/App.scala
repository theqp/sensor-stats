package io.github.theqp.sensorstats

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.std.Console
import fs2.io.file.Path
import java.nio.file.NoSuchFileException
import util.chaining.scalaUtilChainingOps

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
        e.pipe {
          case e: NoSuchFileException =>
            Some(s"Cannot access '${e.getFile}': No such directory")
          case e: IllegalArgs =>
            Some(e.getMessage())
          case e: java.nio.file.NotDirectoryException =>
            Some(s"${e.getFile} is not a directory")
          case _ => None
        }.pipe {
          case Some(msg) => Console[IO].errorln(msg).as(ExitCode.Error)
          case None      => IO.raiseError(e)
        }

      }
