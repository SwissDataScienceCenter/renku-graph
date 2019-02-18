package ch.datascience.triplesgenerator.eventprocessing.filelog

import java.io.{BufferedReader, File, FileReader}
import java.nio.file.Path

import cats.effect.IO._
import cats.effect._
import cats.implicits._
import ch.datascience.triplesgenerator.eventprocessing.EventProcessorRunner

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private[eventprocessing] class FileEventProcessorRunner(
    eventLogFilePath: Path,
    eventProcessor:   String => IO[Unit]
)(
    implicit contextShift: ContextShift[IO],
    executionContext:      ExecutionContext
) extends EventProcessorRunner[IO](eventProcessor) {

  import FileEventProcessorRunner.interval

  private implicit val timer: Timer[IO] = IO.timer(executionContext)

  lazy val run: IO[ExitCode] =
    for {
      file <- validateFile
      _    <- fileReader(file).bracket(checkForNewLine)(closeReader)
    } yield ExitCode.Success

  private lazy val validateFile: IO[File] = IO {
    eventLogFilePath.toFile
  }

  private def fileReader(file: File): IO[BufferedReader] =
    contextShift.shift *> IO(new BufferedReader(new FileReader(file)))

  private def checkForNewLine(reader: BufferedReader): IO[Unit] =
    IO(Option(reader.readLine()))
      .flatMap(sleepOrProcessLine)
      .flatMap(_ => checkForNewLine(reader))

  private lazy val sleepOrProcessLine: Option[String] => IO[Unit] = {
    case None => IO.sleep(interval)
    case Some(line) =>
      eventProcessor(line).recoverWith {
        case NonFatal(_) => IO.unit
      }
  }

  private def closeReader(reader: BufferedReader): IO[Unit] =
    IO(reader.close())
}

private object FileEventProcessorRunner {
  import scala.concurrent.duration._
  import scala.language.postfixOps

  val interval: FiniteDuration = 500 millis
}
