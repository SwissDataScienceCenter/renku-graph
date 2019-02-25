/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.triplesgenerator.eventprocessing.filelog

import java.io.{BufferedReader, File, FileReader}

import cats.effect.IO._
import cats.effect._
import cats.implicits._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.triplesgenerator.eventprocessing.{EventProcessor, EventProcessorRunner}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class FileEventProcessorRunner(
    eventProcessor: EventProcessor[IO],
    configProvider: LogFileConfigProvider[IO] = new LogFileConfigProvider[IO](),
    logger:         Logger[IO] = ApplicationLogger
)(
    implicit contextShift: ContextShift[IO],
    executionContext:      ExecutionContext
) extends EventProcessorRunner[IO](eventProcessor) {

  import FileEventProcessorRunner.interval

  private implicit val timer: Timer[IO] = IO.timer(executionContext)

  lazy val run: IO[Unit] =
    for {
      file <- contextShift.shift *> validateFile
      _    <- fileReader(file).bracket(startProcessingLines)(closeReader)
    } yield ()

  private lazy val validateFile: IO[File] =
    configProvider.get.map(_.toFile)

  private def fileReader(file: File): IO[BufferedReader] =
    IO(new BufferedReader(new FileReader(file)))

  private def startProcessingLines(reader: BufferedReader): IO[Unit] =
    for {
      _ <- logger.info("Listening for new events")
      _ <- checkForNewLine(reader)
    } yield ()

  private def checkForNewLine(reader: BufferedReader): IO[Unit] =
    for {
      maybeLine <- IO(Option(reader.readLine()))
      _         <- sleepOrProcessLine(maybeLine)
      _         <- checkForNewLine(reader)
    } yield ()

  private lazy val sleepOrProcessLine: Option[String] => IO[_] = {
    case None => IO.sleep(interval)
    case Some(line) =>
      contextShift.shift *>
        (IO.pure() flatMap (_ => eventProcessor(line))).start
  }

  private def closeReader(reader: BufferedReader): IO[Unit] =
    IO(reader.close())
}

private object FileEventProcessorRunner {
  import scala.concurrent.duration._
  import scala.language.postfixOps

  val interval: FiniteDuration = 500 millis
}
