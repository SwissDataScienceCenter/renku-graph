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

package ch.datascience.triplesgenerator

import cats.effect.{ExitCode, IO, IOApp}
import ch.datascience.triplesgenerator.eventprocessing.filelog.FileEventProcessorRunner
import ch.datascience.triplesgenerator.eventprocessing._
import ch.datascience.triplesgenerator.init.{FusekiDatasetInitializer, IOFusekiDatasetInitializer}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object Microservice extends IOApp {

  private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global
  private val datasetInitializer = new IOFusekiDatasetInitializer

  private val eventProcessorRunner = new EventsSource[IO](new FileEventProcessorRunner(_))
    .withEventsProcessor(new IOCommitEventProcessor())

  override def run(args: List[String]): IO[ExitCode] =
    new MicroserviceRunner(datasetInitializer, eventProcessorRunner).run(args)
}

private class MicroserviceRunner(
    datasetInitializer:   FusekiDatasetInitializer[IO],
    eventProcessorRunner: EventProcessorRunner[IO]
) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _      <- datasetInitializer.run
      result <- eventProcessorRunner.run
    } yield ExitCode.Success
}
