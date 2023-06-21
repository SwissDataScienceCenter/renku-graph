/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventProcessorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "do nothing if the given project does not exist in TS" in {

    syncRepoMetadataEvents[IO].map(_.generateOne) >>= { event =>
      givenTSDataFinding(event.path, returning = Option.empty[DataExtract.TS].pure[IO])
      givenGLDataFinding(event.path, returning = glDataExtracts(having = event.path).generateSome.pure[IO])

      processor.process(event).assertNoException
    }
  }

  it should "do nothing if the given project does not exist in GL" in {

    syncRepoMetadataEvents[IO].map(_.generateOne) >>= { event =>
      givenTSDataFinding(event.path, returning = tsDataExtracts(having = event.path).generateSome.pure[IO])
      givenGLDataFinding(event.path, returning = Option.empty[DataExtract.GL].pure[IO])

      processor.process(event).assertNoException
    }
  }

  it should "fetch relevant data from TS and GL, " +
    "extract data from the payload, " +
    "calculate relevant update commands and execute them" +
    "- case when no payload in the event" in {

      val event = syncRepoMetadataWithoutPayloadEvents.generateOne

      val tsData = tsDataExtracts(having = event.path).generateOne
      givenTSDataFinding(event.path, returning = tsData.some.pure[IO])

      val glData = glDataExtracts(having = event.path).generateOne
      givenGLDataFinding(event.path, returning = glData.some.pure[IO])

      val updates = updateCommands.generateList()
      givenUpdateCommandsCalculation(tsData, glData, maybePayloadData = None, returning = updates)

      givenUpdateCommandsExecution(updates, returning = ().pure[IO])

      processor.process(event).assertNoException
    }

  it should "fetch relevant data from TS and GL, " +
    "extract data from the payload, " +
    "calculate relevant update commands and execute them" +
    "- case with payload in the event" in {

      syncRepoMetadataWithPayloadEvents[IO].map(_.generateOne) >>= {
        case event @ SyncRepoMetadata(path, Some(payload)) =>
          val tsData = tsDataExtracts(having = path).generateOne
          givenTSDataFinding(path, returning = tsData.some.pure[IO])

          val glData = glDataExtracts(having = path).generateOne
          givenGLDataFinding(path, returning = glData.some.pure[IO])

          val maybePayloadData = payloadDataExtracts(having = path).generateOption
          givenPayloadDataExtraction(path, payload, returning = maybePayloadData.pure[IO])

          val updates = updateCommands.generateList()
          givenUpdateCommandsCalculation(tsData, glData, maybePayloadData, returning = updates)

          givenUpdateCommandsExecution(updates, returning = ().pure[IO])

          processor.process(event).assertNoException
        case _ => fail("expecting payload")
      }
    }

  it should "log an error in the case of a failure" in {

    syncRepoMetadataEvents[IO].map(_.generateOne) >>= { event =>
      val exception = exceptions.generateOne
      givenTSDataFinding(event.path, returning = exception.raiseError[IO, Nothing])

      val glData = glDataExtracts(having = event.path).generateOne
      givenGLDataFinding(event.path, returning = glData.some.pure[IO])

      processor.process(event).assertNoException >>
        logger.logged(Error(show"$categoryName: $event processing failure", exception)).pure[IO]
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val tsDataFinder             = mock[TSDataFinder[IO]]
  private lazy val glDataFinder             = mock[GLDataFinder[IO]]
  private lazy val payloadDataExtractor     = mock[PayloadDataExtractor[IO]]
  private lazy val updateCommandsCalculator = mock[UpdateCommandsCalculator]
  private lazy val updateCommandsRunner     = mock[UpdateCommandsRunner[IO]]
  private lazy val processor = new EventProcessorImpl[IO](tsDataFinder,
                                                          glDataFinder,
                                                          payloadDataExtractor,
                                                          updateCommandsCalculator,
                                                          updateCommandsRunner
  )

  private def givenTSDataFinding(path: projects.Path, returning: IO[Option[DataExtract.TS]]) =
    (tsDataFinder.fetchTSData _)
      .expects(path)
      .returning(returning)

  private def givenGLDataFinding(path: projects.Path, returning: IO[Option[DataExtract.GL]]) =
    (glDataFinder.fetchGLData _)
      .expects(path)
      .returning(returning)

  private def givenPayloadDataExtraction(path:      projects.Path,
                                         payload:   ZippedEventPayload,
                                         returning: IO[Option[DataExtract.Payload]]
  ) = (payloadDataExtractor.extractPayloadData _)
    .expects(path, payload)
    .returning(returning)

  private def givenUpdateCommandsCalculation(tsData:           DataExtract.TS,
                                             glData:           DataExtract.GL,
                                             maybePayloadData: Option[DataExtract.Payload],
                                             returning:        List[UpdateCommand]
  ) = (updateCommandsCalculator.calculateUpdateCommands _)
    .expects(tsData, glData, maybePayloadData)
    .returning(returning)

  private def givenUpdateCommandsExecution(updates: List[UpdateCommand], returning: IO[Unit]) =
    updates.foreach { update =>
      (updateCommandsRunner
        .run(_: UpdateCommand))
        .expects(update)
        .returning(returning)
    }
}
