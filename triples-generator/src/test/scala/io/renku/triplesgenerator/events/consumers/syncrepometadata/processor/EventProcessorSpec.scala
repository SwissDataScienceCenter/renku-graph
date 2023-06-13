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
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import io.renku.triplesstore.SparqlQuery
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventProcessorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "do nothing if the given project does not exist in TS" in {

    syncRepoMetadataEvents[IO].map(_.generateOne).flatMap { event =>
      givenTSDataFinding(event.path, returning = Option.empty[DataExtract].pure[IO])
      givenGLDataFinding(event.path, returning = dataExtracts(having = event.path).generateSome.pure[IO])

      processor.process(event).asserting(_ shouldBe ())
    }
  }

  it should "do nothing if the given project does not exist in GL" in {

    syncRepoMetadataEvents[IO].map(_.generateOne).flatMap { event =>
      givenTSDataFinding(event.path, returning = dataExtracts(having = event.path).generateSome.pure[IO])
      givenGLDataFinding(event.path, returning = Option.empty[DataExtract].pure[IO])

      processor.process(event).asserting(_ shouldBe ())
    }
  }

  it should "fetch relevant data from TS and GL, " +
    "extract data from the payload, " +
    "generate relevant SPARQL upserts and run them" +
    "- case when no payload in the event" in {

      val event = syncRepoMetadataWithoutPayloadEvents.generateOne

      val tsData = dataExtracts(having = event.path).generateOne
      givenTSDataFinding(event.path, returning = tsData.some.pure[IO])

      val glData = dataExtracts(having = event.path).generateOne
      givenGLDataFinding(event.path, returning = glData.some.pure[IO])

      val upserts = sparqlQueries.generateList()
      givenUpsertsCalculation(tsData, glData, maybePayloadData = None, returning = upserts)

      givenUpsertsRunning(upserts, returning = ().pure[IO])

      processor.process(event).asserting(_ shouldBe ())
    }

  it should "fetch relevant data from TS and GL, " +
    "extract data from the payload, " +
    "generate relevant SPARQL upserts and run them" +
    "- case with payload in the event" in {

      syncRepoMetadataWithPayloadEvents[IO]
        .map(_.generateOne)
        .flatMap {
          case event @ SyncRepoMetadata(_, Some(payload)) =>
            val tsData = dataExtracts(having = event.path).generateOne
            givenTSDataFinding(event.path, returning = tsData.some.pure[IO])

            val glData = dataExtracts(having = event.path).generateOne
            givenGLDataFinding(event.path, returning = glData.some.pure[IO])

            val maybePayloadData = dataExtracts(having = event.path).generateOption
            givenPayloadDataExtraction(payload, returning = maybePayloadData.pure[IO])

            val upserts = sparqlQueries.generateList()
            givenUpsertsCalculation(tsData, glData, maybePayloadData, returning = upserts)

            givenUpsertsRunning(upserts, returning = ().pure[IO])

            processor.process(event).asserting(_ shouldBe ())
          case _ => fail("expecting payload")
        }
    }

  it should "log an error in the case of a failure" in {

    syncRepoMetadataEvents[IO]
      .map(_.generateOne)
      .flatMap { event =>
        val exception = exceptions.generateOne
        givenTSDataFinding(event.path, returning = exception.raiseError[IO, Nothing])

        val glData = dataExtracts(having = event.path).generateOne
        givenGLDataFinding(event.path, returning = glData.some.pure[IO])

        processor.process(event).asserting(_ shouldBe ()) >>
          logger.loggedOnly(Error(show"$categoryName: $event processing failure", exception)).pure[IO]
      }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val tsDataFinder         = mock[TSDataFinder[IO]]
  private lazy val glDataFinder         = mock[GLDataFinder[IO]]
  private lazy val payloadDataExtractor = mock[PayloadDataExtractor[IO]]
  private lazy val upsertsCalculator    = mock[UpsertsCalculator]
  private lazy val upsertsRunner        = mock[UpsertsRunner[IO]]
  private lazy val processor =
    new EventProcessorImpl[IO](tsDataFinder, glDataFinder, payloadDataExtractor, upsertsCalculator, upsertsRunner)

  private def givenTSDataFinding(path: projects.Path, returning: IO[Option[DataExtract]]) =
    (tsDataFinder.fetchTSData _)
      .expects(path)
      .returning(returning)

  private def givenGLDataFinding(path: projects.Path, returning: IO[Option[DataExtract]]) =
    (glDataFinder.fetchGLData _)
      .expects(path)
      .returning(returning)

  private def givenPayloadDataExtraction(payload: ZippedEventPayload, returning: IO[Option[DataExtract]]) =
    (payloadDataExtractor.extractPayloadData _)
      .expects(payload)
      .returning(returning)

  private def givenUpsertsCalculation(tsData:           DataExtract,
                                      glData:           DataExtract,
                                      maybePayloadData: Option[DataExtract],
                                      returning:        List[SparqlQuery]
  ) =
    (upsertsCalculator.calculateUpserts _)
      .expects(tsData, glData, maybePayloadData)
      .returning(returning)

  private def givenUpsertsRunning(upserts: List[SparqlQuery], returning: IO[Unit]) =
    (upsertsRunner.run _)
      .expects(upserts)
      .returning(returning)
}
