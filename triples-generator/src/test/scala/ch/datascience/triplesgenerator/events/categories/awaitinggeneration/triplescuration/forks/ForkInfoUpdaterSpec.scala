/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.forks

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, jsonLDTriples}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.EventProcessingGenerators.{commitEvents, curationRecoverableErrors}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEvent
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.CurationGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ForkInfoUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import EitherT._

  "updateForkInfo" should {
    "do the payload transformation and prepare update group" in new TestCase {
      val transformedTriples = jsonLDTriples.generateOne
      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](transformedTriples))

      val updateGroup = curationUpdatesGroups[IO].generateOne
      (updatesCreator
        .create(_: CommitEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(updateGroup)

      forkInfoUpdater
        .updateForkInfo(event, givenCuratedTriples)
        .value
        .unsafeRunSync() shouldBe Right(
        CuratedTriples[IO](triples = transformedTriples,
                           updatesGroups = givenCuratedTriples.updatesGroups :+ updateGroup
        )
      )
    }

    "return a ProcessingRecoverableError if the triple transformation fails" in new TestCase {
      val error = curationRecoverableErrors.generateOne
      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(leftT[IO, JsonLDTriples](error))

      forkInfoUpdater.updateForkInfo(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Left(error)
    }

    "fail if an error is raised when transforming the triples" in new TestCase {

      val exception = exceptions.generateOne

      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(EitherT(exception.raiseError[IO, Either[ProcessingRecoverableError, JsonLDTriples]]))

      intercept[Exception] {
        forkInfoUpdater.updateForkInfo(event, givenCuratedTriples).value.unsafeRunSync()
      } shouldBe exception
    }
  }
  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val event               = commitEvents.generateOne
    val givenCuratedTriples = curatedTriplesObjects[IO].generateOne

    val payloadTransformer = mock[PayloadTransformer[IO]]
    val updatesCreator     = mock[UpdatesCreator[IO]]
    val forkInfoUpdater    = new ForkInfoUpdaterImpl(payloadTransformer, updatesCreator)
  }
}
