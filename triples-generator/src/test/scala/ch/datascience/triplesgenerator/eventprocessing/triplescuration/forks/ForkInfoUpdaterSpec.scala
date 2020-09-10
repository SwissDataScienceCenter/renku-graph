/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, jsonLDTriples}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators.{commitEvents, curationRecoverableErrors}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ForkInfoUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import EitherT._

  "updateForkInfo" should {
    "do the payload transformation and prepare update functions" in new TestCase {
      val transformedTriples = jsonLDTriples.generateOne
      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](transformedTriples))

      val updateFunctions = listOf(curationUpdateFunctions[IO]).generateOne
      (updateFunctionsCreator
        .create(_: CommitEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](updateFunctions))

      forkInfoUpdater
        .updateForkInfo(event, givenCuratedTriples)
        .value
        .unsafeRunSync() shouldBe Right(
        CuratedTriples[IO](triples = transformedTriples, updates = givenCuratedTriples.updates ++ updateFunctions)
      )
    }

    "return a ProcessingRecoverableError if the triple transformation fails" in new TestCase {
      val error = curationRecoverableErrors.generateOne
      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(leftT[IO, JsonLDTriples](error))

      val updateFunctions = listOf(curationUpdateFunctions[IO]).generateOne
      (updateFunctionsCreator
        .create(_: CommitEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](updateFunctions))

      forkInfoUpdater.updateForkInfo(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Left(error)
    }

    "return a ProcessingRecoverableError if the update function creator fails" in new TestCase {

      val transformedTriples = jsonLDTriples.generateOne
      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](transformedTriples))

      val error = curationRecoverableErrors.generateOne
      (updateFunctionsCreator
        .create(_: CommitEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(leftT[IO, List[UpdateFunction[IO]]](error))

      forkInfoUpdater.updateForkInfo(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Left(error)
    }

    "fail if an error is raised when transforming the triples" in new TestCase {

      val exception = exceptions.generateOne

      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(EitherT(exception.raiseError[IO, Either[ProcessingRecoverableError, JsonLDTriples]]))

      val updateFunctions = listOf(curationUpdateFunctions[IO]).generateOne
      (updateFunctionsCreator
        .create(_: CommitEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](updateFunctions))

      intercept[Exception] {
        forkInfoUpdater.updateForkInfo(event, givenCuratedTriples).value.unsafeRunSync()
      } shouldBe exception
    }

    "fail if an error is raised when creating the updates" in new TestCase {

      val transformedTriples = jsonLDTriples.generateOne
      (payloadTransformer
        .transform(_: CommitEvent, _: JsonLDTriples)(_: Option[AccessToken]))
        .expects(event, givenCuratedTriples.triples, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](transformedTriples))

      val exception = exceptions.generateOne
      (updateFunctionsCreator
        .create(_: CommitEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(EitherT(exception.raiseError[IO, Either[ProcessingRecoverableError, List[UpdateFunction[IO]]]]))

      intercept[Exception] {
        forkInfoUpdater.updateForkInfo(event, givenCuratedTriples).value.unsafeRunSync()
      } shouldBe exception
    }

  }
  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val event               = commitEvents.generateOne
    val givenCuratedTriples = curatedTriplesObjects[IO].generateOne

    val payloadTransformer     = mock[PayloadTransformer[IO]]
    val updateFunctionsCreator = mock[UpdateFunctionsCreator[IO]]
    val forkInfoUpdater        = new ForkInfoUpdaterImpl(payloadTransformer, updateFunctionsCreator)
  }
}
