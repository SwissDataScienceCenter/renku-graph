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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.data.EitherT
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.DataSetInfoEnricher
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks.ForkInfoUpdater
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.{PersonDetailsUpdater, UpdatesCreator}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class TriplesCuratorSpec extends WordSpec with MockFactory {

  "curate" should {

    "pass the given triples through all the curation steps and return the final results" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects.generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(triplesWithPersonDetails.pure[Try])

      val triplesWithForkInfo = curatedTriplesObjects.generateOne
      (forkInfoUpdater
        .updateForkInfo(_: CommitEvent, _: CuratedTriples)(_: Option[AccessToken]))
        .expects(event, triplesWithPersonDetails, maybeAccessToken)
        .returning(triplesWithForkInfo.toRightT)

      val triplesWithEnrichedDataset = curatedTriplesObjects.generateOne
      (dataSetInfoEnricher.enrichDataSetInfo _)
        .expects(triplesWithForkInfo)
        .returning(triplesWithEnrichedDataset.toRightT)

      curator.curate(event, triples).value shouldBe Right(triplesWithEnrichedDataset).pure[Try]
    }

    "fail with the failure from the person details update" in new TestCase {

      val exception = exceptions.generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(exception.raiseError[Try, CuratedTriples])

      curator.curate(event, triples).value shouldBe exception.raiseError[Try, CuratedTriples]
    }

    "fail with the failure from the fork info update" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects.generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(triplesWithPersonDetails.pure[Try])

      val exception = exceptions.generateOne
      (forkInfoUpdater
        .updateForkInfo(_: CommitEvent, _: CuratedTriples)(_: Option[AccessToken]))
        .expects(event, triplesWithPersonDetails, maybeAccessToken)
        .returning(exception.toEitherTError)

      curator.curate(event, triples).value shouldBe exception.raiseError[Try, CuratedTriples]
    }

    "fail with the failure from the dataset enricher update" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects.generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(triplesWithPersonDetails.pure[Try])

      val triplesWithForkInfo = curatedTriplesObjects.generateOne
      (forkInfoUpdater
        .updateForkInfo(_: CommitEvent, _: CuratedTriples)(_: Option[AccessToken]))
        .expects(event, triplesWithPersonDetails, maybeAccessToken)
        .returning(triplesWithForkInfo.toRightT)

      val exception = exceptions.generateOne
      (dataSetInfoEnricher.enrichDataSetInfo _)
        .expects(triplesWithForkInfo)
        .returning(exception.toEitherTError)

      curator.curate(event, triples).value shouldBe exception.raiseError[Try, CuratedTriples]
    }

    s"return $CurationRecoverableError if forkInfoUpdater returns one" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects.generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(triplesWithPersonDetails.pure[Try])

      val exception = CurationRecoverableError(nonBlankStrings().generateOne.value, exceptions.generateOne)
      (forkInfoUpdater
        .updateForkInfo(_: CommitEvent, _: CuratedTriples)(_: Option[AccessToken]))
        .expects(event, triplesWithPersonDetails, maybeAccessToken)
        .returning(exception.toLeftT)

      curator.curate(event, triples).value shouldBe Left(exception).pure[Try]
    }
  }

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val triples = jsonLDTriples.generateOne
    val event   = commitEvents.generateOne

    class TryPersonDetailsUpdater(updatesCreator: UpdatesCreator) extends PersonDetailsUpdater[Try](updatesCreator)
    val personDetailsUpdater = mock[TryPersonDetailsUpdater]
    val forkInfoUpdater      = mock[ForkInfoUpdater[Try]]
    val dataSetInfoEnricher  = mock[DataSetInfoEnricher[Try]]
    val curator              = new TriplesCurator[Try](personDetailsUpdater, forkInfoUpdater, dataSetInfoEnricher)
  }

  private implicit class TriplesOps(out: CuratedTriples) {
    lazy val toRightT: EitherT[Try, ProcessingRecoverableError, CuratedTriples] =
      EitherT.rightT[Try, ProcessingRecoverableError](out)
  }

  private implicit class ExceptionOps(exception: Exception) {
    lazy val toEitherTError: EitherT[Try, ProcessingRecoverableError, CuratedTriples] =
      EitherT[Try, ProcessingRecoverableError, CuratedTriples](
        exception.raiseError[Try, Either[ProcessingRecoverableError, CuratedTriples]]
      )
  }

  private implicit class RecoverableErrorOps(exception: ProcessingRecoverableError) {
    lazy val toLeftT: EitherT[Try, ProcessingRecoverableError, CuratedTriples] =
      EitherT.leftT[Try, CuratedTriples](exception)
  }
}
