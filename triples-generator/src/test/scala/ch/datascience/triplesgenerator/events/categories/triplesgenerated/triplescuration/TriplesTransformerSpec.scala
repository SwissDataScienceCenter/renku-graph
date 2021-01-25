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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration

import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedEvent
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.DataSetInfoEnricher
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.ProjectInfoUpdater
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsUpdater
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.ProjectInfoUpdater
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TriplesTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "curate" should {

    "pass the given triples through all the curation steps and return the final results" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects[Try].generateOne
      (personDetailsUpdater.updatePersonDetails _)
        .expects(CuratedTriples[Try](triplesGeneratedEvent.triples, updatesGroups = Nil),
                 triplesGeneratedEvent.project.path
        )
        .returning(triplesWithPersonDetails.toRightT)

      val triplesWithForkInfo = curatedTriplesObjects[Try].generateOne
      (projectInfoUpdater
        .updateProjectInfo(_: TriplesGeneratedEvent, _: CuratedTriples[Try])(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, triplesWithPersonDetails, maybeAccessToken)
        .returning(triplesWithForkInfo.toRightT)

      val triplesWithEnrichedDataset = curatedTriplesObjects[Try].generateOne
      (dataSetInfoEnricher.enrichDataSetInfo _)
        .expects(triplesWithForkInfo)
        .returning(triplesWithEnrichedDataset.toRightT)

      curator.transform(triplesGeneratedEvent).value shouldBe Right(triplesWithEnrichedDataset).pure[Try]
    }

    "fail with the failure from the person details update" in new TestCase {

      val exception = exceptions.generateOne
      (personDetailsUpdater.updatePersonDetails _)
        .expects(CuratedTriples[Try](triplesGeneratedEvent.triples, updatesGroups = Nil),
                 triplesGeneratedEvent.project.path
        )
        .returning(exception.toEitherTError)

      curator.transform(triplesGeneratedEvent).value shouldBe exception.raiseError[Try, CuratedTriples[Try]]
    }

    "fail with the failure from the fork info update" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects[Try].generateOne
      (personDetailsUpdater.updatePersonDetails _)
        .expects(CuratedTriples[Try](triplesGeneratedEvent.triples, updatesGroups = Nil),
                 triplesGeneratedEvent.project.path
        )
        .returning(triplesWithPersonDetails.toRightT)

      val exception = exceptions.generateOne
      (projectInfoUpdater
        .updateProjectInfo(_: TriplesGeneratedEvent, _: CuratedTriples[Try])(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, triplesWithPersonDetails, maybeAccessToken)
        .returning(exception.toEitherTError)

      curator.transform(triplesGeneratedEvent).value shouldBe exception.raiseError[Try, CuratedTriples[Try]]
    }

    "fail with the failure from the dataset enricher update" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects[Try].generateOne
      (personDetailsUpdater.updatePersonDetails _)
        .expects(CuratedTriples[Try](triplesGeneratedEvent.triples, updatesGroups = Nil),
                 triplesGeneratedEvent.project.path
        )
        .returning(triplesWithPersonDetails.toRightT)

      val triplesWithForkInfo = curatedTriplesObjects[Try].generateOne
      (projectInfoUpdater
        .updateProjectInfo(_: TriplesGeneratedEvent, _: CuratedTriples[Try])(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, triplesWithPersonDetails, maybeAccessToken)
        .returning(triplesWithForkInfo.toRightT)

      val exception = exceptions.generateOne
      (dataSetInfoEnricher.enrichDataSetInfo _)
        .expects(triplesWithForkInfo)
        .returning(exception.toEitherTError)

      curator.transform(triplesGeneratedEvent).value shouldBe exception.raiseError[Try, CuratedTriples[Try]]
    }

    s"return $CurationRecoverableError if personDetailsUpdater returns one" in new TestCase {

      val exception = CurationRecoverableError(nonBlankStrings().generateOne.value, exceptions.generateOne)
      (personDetailsUpdater.updatePersonDetails _)
        .expects(CuratedTriples[Try](triplesGeneratedEvent.triples, updatesGroups = Nil),
                 triplesGeneratedEvent.project.path
        )
        .returning(exception.toLeftT)

      curator.transform(triplesGeneratedEvent).value shouldBe Left(exception).pure[Try]
    }

    s"return $CurationRecoverableError if forkInfoUpdater returns one" in new TestCase {

      val triplesWithPersonDetails = curatedTriplesObjects[Try].generateOne
      (personDetailsUpdater.updatePersonDetails _)
        .expects(CuratedTriples[Try](triplesGeneratedEvent.triples, updatesGroups = Nil),
                 triplesGeneratedEvent.project.path
        )
        .returning(triplesWithPersonDetails.toRightT)

      val exception = CurationRecoverableError(nonBlankStrings().generateOne.value, exceptions.generateOne)
      (projectInfoUpdater
        .updateProjectInfo(_: TriplesGeneratedEvent, _: CuratedTriples[Try])(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, triplesWithPersonDetails, maybeAccessToken)
        .returning(exception.toLeftT)

      curator.transform(triplesGeneratedEvent).value shouldBe Left(exception).pure[Try]
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val triplesGeneratedEvent = triplesGeneratedEvents.generateOne

    val personDetailsUpdater = mock[PersonDetailsUpdater[Try]]
    val projectInfoUpdater   = mock[ProjectInfoUpdater[Try]]
    val dataSetInfoEnricher  = mock[DataSetInfoEnricher[Try]]
    val curator              = new TriplesTransformerImpl[Try](personDetailsUpdater, projectInfoUpdater, dataSetInfoEnricher)
  }

  private implicit class TriplesOps(out: CuratedTriples[Try]) {
    lazy val toRightT: EitherT[Try, ProcessingRecoverableError, CuratedTriples[Try]] =
      EitherT.rightT[Try, ProcessingRecoverableError](out)
  }

  private implicit class ExceptionOps(exception: Exception) {
    lazy val toEitherTError: EitherT[Try, ProcessingRecoverableError, CuratedTriples[Try]] =
      EitherT[Try, ProcessingRecoverableError, CuratedTriples[Try]](
        exception.raiseError[Try, Either[ProcessingRecoverableError, CuratedTriples[Try]]]
      )
  }

  private implicit class RecoverableErrorOps(exception: ProcessingRecoverableError) {
    lazy val toLeftT: EitherT[Try, ProcessingRecoverableError, CuratedTriples[Try]] =
      EitherT.leftT[Try, CuratedTriples[Try]](exception)
  }

}
