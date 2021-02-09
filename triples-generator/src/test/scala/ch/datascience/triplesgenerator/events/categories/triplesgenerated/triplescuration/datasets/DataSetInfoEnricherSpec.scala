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
package datasets

import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.client.RestClientError.{ConnectivityException, UnexpectedResponseException}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.DataSetInfoFinder.DatasetInfo
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class DataSetInfoEnricherSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "enrichDataSetInfo" should {

    "do nothing if there's no DataSet entity in the given JsonLD" in new TestCase {

      (infoFinder.findDatasetsInfo _).expects(curatedTriples.triples).returning(Set.empty[DatasetInfo].pure[Try])

      enricher.enrichDataSetInfo(curatedTriples) shouldBe EitherT.rightT[Try, ProcessingRecoverableError](
        curatedTriples
      )
    }

    "update JSON-LD with topmost data and prepare updates for descendant datasets" in new TestCase {

      val datasetInfoList = datasetInfos.generateNonEmptyList()

      (infoFinder.findDatasetsInfo _)
        .expects(curatedTriples.triples)
        .returning(datasetInfoList.toList.toSet.pure[Try])

      val topmostDatas = datasetInfoList map { datasetInfo =>
        val topmostData =
          TopmostData(datasetInfo._1, datasetTopmostSameAs.generateOne, datasetTopmostDerivedFroms.generateOne)
        (topmostDataFinder.findTopmostData _).expects(datasetInfo).returning(topmostData.pure[Try])
        topmostData
      }

      val updatedCuratedTriples = topmostDatas.foldLeft(curatedTriples) { (triples, _) =>
        val updatedTriples = curatedTriplesObjects[Try].generateOne
        (triplesUpdater.mergeTopmostDataIntoTriples[Try] _)
          .expects(triples, *)
          .returning(updatedTriples)
        updatedTriples
      }

      val curatedTriplesWithUpdates = topmostDatas.foldLeft(updatedCuratedTriples) { (triples, _) =>
        val triplesWithUpdates = curatedTriplesObjects[Try].generateOne
        (descendantsUpdater
          .prepareUpdates[Try](
            _: CuratedTriples[Try],
            _: TopmostData
          )(_: MonadError[Try, Throwable]))
          .expects(triples, *, *)
          .returning(triplesWithUpdates)
        triplesWithUpdates
      }

      enricher.enrichDataSetInfo(curatedTriples).value shouldBe Success(Right(curatedTriplesWithUpdates))
    }

    "return recoverable error when finding topmost data fails with connectivity issue" in new TestCase {
      val datasetInfoList = datasetInfos.generateNonEmptyList()

      (infoFinder.findDatasetsInfo _)
        .expects(curatedTriples.triples)
        .returning(datasetInfoList.toList.toSet.pure[Try])

      val exception = ConnectivityException("Connectivity exception", exceptions.generateOne)
      datasetInfoList.toList.foreach { datasetInfo =>
        (topmostDataFinder.findTopmostData _).expects(datasetInfo).returning(exception.raiseError[Try, TopmostData])
      }

      enricher.enrichDataSetInfo(curatedTriples).value shouldBe Success(
        Left(CurationRecoverableError("Problem with finding top most data", exception))
      )
    }

    "return recoverable error when finding topmost data fails with unexpected response" in new TestCase {
      val datasetInfoList = datasetInfos.generateNonEmptyList()

      (infoFinder.findDatasetsInfo _)
        .expects(curatedTriples.triples)
        .returning(datasetInfoList.toList.toSet.pure[Try])

      val exception = UnexpectedResponseException("Unexpected response exception")
      datasetInfoList.toList.foreach { datasetInfo =>
        (topmostDataFinder.findTopmostData _).expects(datasetInfo).returning(exception.raiseError[Try, TopmostData])
      }

      enricher.enrichDataSetInfo(curatedTriples).value shouldBe Success(
        Left(CurationRecoverableError("Problem with finding top most data", exception))
      )
    }

    "fail when finding topmost data fails with other exception" in new TestCase {

      val datasetInfoList = datasetInfos.generateNonEmptyList()

      (infoFinder.findDatasetsInfo _)
        .expects(curatedTriples.triples)
        .returning(datasetInfoList.toList.toSet.pure[Try])

      val exception = exceptions.generateOne
      datasetInfoList.toList.foreach { datasetInfo =>
        (topmostDataFinder.findTopmostData _).expects(datasetInfo).returning(exception.raiseError[Try, TopmostData])
      }

      enricher.enrichDataSetInfo(curatedTriples).value shouldBe Failure(exception)
    }

    "fail when preparing updates fails" in new TestCase {

      val datasetInfoList = datasetInfos.generateNonEmptyList()

      (infoFinder.findDatasetsInfo _)
        .expects(curatedTriples.triples)
        .returning(datasetInfoList.toList.toSet.pure[Try])

      val topmostDatas = datasetInfoList map { datasetInfo =>
        val topmostData =
          TopmostData(datasetInfo._1, datasetTopmostSameAs.generateOne, datasetTopmostDerivedFroms.generateOne)
        (topmostDataFinder.findTopmostData _).expects(datasetInfo).returning(topmostData.pure[Try])
        topmostData
      }

      val updatedCuratedTriples = topmostDatas.foldLeft(curatedTriples) { (triples, _) =>
        val updatedTriples = curatedTriplesObjects[Try].generateOne
        (triplesUpdater.mergeTopmostDataIntoTriples[Try] _)
          .expects(triples, *)
          .returning(updatedTriples)
        updatedTriples
      }

      val exception = exceptions.generateOne

      (descendantsUpdater
        .prepareUpdates[Try](
          _: CuratedTriples[Try],
          _: TopmostData
        )(_: MonadError[Try, Throwable]))
        .expects(updatedCuratedTriples, *, *)
        .throwing(exception)

      enricher.enrichDataSetInfo(curatedTriples).value shouldBe Failure(exception)
    }
  }

  private trait TestCase {
    val curatedTriples = curatedTriplesObjects[Try].generateOne

    val infoFinder = mock[DataSetInfoFinder[Try]]
    val triplesUpdater = mock[TriplesUpdater]
    val topmostDataFinder = mock[TopmostDataFinder[Try]]
    val descendantsUpdater = mock[DescendantsUpdater]
    val enricher = new DataSetInfoEnricherImpl[Try](infoFinder, triplesUpdater, topmostDataFinder, descendantsUpdater)
  }

  private lazy val datasetInfos = for {
    datasetId <- entityIds
    maybeSameAs <- datasetSameAs.toGeneratorOfOptions
    maybeDerivedFrom <- datasetDerivedFroms.toGeneratorOfOptions
  } yield (datasetId, maybeSameAs, maybeDerivedFrom)
}
