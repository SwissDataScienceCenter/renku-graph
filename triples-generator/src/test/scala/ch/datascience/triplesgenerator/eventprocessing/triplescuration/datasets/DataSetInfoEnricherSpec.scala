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
package datasets

import cats.data.EitherT
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.DataSetInfoFinder.DatasetInfo
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class DataSetInfoEnricherSpec extends WordSpec with MockFactory {

  "enrichDataSetInfo" should {

    "do nothing if there's no DataSet entity in the given JsonLD" in new TestCase {

      (infoFinder.findDatasetsInfo _).expects(curatedTriples.triples).returning(Set.empty[DatasetInfo].pure[Try])

      enricher.enrichDataSetInfo(curatedTriples) shouldBe EitherT.rightT[Try, ProcessingRecoverableError](
        curatedTriples
      )
    }

    "update JSON-LD with topmost data" in new TestCase {

      val entityId = entityIds.generateOne

      val datasetInfo = (entityId, datasetSameAs.generateOption, datasetDerivedFroms.generateOption)
      (infoFinder.findDatasetsInfo _)
        .expects(curatedTriples.triples)
        .returning(Set(datasetInfo).pure[Try])

      val topmostData = TopmostData(entityId, datasetSameAs.generateOne, datasetDerivedFroms.generateOne)
      (topmostDataFinder.findTopmostData _).expects(datasetInfo).returning(topmostData.pure[Try])

      val updatedCuratedTriples = curatedTriplesObjects.generateOne
      (triplesUpdater.mergeTopmostDataIntoTriples _)
        .expects(curatedTriples, topmostData)
        .returning(updatedCuratedTriples)

      enricher.enrichDataSetInfo(curatedTriples).value shouldBe Success(Right(updatedCuratedTriples))
    }
  }

  private trait TestCase {
    val curatedTriples = curatedTriplesObjects.generateOne

    val infoFinder        = mock[DataSetInfoFinder[Try]]
    val triplesUpdater    = mock[TriplesUpdater]
    val topmostDataFinder = mock[TopmostDataFinder[Try]]
    val enricher          = new DataSetInfoEnricher[Try](infoFinder, triplesUpdater, topmostDataFinder)
  }
}
