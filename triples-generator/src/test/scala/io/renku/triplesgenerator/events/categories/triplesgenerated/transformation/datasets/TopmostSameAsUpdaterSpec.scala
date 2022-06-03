/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation
package datasets

import Generators._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.datasetTopmostSameAs
import io.renku.graph.model.datasets.{InternalSameAs, ResourceId, TopmostSameAs}
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.rdfstore.SparqlQuery
import io.renku.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class TopmostSameAsUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "updateTopmostSameAs" should {

    "find all internally imported datasets, " +
      "find the topmostSameAs values for their sameAs and resourceId, " +
      "update the datasets with the topmostSameAs found for sameAs in KG " +
      "and create update queries" in new TestCase {

        val (dataset1 ::~ dataset2, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternal))
          .addDataset(datasetEntities(provenanceImportedInternal))
          .generateOne
          .bimap(
            _.bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]],
                    _.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
            ),
            _.to[entities.Project]
          )

        val parentTopmostSameAs = datasetTopmostSameAs.generateOne
        findingParentTopmostSameAsFor(dataset1.provenance.sameAs, returning = parentTopmostSameAs.some.pure[Try])
        findingTopmostSameAsFor(dataset1.identification.resourceId, returning = Set.empty[TopmostSameAs].pure[Try])
        val dataset1Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset1 -> Set.empty, returning = dataset1Queries)
        val updatedDataset1 = dataset1.update(parentTopmostSameAs)

        val dataset1CleanUpQueries = sparqlQueries.generateList()
        prepareQueriesForTopmostSameAsCleanup(updatedDataset1 -> parentTopmostSameAs.some,
                                              returning = dataset1CleanUpQueries
        )

        findingParentTopmostSameAsFor(dataset2.provenance.sameAs, returning = None.pure[Try])
        val dataset2KgTopmostSameAses = datasetTopmostSameAs.generateSet(minElements = 1)
        findingTopmostSameAsFor(dataset2.identification.resourceId, returning = dataset2KgTopmostSameAses.pure[Try])
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset2 -> dataset2KgTopmostSameAses, returning = dataset2Queries)

        val dataset2CleanUpQueries = sparqlQueries.generateList()
        prepareQueriesForTopmostSameAsCleanup(dataset2 -> None, returning = dataset2CleanUpQueries)

        val Success(updatedProject -> queries) = updater.updateTopmostSameAs(project -> initialQueries)

        updatedProject               shouldBe ProjectFunctions.update(dataset1, updatedDataset1)(project)
        queries.preDataUploadQueries shouldBe initialQueries.preDataUploadQueries
        queries.postDataUploadQueries shouldBe initialQueries.postDataUploadQueries :::
          dataset1Queries ::: dataset1CleanUpQueries ::: dataset2Queries ::: dataset2CleanUpQueries
      }
  }

  private trait TestCase {
    val initialQueries      = queriesGen.generateOne
    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val updater             = new TopmostSameAsUpdaterImpl[Try](kgDatasetInfoFinder, updatesCreator, ProjectFunctions)

    def findingParentTopmostSameAsFor(sameAs: InternalSameAs, returning: Try[Option[TopmostSameAs]]) =
      (kgDatasetInfoFinder
        .findParentTopmostSameAs(_: InternalSameAs)(_: InternalSameAs.type))
        .expects(sameAs, *)
        .returning(returning)

    def findingTopmostSameAsFor(resourceId: ResourceId, returning: Try[Set[TopmostSameAs]])(implicit
        ev:                                 TopmostSameAs.type
    ) = (kgDatasetInfoFinder
      .findTopmostSameAs(_: ResourceId)(_: ResourceId.type))
      .expects(resourceId, *)
      .returning(returning)

    def prepareQueriesForSameAs(
        dsAndMaybeTopmostSameAses: (entities.Dataset[entities.Dataset.Provenance.ImportedInternal], Set[TopmostSameAs]),
        returning:                 List[SparqlQuery]
    ) = (updatesCreator
      .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal], _: Set[TopmostSameAs]))
      .expects(dsAndMaybeTopmostSameAses._1, dsAndMaybeTopmostSameAses._2)
      .returning(returning)

    def prepareQueriesForTopmostSameAsCleanup(
        dsAndMaybeTopmostSameAs: (entities.Dataset[entities.Dataset.Provenance.ImportedInternal],
                                  Option[TopmostSameAs]
        ),
        returning: List[SparqlQuery]
    ) = (updatesCreator
      .prepareTopmostSameAsCleanup(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal],
                                   _: Option[TopmostSameAs]
      ))
      .expects(dsAndMaybeTopmostSameAs._1, dsAndMaybeTopmostSameAs._2)
      .returning(returning)
  }
}
