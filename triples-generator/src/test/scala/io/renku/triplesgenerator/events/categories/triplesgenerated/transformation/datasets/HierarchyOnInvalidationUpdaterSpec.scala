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

import Generators.queriesGen
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class HierarchyOnInvalidationUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "updateHierarchyOnInvalidation" should {

    "prepare updates for deleted datasets" in new TestCase {
      val (internal ::~ _ ::~ importedExternal ::~ _ ::~ ancestorInternal ::~ _ ::~ ancestorExternal ::~ _,
           projectWithAllDatasets
      ) = anyRenkuProjectEntities
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedExternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorInternal()))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorExternal))
        .generateOne
      val (beforeModification, modification, modificationInvalidated) =
        datasetAndModificationEntities(provenanceInternal,
                                       projectDateCreated = projectWithAllDatasets.dateCreated
        ).map { case internal ::~ modified => (internal, modified, modified.invalidateNow) }.generateOne

      val entitiesProjectWithAllDatasets = projectWithAllDatasets
        .addDatasets(beforeModification, modification, modificationInvalidated)
        .to[entities.Project]

      val internalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.Internal])(
          _: entities.Dataset.Provenance.Internal.type
        ))
        .expects(internal.to[entities.Dataset[entities.Dataset.Provenance.Internal]], *)
        .returning(internalDsQueries)

      val importedExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.ImportedExternal])(
          _: entities.Dataset.Provenance.ImportedExternal.type
        ))
        .expects(importedExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]], *)
        .returning(importedExternalDsQueries)

      val ancestorInternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal])(
          _: entities.Dataset.Provenance.ImportedInternal.type
        ))
        .expects(ancestorInternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]], *)
        .returning(ancestorInternalDsQueries)

      val ancestorExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal])(
          _: entities.Dataset.Provenance.ImportedInternal.type
        ))
        .expects(ancestorExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]], *)
        .returning(ancestorExternalDsQueries)

      val Success(updatedProject -> queries) =
        updater.updateHierarchyOnInvalidation(entitiesProjectWithAllDatasets -> initialQueries)

      updatedProject               shouldBe entitiesProjectWithAllDatasets
      queries.preDataUploadQueries shouldBe initialQueries.preDataUploadQueries
      queries.postDataUploadQueries  should contain theSameElementsAs initialQueries.postDataUploadQueries :::
        internalDsQueries ::: importedExternalDsQueries ::: ancestorInternalDsQueries ::: ancestorExternalDsQueries
    }
  }

  private trait TestCase {
    val initialQueries = queriesGen.generateOne
    val updatesCreator = mock[UpdatesCreator]
    val updater        = new HierarchyOnInvalidationUpdaterImpl[Try](updatesCreator, ProjectFunctions)
  }
}
