/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.tsprovisioning.transformation
package namedgraphs.datasets

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.triplesgenerator.tsprovisioning.Generators._
import io.renku.triplesgenerator.tsprovisioning.ProjectFunctions
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class HierarchyOnInvalidationUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "updateHierarchyOnInvalidation" should {

    "prepare updates for deleted datasets if the invalidated DS does exist only on the project" in new TestCase {
      val (internal ::~ _ ::~ importedExternal ::~ _ ::~ ancestorInternal ::~ _ ::~ ancestorExternal ::~ _, project) =
        anyRenkuProjectEntities
          .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
          .addDatasetAndInvalidation(datasetEntities(provenanceImportedExternal))
          .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorInternal()))
          .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorExternal))
          .generateOne

      val (beforeModification, modification, modificationInvalidated) =
        datasetAndModificationEntities(provenanceInternal, projectDateCreated = project.dateCreated).map {
          case internal ::~ modified => (internal, modified, modified.invalidateNow(personEntities))
        }.generateOne

      val entitiesProject = project
        .addDatasets(beforeModification, modification, modificationInvalidated)
        .to[entities.Project]

      givenDS(modification.to[entities.Dataset[entities.Dataset.Provenance.Modified]],
              notInvalidatedOn = Set(entitiesProject.resourceId)
      )

      val entitiesInternal = internal.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
      givenDS(entitiesInternal, notInvalidatedOn = Set(entitiesProject.resourceId))
      val internalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.Internal])(
          _: entities.Dataset.Provenance.Internal.type
        ))
        .expects(entitiesInternal, *)
        .returning(internalDsQueries)

      val entitiesImportedExternal = importedExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
      givenDS(entitiesImportedExternal, notInvalidatedOn = Set(entitiesProject.resourceId))
      val importedExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: projects.ResourceId,
                                       _: entities.Dataset[entities.Dataset.Provenance.ImportedExternal]
        )(_: entities.Dataset.Provenance.ImportedExternal.type))
        .expects(entitiesProject.resourceId, entitiesImportedExternal, *)
        .returning(importedExternalDsQueries)

      val entitiesAncestorInternal =
        ancestorInternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
      givenDS(entitiesAncestorInternal, notInvalidatedOn = Set(entitiesProject.resourceId))
      val ancestorInternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: projects.ResourceId,
                                       _: entities.Dataset[entities.Dataset.Provenance.ImportedInternal]
        )(_: entities.Dataset.Provenance.ImportedInternal.type))
        .expects(entitiesProject.resourceId, entitiesAncestorInternal, *)
        .returning(ancestorInternalDsQueries)

      val entitiesAncestorExternal =
        ancestorExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
      givenDS(entitiesAncestorExternal, notInvalidatedOn = Set(entitiesProject.resourceId))
      val ancestorExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: projects.ResourceId,
                                       _: entities.Dataset[entities.Dataset.Provenance.ImportedInternal]
        )(_: entities.Dataset.Provenance.ImportedInternal.type))
        .expects(entitiesProject.resourceId, entitiesAncestorExternal, *)
        .returning(ancestorExternalDsQueries)

      val Success(updatedProject -> queries) =
        updater.updateHierarchyOnInvalidation(entitiesProject -> initialQueries)

      updatedProject               shouldBe entitiesProject
      queries.preDataUploadQueries shouldBe initialQueries.preDataUploadQueries
      queries.postDataUploadQueries  should contain theSameElementsAs initialQueries.postDataUploadQueries :::
        internalDsQueries ::: importedExternalDsQueries ::: ancestorInternalDsQueries ::: ancestorExternalDsQueries
    }

    "do nothing if there are more projects where the datasets exist (perhaps there's a fork)" in new TestCase {
      val (internal ::~ _ ::~ importedExternal ::~ _ ::~ ancestorInternal ::~ _ ::~ ancestorExternal ::~ _, project) =
        anyRenkuProjectEntities
          .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
          .addDatasetAndInvalidation(datasetEntities(provenanceImportedExternal))
          .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorInternal()))
          .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorExternal))
          .generateOne

      val (beforeModification, modification, modificationInvalidated) =
        datasetAndModificationEntities(provenanceInternal, projectDateCreated = project.dateCreated).map {
          case internal ::~ modified => (internal, modified, modified.invalidateNow(personEntities))
        }.generateOne

      val entitiesProject = project
        .addDatasets(beforeModification, modification, modificationInvalidated)
        .to[entities.Project]

      val forkProjectIds = projectResourceIds.generateNonEmptyList().toList.toSet
      givenDS(modification.to[entities.Dataset[entities.Dataset.Provenance.Modified]],
              notInvalidatedOn = forkProjectIds
      )
      givenDS(internal.to[entities.Dataset[entities.Dataset.Provenance.Internal]], notInvalidatedOn = forkProjectIds)
      givenDS(importedExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]],
              notInvalidatedOn = forkProjectIds
      )
      givenDS(ancestorInternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]],
              notInvalidatedOn = forkProjectIds
      )
      givenDS(ancestorExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]],
              notInvalidatedOn = forkProjectIds
      )

      val Success(updatedProject -> queries) =
        updater.updateHierarchyOnInvalidation(entitiesProject -> initialQueries)

      updatedProject shouldBe entitiesProject
      queries        shouldBe initialQueries
    }
  }

  private trait TestCase {
    val initialQueries      = queriesGen.generateOne
    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val updater = new HierarchyOnInvalidationUpdaterImpl[Try](kgDatasetInfoFinder, updatesCreator, ProjectFunctions)

    def givenDS(ds: entities.Dataset[entities.Dataset.Provenance], notInvalidatedOn: Set[projects.ResourceId]) =
      (kgDatasetInfoFinder.findWhereNotInvalidated _)
        .expects(ds.resourceId)
        .returning(notInvalidatedOn.pure[Try])

  }
}
