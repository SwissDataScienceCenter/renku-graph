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

package io.renku.knowledgegraph.entities
package finder

import Endpoint._
import Criteria.{Filters, Sorting}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.http.rest.SortBy
import io.renku.triplesstore.{InMemoryJenaForSpec, RenkuDataset}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DatasetsEntitiesFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with FinderSpecOps
    with InMemoryJenaForSpec
    with RenkuDataset
    with IOSpec {

  "findEntities - in case of a shared datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in new TestCase {
      val originalDSAndProject @ originalDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val _ ::~ importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      upload(to = renkuDataset, originalDSProject, importedDSProject)

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
        .unsafeRunSync()
        .results shouldBe List(originalDSAndProject.to[model.Entity.Dataset]).sortBy(_.name.value)
    }

    "de-duplicate datasets having equal sameAs - case of an Exported DS" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val importedDSAndProject1 @ importedDS1 ::~ project1WithImportedDS = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val importedDSAndProject2 @ _ ::~ project2WithImportedDS = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val importedDSAndProject3 @ _ ::~ projectWithDSImportedFromProject = renkuProjectEntities(visibilityPublic)
        .importDataset(importedDS1)
        .generateOne

      upload(to = renkuDataset, project1WithImportedDS, project2WithImportedDS, projectWithDSImportedFromProject)

      val results = finder
        .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
        .unsafeRunSync()
        .results

      results should {
        be(List(importedDSAndProject1.to[model.Entity.Dataset])) or
          be(List(importedDSAndProject2.to[model.Entity.Dataset])) or
          be(List(importedDSAndProject3.to[model.Entity.Dataset]))
      }
    }
  }

  "findEntities - in case of a modified datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in new TestCase {
      val originalDS ::~ modifiedDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ ::~ importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      upload(to = renkuDataset, originalDSProject, importedDSProject)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                   sorting = Sorting.By(Sorting.ByDate, SortBy.Direction.Asc)
          )
        )
        .unsafeRunSync()
        .results shouldBe List(
        (modifiedDS -> originalDSProject).to[model.Entity.Dataset],
        importedDSAndProject.to[model.Entity.Dataset]
      ).sortBy(_.dateAsInstant)
    }
  }

  "findEntities - in case of a invalidated datasets" should {

    "not return invalidated DS" in new TestCase {
      val originalDS ::~ _ ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ ::~ importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      upload(to = renkuDataset, originalDSProject, importedDSProject)

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
        .unsafeRunSync()
        .results shouldBe List(importedDSAndProject.to[model.Entity.Dataset]).sortBy(_.name.value)
    }
  }

  "findEntities - in case of a forks with datasets" should {

    "de-duplicate datasets when on forked projects" in new TestCase {
      val _ ::~ modifiedDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val original ::~ fork = originalDSProject.forkOnce()

      upload(to = renkuDataset, original, fork)

      val results = finder
        .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
        .unsafeRunSync()
        .results

      results should {
        be(List((modifiedDS -> original).to[model.Entity.Dataset])) or
          be(List((modifiedDS -> fork).to[model.Entity.Dataset]))
      }
    }
  }

  "findEntities - in case of a dataset on forks with different visibility" should {

    "favour dataset on public projects if exist" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val dsAndPublicProject @ _ ::~ publicProject = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val original ::~ fork = {
        val original ::~ fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }

      upload(to = renkuDataset, original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)), maybeUser = member.toAuthUser.some)
        )
        .unsafeRunSync()
        .results shouldBe List(dsAndPublicProject.to[model.Entity.Dataset])
    }

    "favour dataset on internal projects over private projects if exist" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndInternalProject @ _ ::~ internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      val original ::~ fork = {
        val original ::~ fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }

      upload(to = renkuDataset, original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)), maybeUser = member.toAuthUser.some)
        )
        .unsafeRunSync()
        .results shouldBe List(dsAndInternalProject.to[model.Entity.Dataset])
    }

    "select dataset on private project if there's no project with broader visibility" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndProject @ _ ::~ privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      upload(to = renkuDataset, privateProject)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)), maybeUser = member.toAuthUser.some)
        )
        .unsafeRunSync()
        .results shouldBe List(dsAndProject.to[model.Entity.Dataset])
    }
  }
}
