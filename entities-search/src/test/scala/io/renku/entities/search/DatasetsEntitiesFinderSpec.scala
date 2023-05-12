/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.entities.search

import Criteria.{Filters, Sort}
import EntityConverters._
import cats.effect.IO
import cats.syntax.all._
import io.renku.entities.search.diff.SearchDiffInstances
import io.renku.entities.searchgraphs.SearchInfoDataset
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.http.rest.{SortBy, Sorting}
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class DatasetsEntitiesFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with EntitiesGenerators
    with FinderSpecOps
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDataset
    with SearchDiffInstances
    with AdditionalMatchers
    with IOSpec {

  implicit val ioLogger: Logger[IO] = Slf4jLogger.getLogger[IO]

  "findEntities - in case of a shared datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in new TestCase {
      val originalDSAndProject @ originalDS -> originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val _ -> importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      val result = IOBody(
        List(originalDSProject, importedDSProject)
          .traverse_(provisionTestProject)
          .flatMap(_ =>
            finder
              .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
          )
      )

      result.results shouldBe List(originalDSAndProject.to[model.Entity.Dataset]).sortBy(_.name)(nameOrdering)
    }

    "de-duplicate datasets having equal sameAs - case of an Exported DS" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val importedDSAndProject1 @ importedDS1 -> project1WithImportedDS = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val importedDSAndProject2 @ _ -> project2WithImportedDS = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val importedDSAndProject3 @ _ -> projectWithDSImportedFromProject = renkuProjectEntities(visibilityPublic)
        .importDataset(importedDS1)
        .generateOne

      val result = IOBody {
        provisionTestProjects(project1WithImportedDS, project2WithImportedDS, projectWithDSImportedFromProject)
          .flatMap(_ =>
            finder
              .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
          )
      }

      result.results should {
        be(List(importedDSAndProject1.to[model.Entity.Dataset])) or
          be(List(importedDSAndProject2.to[model.Entity.Dataset])) or
          be(List(importedDSAndProject3.to[model.Entity.Dataset]))
      }
    }
  }

  "findEntities - in case of a modified datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in new TestCase {
      val ((originalDS, modifiedDS), originalDSProject) = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ -> importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      val result = IOBody {
        provisionTestProjects(originalDSProject, importedDSProject) >> finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     sorting = Sorting(Sort.By(Sort.ByDate, SortBy.Direction.Asc))
            )
          )
      }

      result.results shouldMatchTo List(
        (modifiedDS -> originalDSProject).to[model.Entity.Dataset],
        importedDSAndProject.to[model.Entity.Dataset]
      ).sortBy(_.dateAsInstant)
    }
  }

  "findEntities - in case of a invalidated datasets" should {

    "not return invalidated DS" in new TestCase {
      val ((originalDS, _), originalDSProject) = renkuProjectEntities(visibilityPublic)
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ -> importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      val result = IOBody {
        provisionTestProjects(originalDSProject, importedDSProject).flatMap(_ =>
          finder
            .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
        )
      }

      result.results shouldBe List(importedDSAndProject.to[model.Entity.Dataset]).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - in case of a forks with datasets" should {

    "de-duplicate datasets when on forked projects" in new TestCase {
      val ((_, modifiedDS), originalDSProject) = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val original -> fork = originalDSProject.forkOnce()

      val result = IOBody {
        provisionTestProjects(original, fork) >>
          finder.findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
      }

      result.results should {
        be(List((modifiedDS -> original).to[model.Entity.Dataset])) or
          be(List((modifiedDS -> fork).to[model.Entity.Dataset]))
      }
    }
  }

  "findEntities - in case of a dataset on forks with different visibility" should {

    "favour dataset on public projects if exist" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val dsAndPublicProject @ _ -> publicProject = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val original -> fork = {
        val original -> fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }

      val result = IOBody {
        provisionTestProjects(original, fork) >> finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     maybeUser = member.toAuthUser.some
            )
          )
      }

      result.results shouldBe List(dsAndPublicProject.to[model.Entity.Dataset])
    }

    "favour dataset on internal projects over private projects if exist" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndInternalProject @ _ -> internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      val original -> fork = {
        val original -> fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }

      val result = IOBody {
        provisionTestProjects(original, fork) >> finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     maybeUser = member.toAuthUser.some
            )
          )
      }

      result.results shouldBe List(dsAndInternalProject.to[model.Entity.Dataset])
    }

    "select dataset on private project if there's no project with broader visibility" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndProject @ _ -> privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      val result = IOBody {
        provisionTestProjects(privateProject) >> finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     maybeUser = member.toAuthUser.some
            )
          )
      }

      result.results shouldBe List(dsAndProject.to[model.Entity.Dataset])
    }
  }
}
