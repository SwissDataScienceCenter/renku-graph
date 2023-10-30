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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.search.Criteria.{Filters, Sort}
import io.renku.entities.search.EntityConverters._
import io.renku.entities.search.diff.SearchDiffInstances
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.http.rest.{SortBy, Sorting}
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DatasetsEntitiesFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with should.Matchers
    with EntitiesGenerators
    with FinderSpecOps
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with SearchDiffInstances
    with AdditionalMatchers {

  "findEntities - in case of a shared datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in {

      val originalDSAndProject @ originalDS -> originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val _ -> importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      provisionTestProjects(originalDSProject, importedDSProject) >>
        finder
          .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
          .asserting { paged =>
            paged.results shouldBe List(originalDSAndProject.to[model.Entity.Dataset]).sortBy(_.name)(nameOrdering)
          }
    }

    "de-duplicate datasets having equal sameAs - case of an Exported DS" in {

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

      provisionTestProjects(project1WithImportedDS, project2WithImportedDS, projectWithDSImportedFromProject) >>
        finder.findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset)))).asserting { paged =>
          paged.results should {
            be(List(importedDSAndProject1.to[model.Entity.Dataset])) or
              be(List(importedDSAndProject2.to[model.Entity.Dataset])) or
              be(List(importedDSAndProject3.to[model.Entity.Dataset]))
          }
        }
    }
  }

  "findEntities - in case of a modified datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in {

      val ((originalDS, modifiedDS), originalDSProject) = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ -> importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      provisionTestProjects(originalDSProject, importedDSProject) >>
        finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     sorting = Sorting(Sort.By(Sort.ByDate, SortBy.Direction.Asc))
            )
          )
          .asserting { paged =>
            paged.results shouldMatchTo List(
              (modifiedDS -> originalDSProject).to[model.Entity.Dataset],
              importedDSAndProject.to[model.Entity.Dataset]
            ).sortBy(_.dateAsInstant).map(_.copy(date = originalDS.provenance.date))
          }

    }
  }

  "findEntities - in case of a invalidated datasets" should {

    "not return invalidated DS" in {

      val ((originalDS, _), originalDSProject) = renkuProjectEntities(visibilityPublic)
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ -> importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      provisionTestProjects(originalDSProject, importedDSProject) >>
        finder
          .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset))))
          .asserting { paged =>
            paged.results shouldBe List(importedDSAndProject.to[model.Entity.Dataset]).sortBy(_.name)(nameOrdering)
          }
    }
  }

  "findEntities - in case of a forks with datasets" should {

    "de-duplicate datasets when on forked projects" in {

      val ((originalDS, modifiedDS), originalDSProject) = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val original -> fork = originalDSProject.forkOnce()

      provisionTestProjects(original, fork) >>
        finder.findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Dataset)))).asserting { paged =>
          paged.results should {
            be(List((modifiedDS -> original).to[model.Entity.Dataset].copy(date = originalDS.provenance.date))) or
              be(List((modifiedDS -> fork).to[model.Entity.Dataset].copy(date = originalDS.provenance.date)))
          }
        }
    }
  }

  "findEntities - in case of a dataset on forks with different visibility" should {

    "favour dataset on public projects if exist" in {

      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val dsAndPublicProject @ _ -> publicProject = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val member = projectMemberEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val original -> fork = {
        val original -> fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }

      provisionTestProjects(original, fork) >>
        finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     maybeUser = member.person.toAuthUser.some
            )
          )
          .asserting { paged =>
            paged.results shouldBe List(dsAndPublicProject.to[model.Entity.Dataset])
          }
    }

    "favour dataset on internal projects over private projects if exist" in {

      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = projectMemberEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndInternalProject @ _ -> internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      val original -> fork = {
        val original -> fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }

      provisionTestProjects(original, fork) >>
        finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     maybeUser = member.person.toAuthUser.some
            )
          )
          .asserting { paged =>
            paged.results shouldBe List(dsAndInternalProject.to[model.Entity.Dataset])
          }
    }

    "select dataset on private project if there's no project with broader visibility" in {

      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = projectMemberEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndProject @ _ -> privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      provisionTestProjects(privateProject) >>
        finder
          .findEntities(
            Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Dataset)),
                     maybeUser = member.person.toAuthUser.some
            )
          )
          .asserting { paged =>
            paged.results shouldBe List(dsAndProject.to[model.Entity.Dataset])
          }
    }
  }

  private implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val finder: EntitiesFinder[IO] =
    new EntitiesFinderImpl[IO](projectsDSConnectionInfo, EntitiesFinder.finders)
}
