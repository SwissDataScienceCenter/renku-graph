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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.EntityGenerators.invalidationEntity
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.ProjectsGenerators.projects
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IOProjectDatasetsFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProjectDatasets" should {

    "return the very last modification of a dataset in the given project" in new TestCase {
      forAll(projects) { project =>
        val originalDataset = nonModifiedDatasets().generateOne.copy(projects = List(project.toDatasetProject))
        val datasetModification1 = modifiedDatasetsOnFirstProject(originalDataset).generateOne
          .copy(maybeDescription = datasetDescriptions.generateSome)
        val datasetModification2 = modifiedDatasetsOnFirstProject(datasetModification1).generateOne
          .copy(maybeDescription = datasetDescriptions.generateSome)

        loadToStore(
          randomDataSetCommit,
          originalDataset.toJsonLD()(projects = List(project)),
          datasetModification1.toJsonLD(topmostDerivedFrom = originalDataset.entityId.asTopmostDerivedFrom,
                                        projects = List(project)
          ),
          datasetModification2.toJsonLD(topmostDerivedFrom = originalDataset.entityId.asTopmostDerivedFrom,
                                        projects = List(project)
          )
        )

        datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
          (datasetModification2.id,
           originalDataset.versions.initial,
           datasetModification2.title,
           datasetModification2.name,
           Right(datasetModification2.derivedFrom)
          )
        )
      }
    }

    "return non-modified datasets and the very last modifications of project's datasets" in new TestCase {
      forAll(projects) { project =>
        val dataset1 = nonModifiedDatasets().generateOne.copy(projects = List(project.toDatasetProject))
        val dataset2 = nonModifiedDatasets().generateOne.copy(projects = List(project.toDatasetProject))
        val dataset2Modification =
          modifiedDatasetsOnFirstProject(dataset2).generateOne.copy(maybeDescription = datasetDescriptions.generateSome)

        loadToStore(
          dataset1.toJsonLD()(projects = List(project)),
          dataset2.toJsonLD()(projects = List(project)),
          dataset2Modification.toJsonLD(projects = List(project))
        )

        datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
          (dataset1.id, dataset1.versions.initial, dataset1.title, dataset1.name, Left(dataset1.sameAs)),
          (dataset2Modification.id,
           dataset2.versions.initial,
           dataset2Modification.title,
           dataset2Modification.name,
           Right(dataset2Modification.derivedFrom)
          )
        )
      }
    }

    "return all datasets of the given project without merging datasets having the same sameAs" in new TestCase {
      forAll(projects) { project =>
        val sharedSameAs = datasetSameAs.generateOne
        val dataset1 =
          nonModifiedDatasets().generateOne.copy(sameAs = sharedSameAs, projects = List(project.toDatasetProject))
        val dataset2 =
          nonModifiedDatasets().generateOne.copy(sameAs = sharedSameAs, projects = List(project.toDatasetProject))

        loadToStore(
          randomDataSetCommit,
          dataset1.toJsonLD()(projects = List(project)),
          dataset2.toJsonLD()(projects = List(project))
        )

        datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
          (dataset1.id, dataset1.versions.initial, dataset1.title, dataset1.name, Left(sharedSameAs)),
          (dataset2.id, dataset2.versions.initial, dataset2.title, dataset2.name, Left(sharedSameAs))
        )
      }
    }

    "return None if there are no datasets in the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      datasetsFinder.findProjectDatasets(projectPath).unsafeRunSync() shouldBe List.empty
    }

    "not returned deleted dataset" in new TestCase {
      forAll(projects) { project =>
        val datasetProject = project.toDatasetProject
        val dataset1       = nonModifiedDatasets().generateOne.copy(projects = List(datasetProject))
        val datasetToBeInvalidated = nonModifiedDatasets(
        ).generateOne.copy(projects = List(datasetProject))

        val entityWithInvalidation = invalidationEntity(datasetToBeInvalidated.id, project).generateOne
        loadToStore(
          dataset1.toJsonLD()(projects = List(project)),
          datasetToBeInvalidated.toJsonLD()(projects = List(project)),
          entityWithInvalidation.asJsonLD
        )

        datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
          (dataset1.id, dataset1.versions.initial, dataset1.title, dataset1.name, Left(dataset1.sameAs))
        )
      }
    }

    "not returned deleted dataset when its latest version was deleted" in new TestCase {
      forAll(projects) { project =>
        val datasetProject = project.toDatasetProject
        val dataset1       = nonModifiedDatasets().generateOne.copy(projects = List(datasetProject))
        val dataset2 = nonModifiedDatasets(
        ).generateOne.copy(projects = List(datasetProject))

        val dataset2Modification =
          modifiedDatasetsOnFirstProject(dataset2).generateOne.copy(maybeDescription = datasetDescriptions.generateSome,
                                                                    projects = List(datasetProject)
          )

        val entityWithInvalidation =
          invalidationEntity(dataset2Modification.id, project, dataset2.entityId.asTopmostDerivedFrom.some).generateOne
        loadToStore(
          dataset1.toJsonLD()(projects = List(project)),
          dataset2.toJsonLD()(projects = List(project)),
          dataset2Modification.toJsonLD(topmostDerivedFrom = dataset2.entityId.asTopmostDerivedFrom,
                                        projects = List(project)
          ),
          entityWithInvalidation.asJsonLD
        )

        datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
          (dataset1.id, dataset1.versions.initial, dataset1.title, dataset1.name, Left(dataset1.sameAs))
        )
      }
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetsFinder       = new IOProjectDatasetsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
