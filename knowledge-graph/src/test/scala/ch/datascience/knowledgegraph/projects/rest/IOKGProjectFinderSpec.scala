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

package ch.datascience.knowledgegraph.projects.rest

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.projects.Path
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.Parent
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder, entities}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IOKGProjectFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findProject" should {

    "return details of the project with the given path when there's no parent parent" in new TestCase {
      forAll(kgProjects(parentsGen = emptyOptionOf[Parent])) { project =>
        val projectCreator = project.created.creator
        loadToStore(
          fileCommit(commitId = commitIds.generateOne)(projectPath = projectPaths.generateOne),
          fileCommit(
            commitId      = commitIds.generateOne,
            committedDate = CommittedDate(project.created.date.value)
          )(
            projectPath        = project.path,
            projectName        = project.name,
            projectDateCreated = project.created.date,
            projectCreator     = Person(projectCreator.name, projectCreator.maybeEmail),
            maybeParent        = None
          )
        )

        metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project)
      }
    }

    "return details of the project with the given path if it has a parent project" in new TestCase {
      forAll(kgProjects(parentsGen = parents.toGeneratorOfSomes)) { project =>
        loadToStore(
          fileCommit(
            commitId = commitIds.generateOne
          )(
            projectPath        = project.path,
            projectName        = project.name,
            projectDateCreated = project.created.date,
            projectCreator     = entities.Person(project.created.creator.name, project.created.creator.maybeEmail),
            maybeParent = project.maybeParent.map { parent =>
              entities.Project(
                parent.resourceId.toUnsafe[Path],
                parent.name,
                parent.created.date,
                creator            = entities.Person(parent.created.creator.name, parent.created.creator.maybeEmail),
                maybeParentProject = None
              )
            }
          )
        )

        metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project)
      }
    }

    "return None if there's no project with the given path" in new TestCase {
      metadataFinder.findProject(projectPaths.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val metadataFinder       = new IOKGProjectFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
