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
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.{Parent, ProjectCreator}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder, entities}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IOKGProjectFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProject" should {

    "return details of the project with the given path when there's no parent" in new TestCase {
      forAll(kgProjects(parentsGen = emptyOptionOf[Parent])) { project =>
        val maybeProjectCreator = project.created.maybeCreator
        loadToStore(
          fileCommit(commitId = commitIds.generateOne)(projectPath = projectPaths.generateOne),
          fileCommit(
            commitId      = commitIds.generateOne,
            committedDate = CommittedDate(project.created.date.value)
          )(
            projectPath         = project.path,
            projectName         = project.name,
            projectDateCreated  = project.created.date,
            maybeProjectCreator = maybeProjectCreator.toMaybePerson,
            maybeParent         = None
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
            projectPath         = project.path,
            projectName         = project.name,
            projectDateCreated  = project.created.date,
            maybeProjectCreator = project.created.maybeCreator.toMaybePerson,
            maybeParent = project.maybeParent.map { parent =>
              entities.Project(
                parent.resourceId.toUnsafe[Path],
                parent.name,
                parent.created.date,
                maybeCreator       = parent.created.maybeCreator.toMaybePerson,
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

  private implicit class ProjectCreatorOps(maybeCreator: Option[ProjectCreator]) {
    lazy val toMaybePerson: Option[entities.Person] = maybeCreator.map { creator =>
      entities.Person(creator.name, creator.maybeEmail)
    }
  }
}
