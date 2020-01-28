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

import java.time.temporal.ChronoUnit.DAYS

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.KGProject
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IOKGProjectFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findProject" should {

    "return details of the project with the given path" in new TestCase {
      forAll { project: KGProject =>
        val projectCreator = project.created.creator
        loadToStore(
          fileCommit(commitId = commitIds.generateOne)(projectPath = projectPaths.generateOne),
          fileCommit(
            commitId      = commitIds.generateOne,
            committedDate = CommittedDate(project.created.date.value),
            committer     = Person(projectCreator.name, projectCreator.email)
          )(
            projectPath = project.path,
            projectName = project.name
          )
        )

        metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project)
      }
    }

    "return details of the project with the given path if there are forks of it" in new TestCase {
      val project             = kgProjects.generateOne
      val projectCreationDate = committedDates.generateOne
      val projectCreator      = Person(project.created.creator.name, project.created.creator.email)
      val forkCreator         = Person(names.generateOne, emails.generateOne)
      loadToStore(
        fileCommit(
          commitId      = commitIds.generateOne,
          committedDate = projectCreationDate,
          committer     = projectCreator
        )(
          projectPath        = project.path,
          projectName        = project.name,
          projectDateCreated = project.created.date
        ),
        fileCommit(
          commitId      = commitIds.generateOne,
          committedDate = CommittedDate(projectCreationDate.value.plus(2, DAYS)),
          committer     = forkCreator
        )(
          projectPath        = project.path,
          projectName        = project.name,
          projectDateCreated = project.created.date
        )
      )

      metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project)
    }

    "return None if there's no project with the given path" in new TestCase {
      metadataFinder.findProject(projectPaths.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger = TestLogger[IO]()
    val metadataFinder = new IOKGProjectFinder(rdfStoreConfig, renkuBaseUrl, logger)
  }
}
