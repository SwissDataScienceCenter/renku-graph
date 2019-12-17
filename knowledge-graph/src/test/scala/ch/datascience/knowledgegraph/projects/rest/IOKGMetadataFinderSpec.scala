/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples.{renkuBaseUrl, singleFileAndCommit, triples}
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IOKGMetadataFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findProject" should {

    "return details of the project with the given path" in new TestCase {
      forAll { project: Project =>
        loadToStore(
          triples(
            singleFileAndCommit(projectPaths.generateOne, commitId = commitIds.generateOne),
            singleFileAndCommit(project.path,
                                project.name,
                                project.created.date,
                                project.created.creator.name -> project.created.creator.email,
                                commitIds.generateOne)
          )
        )

        metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project)
      }
    }

    "return None if there's no project with the given path" in new TestCase {
      metadataFinder.findProject(projectPaths.generateOne).unsafeRunSync() shouldBe None
    }

    "throw an exception if there's more than on project with the given path" in new TestCase {
      val projectPath = projectPaths.generateOne

      loadToStore(
        triples(
          singleFileAndCommit(projectPath, commitId = commitIds.generateOne),
          singleFileAndCommit(projectPath, commitId = commitIds.generateOne)
        )
      )

      intercept[Exception] {
        metadataFinder.findProject(projectPath).unsafeRunSync() shouldBe None
      }.getMessage shouldBe s"More than one project with $projectPath path"
    }
  }

  private trait TestCase {
    private val logger = TestLogger[IO]()
    val metadataFinder = new IOKGMetadataFinder(rdfStoreConfig, renkuBaseUrl, logger)
  }
}
