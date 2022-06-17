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

package io.renku.graph.http.server.security

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabApiUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DatasetIdRecordsFinderSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "apply" should {

    "return SecurityRecord with project visibility, path and all project members" in new TestCase {
      val (dataset, project) =
        renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceNonModified)).generateOne

      loadToStore(project)

      recordsFinder(dataset.identification.identifier).unsafeRunSync() shouldBe List(
        (project.visibility, project.path, project.members.flatMap(_.maybeGitLabId))
      )
    }

    "return SecurityRecord with project visibility, path and no member is project has none" in new TestCase {
      val (dataset, project) =
        renkuProjectEntities(anyVisibility)
          .map(_.copy(members = Set.empty))
          .addDataset(datasetEntities(provenanceNonModified))
          .generateOne

      loadToStore(project)

      recordsFinder(dataset.identification.identifier).unsafeRunSync() shouldBe List(
        (project.visibility, project.path, Set.empty)
      )
    }

    "return SecurityRecords with projects visibilities, paths and members" in new TestCase {
      val (dataset, parentProject ::~ project) =
        renkuProjectEntities(anyVisibility)
          .map(_.copy(members = Set.empty))
          .addDataset(datasetEntities(provenanceNonModified))
          .forkOnce()
          .generateOne

      loadToStore(parentProject, project)

      recordsFinder(dataset.identification.identifier).unsafeRunSync() should contain theSameElementsAs List(
        (parentProject.visibility, parentProject.path, parentProject.members.flatMap(_.maybeGitLabId)),
        (project.visibility, project.path, project.members.flatMap(_.maybeGitLabId))
      )
    }

    "nothing if there's no project with the given path" in new TestCase {
      recordsFinder(datasetIdentifiers.generateOne).unsafeRunSync() shouldBe Nil
    }
  }

  private implicit lazy val gitLabApiUrl: GitLabApiUrl = gitLabUrls.generateOne.apiV4

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val recordsFinder = new DatasetIdRecordsFinderImpl[IO](renkuStoreConfig)
  }
}
