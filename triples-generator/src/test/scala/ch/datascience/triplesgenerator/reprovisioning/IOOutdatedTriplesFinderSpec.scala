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

package ch.datascience.triplesgenerator.reprovisioning

import ReProvisioningGenerators._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FullProjectPath
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class IOOutdatedTriplesFinderSpec extends WordSpec with InMemoryRdfStore {

  "findOutdatedTriples" should {

    "return single project's commits if related to agent with version different than the current one - " +
      "case with multiple projects with outdated triples" in new TestCase {

      val project1               = projectPaths.generateOne
      val project1OutdatedCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project2               = projectPaths.generateOne
      val project2OutdatedCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project1,
                              commitId      = project1OutdatedCommit.toCommitId,
                              schemaVersion = schemaVersions.generateOne),
          singleFileAndCommit(project2,
                              commitId      = project2OutdatedCommit.toCommitId,
                              schemaVersion = schemaVersions.generateOne)
        )
      )

      // format: off
      triplesFinder.findOutdatedTriples.value.unsafeRunSync() should (
        be(Some(OutdatedTriples(FullProjectPath(renkuBaseUrl, project1), Set(project1OutdatedCommit))))
        or
        be(Some(OutdatedTriples(FullProjectPath(renkuBaseUrl, project2), Set(project2OutdatedCommit))))
      )
      // format: on
    }

    "return all project's commits having triples related to agent version different than the current one" in new TestCase {

      val project1                = projectPaths.generateOne
      val project1Commit1NoAgent  = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project1Commit2Outdated = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project1Commit3UpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project1,
                              commitId      = project1Commit1NoAgent.toCommitId,
                              schemaVersion = schemaVersions.generateOne),
          singleFileAndCommit(project1,
                              commitId           = project1Commit2Outdated.toCommitId,
                              schemaVersion      = schemaVersions.generateOne),
          singleFileAndCommit(project1, commitId = project1Commit3UpToDate.toCommitId, schemaVersion = schemaVersion)
        )
      )

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe Some(
        OutdatedTriples(FullProjectPath(renkuBaseUrl, project1), Set(project1Commit1NoAgent, project1Commit2Outdated))
      )
    }

    "return no results if there's no project with outdated commits" in new TestCase {

      val project               = projectPaths.generateOne
      val projectCommitUpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project, commitId = projectCommitUpToDate.toCommitId, schemaVersion = schemaVersion)
        )
      )

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val schemaVersion = schemaVersions.generateOne
    val triplesFinder = new IOOutdatedTriplesFinder(rdfStoreConfig, schemaVersion, TestLogger())
  }

  private implicit class CommitIdResouceOps(commitIdResource: CommitIdResource) {
    import cats.implicits._
    lazy val toCommitId = commitIdResource.as[Try, CommitId].fold(throw _, identity)
  }
}
