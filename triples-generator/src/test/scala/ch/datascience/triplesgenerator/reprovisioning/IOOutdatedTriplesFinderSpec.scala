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

    "return single project's commits having triples with either no agent or agent with version different that the current one " +
      "if there are multiple projects with outdated triples" in new TestCase {

      val project1               = projectPaths.generateOne
      val project1OutdatedCommit = commitIdResources.generateOne
      val project2               = projectPaths.generateOne
      val project2OutdatedCommit = commitIdResources.generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project1, project1OutdatedCommit.toCommitId, maybeSchemaVersion = None),
          singleFileAndCommit(project2, project2OutdatedCommit.toCommitId, maybeSchemaVersion = None)
        )
      )

      // format: off
      triplesFinder.findOutdatedTriples.value.unsafeRunSync() should (
        be(Some(OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project1), Set(project1OutdatedCommit)))) 
        or
        be(Some(OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project2), Set(project2OutdatedCommit))))
      )
      // format: on
    }

    "return all project's commits having triples with no agent or agent with different version in one result" in new TestCase {

      val project1                = projectPaths.generateOne
      val project1Commit1NoAgent  = commitIdResources.generateOne
      val project1Commit2Outdated = commitIdResources.generateOne
      val project1Commit3UpToDate = commitIdResources.generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project1, project1Commit1NoAgent.toCommitId, maybeSchemaVersion = None),
          singleFileAndCommit(project1,
                              project1Commit2Outdated.toCommitId,
                              maybeSchemaVersion                                               = Some(schemaVersions.generateOne)),
          singleFileAndCommit(project1, project1Commit3UpToDate.toCommitId, maybeSchemaVersion = Some(schemaVersion))
        )
      )

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe Some(
        OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project1),
                        Set(project1Commit1NoAgent, project1Commit2Outdated))
      )
    }

    "return no results if there's no project with outdated commits" in new TestCase {

      val project               = projectPaths.generateOne
      val projectCommitUpToDate = commitIdResources.generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project, projectCommitUpToDate.toCommitId, maybeSchemaVersion = Some(schemaVersion))
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
    lazy val toCommitId = commitIdResource.to[Try, CommitId].fold(throw _, identity)
  }
}
