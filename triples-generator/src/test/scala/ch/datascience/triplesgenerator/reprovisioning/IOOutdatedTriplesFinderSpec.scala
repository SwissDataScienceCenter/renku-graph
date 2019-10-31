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
import ch.datascience.generators.Generators.setOf
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.RdfStoreData._
import org.scalacheck.Gen
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
      val triples = RDF(
        singleFileAndCommitTriples(project1, project1OutdatedCommit.toCommitId, None),
        singleFileAndCommitTriples(project2, project2OutdatedCommit.toCommitId, None)
      )

      loadToStore(triples)

      // format: off
      triplesFinder.findOutdatedTriples.value.unsafeRunSync() should (
        be(Some(OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project1), Set(project1OutdatedCommit)))) 
        or
        be(Some(OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project2), Set(project2OutdatedCommit))))
      )
      // format: on
    }

    "return single project's chunk of 10 commits having outdated triples in one go" in new TestCase {

      val project                  = projectPaths.generateOne
      val outdatedCommitsNumber    = 12
      val outdatedCommitsResources = Gen.listOfN(outdatedCommitsNumber, commitIdResources).generateOne
      val triples = RDF(
        outdatedCommitsResources map (commitId =>
          singleFileAndCommitTriples(project, commitId.toCommitId, Some(schemaVersions.generateOne))): _*
      )

      loadToStore(triples)

      val Some(outdatedTriples) = triplesFinder.findOutdatedTriples.value.unsafeRunSync()

      outdatedTriples.projectPath shouldBe FullProjectPath.from(renkuBaseUrl, project)
      outdatedTriples.commits     should have size 10
    }

    "return all project's commits having triples with no agent or agent with different version in one result" in new TestCase {

      val project1                = projectPaths.generateOne
      val project1Commit1NoAgent  = commitIdResources.generateOne
      val project1Commit2Outdated = commitIdResources.generateOne
      val project1Commit3UpToDate = commitIdResources.generateOne
      val triples = RDF(
        singleFileAndCommitTriples(project1, project1Commit1NoAgent.toCommitId, None),
        singleFileAndCommitTriples(project1, project1Commit2Outdated.toCommitId, Some(schemaVersions.generateOne)),
        singleFileAndCommitTriples(project1, project1Commit3UpToDate.toCommitId, Some(schemaVersion))
      )

      loadToStore(triples)

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe Some(
        OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project1),
                        Set(project1Commit1NoAgent, project1Commit2Outdated))
      )
    }

    "return no results if there's no project with outdated commits" in new TestCase {

      val project               = projectPaths.generateOne
      val projectCommitUpToDate = commitIdResources.generateOne
      val triples = RDF(
        singleFileAndCommitTriples(project, projectCommitUpToDate.toCommitId, Some(schemaVersion))
      )

      loadToStore(triples)

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
