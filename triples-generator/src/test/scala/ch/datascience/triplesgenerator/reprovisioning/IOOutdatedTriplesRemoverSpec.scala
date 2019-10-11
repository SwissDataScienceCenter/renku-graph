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

import ch.datascience.generators.CommonGraphGenerators.schemaVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FullProjectPath
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningGenerators.commitIdResources
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class IOOutdatedTriplesRemoverSpec extends WordSpec with InMemoryRdfStore {

  "removeOutdatedTriples" should {

    "remove all and only the triples from a given project related to the given commits" in new TestCase {

      val project1                = projectPaths.generateOne
      val project1Commit1NoAgent  = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project1Commit2Outdated = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project1Commit3UpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project2                = projectPaths.generateOne
      val project2OutdatedCommit  = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project1, project1Commit1NoAgent.toCommitId, maybeSchemaVersion  = None),
          singleFileAndCommit(project1, project1Commit2Outdated.toCommitId, maybeSchemaVersion = None),
          singleFileAndCommit(project1,
                              project1Commit3UpToDate.toCommitId,
                              maybeSchemaVersion                                              = Some(schemaVersions.generateOne)),
          singleFileAndCommit(project2, project2OutdatedCommit.toCommitId, maybeSchemaVersion = None)
        )
      )

      val outdatedTriples = OutdatedTriples(
        FullProjectPath(renkuBaseUrl, project1),
        Set(project1Commit1NoAgent, project1Commit2Outdated)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}").unsafeRunSync()
      leftTriples
        .map(row => row("subject"))
        .toSet
        .filter(triplesMatching(outdatedTriples.commits)) shouldBe empty
    }

    "remove all the triples related to the given commits together with eventual dataset triples" in new TestCase {

      val project               = projectPaths.generateOne
      val projectCommitOutdated = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectCommitUpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(project,
                                         commitId      = projectCommitOutdated.toCommitId,
                                         schemaVersion = schemaVersions.generateOne),
          singleFileAndCommitWithDataset(project,
                                         commitId      = projectCommitUpToDate.toCommitId,
                                         schemaVersion = schemaVersions.generateOne)
        )
      )

      val outdatedTriples = OutdatedTriples(FullProjectPath(renkuBaseUrl, project), Set(projectCommitOutdated))

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}").unsafeRunSync()
      leftTriples
        .map(row => row("subject"))
        .toSet
        .filter(triplesMatching(outdatedTriples.commits)) shouldBe empty
    }

    "remove all the triples related to the given commits together with orphan agent triples" in new TestCase {

      val outdatedSchemaVersion = schemaVersions.generateOne
      val project               = projectPaths.generateOne
      val projectCommitOutdated = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectCommitUpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(project,
                                         commitId      = projectCommitOutdated.toCommitId,
                                         schemaVersion = outdatedSchemaVersion)
        )
      )

      val outdatedTriples = OutdatedTriples(FullProjectPath(renkuBaseUrl, project), Set(projectCommitOutdated))

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatching(outdatedTriples.commits)) shouldBe empty
      leftTriples.filter(_ contains outdatedSchemaVersion.value)   shouldBe empty
    }

    "do not remove agent triples if they are still referenced to another project" in new TestCase {

      val outdatedSchemaVersion  = schemaVersions.generateOne
      val project1               = projectPaths.generateOne
      val project1OutdatedCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project2               = projectPaths.generateOne
      val project2OutdatedCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(project1,
                                         commitId      = project1OutdatedCommit.toCommitId,
                                         schemaVersion = outdatedSchemaVersion),
          singleFileAndCommitWithDataset(project2,
                                         commitId      = project2OutdatedCommit.toCommitId,
                                         schemaVersion = outdatedSchemaVersion)
        )
      )

      val outdatedTriples = OutdatedTriples(FullProjectPath(renkuBaseUrl, project1), Set(project1OutdatedCommit))

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatching(outdatedTriples.commits)) shouldBe empty
      leftTriples.filter(_ contains outdatedSchemaVersion.value)   should not be empty
    }
  }

  private trait TestCase {
    val triplesRemover = new IOOutdatedTriplesRemover(rdfStoreConfig, TestLogger())
  }

  private def triplesMatching(commits: Set[CommitIdResource])(subject: String) =
    commits.map(_.toCommitId.value).exists(subject.contains)

  private implicit class CommitIdResouceOps(commitIdResource: CommitIdResource) {
    import cats.implicits._
    lazy val toCommitId = commitIdResource.as[Try, CommitId].fold(throw _, identity)
  }
}
