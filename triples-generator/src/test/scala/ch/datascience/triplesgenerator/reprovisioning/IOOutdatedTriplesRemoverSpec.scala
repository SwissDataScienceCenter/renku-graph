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
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.events.EventsGenerators.projectPaths
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.RdfStoreData._
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningGenerators.commitIdResources
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class IOOutdatedTriplesRemoverSpec extends WordSpec with InMemoryRdfStore {

  "removeOutdatedTriples" should {

    "remove all and only the triples from a given project related to the given commits" in new TestCase {

      val project1                = projectPaths.generateOne
      val project1Commit1NoAgent  = commitIdResources.generateOne
      val project1Commit2Outdated = commitIdResources.generateOne
      val project1Commit3UpToDate = commitIdResources.generateOne
      val project2                = projectPaths.generateOne
      val project2OutdatedCommit  = commitIdResources.generateOne
      val triples = RDF(
        singleFileAndCommitTriples(project1, project1Commit1NoAgent.toCommitId, None),
        singleFileAndCommitTriples(project1, project1Commit2Outdated.toCommitId, None),
        singleFileAndCommitTriples(project1, project1Commit3UpToDate.toCommitId, Some(schemaVersions.generateOne)),
        singleFileAndCommitTriples(project2, project2OutdatedCommit.toCommitId, None)
      )

      loadToStore(triples)

      val outdatedTriples = OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project1),
                                            Set(project1Commit1NoAgent, project1Commit2Outdated))

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
      val projectCommitOutdated = commitIdResources.generateOne
      val projectCommitUpToDate = commitIdResources.generateOne
      val triples = RDF(
        singleFileAndCommitWithDataset(project, projectCommitOutdated.toCommitId, schemaVersions.generateOne),
        singleFileAndCommitWithDataset(project, projectCommitUpToDate.toCommitId, schemaVersions.generateOne)
      )

      loadToStore(triples)

      val outdatedTriples = OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project), Set(projectCommitOutdated))

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
      val projectCommitOutdated = commitIdResources.generateOne
      val projectCommitUpToDate = commitIdResources.generateOne
      val triples = RDF(
        singleFileAndCommitWithDataset(project, projectCommitOutdated.toCommitId, outdatedSchemaVersion)
      )

      loadToStore(triples)

      val outdatedTriples = OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project), Set(projectCommitOutdated))

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
      val project1OutdatedCommit = commitIdResources.generateOne
      val project2               = projectPaths.generateOne
      val project2OutdatedCommit = commitIdResources.generateOne
      val triples = RDF(
        singleFileAndCommitWithDataset(project1, project1OutdatedCommit.toCommitId, outdatedSchemaVersion),
        singleFileAndCommitWithDataset(project2, project2OutdatedCommit.toCommitId, outdatedSchemaVersion)
      )

      loadToStore(triples)

      val outdatedTriples = OutdatedTriples(FullProjectPath.from(renkuBaseUrl, project1), Set(project1OutdatedCommit))

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatching(outdatedTriples.commits))                shouldBe empty
      leftTriples.filter(_ contains outdatedSchemaVersion.value.replace(".", "")) should not be empty
    }
  }

  private trait TestCase {
    val triplesRemover = new IOOutdatedTriplesRemover(rdfStoreConfig, TestLogger())
  }

  private def triplesMatching(commits: Set[CommitIdResource])(subject: String) =
    commits.map(_.toCommitId.value).exists(subject.contains)

  private implicit class CommitIdResouceOps(commitIdResource: CommitIdResource) {
    import cats.implicits._
    lazy val toCommitId = commitIdResource.to[Try, CommitId].fold(throw _, identity)
  }
}
