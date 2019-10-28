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

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators.{emails, names, schemaVersions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.setOf
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FullProjectPath
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities.Person
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningGenerators.commitIdResources
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class IOOutdatedTriplesRemoverSpec extends WordSpec with InMemoryRdfStore {

  "removeOutdatedTriples" should {

    "remove all and only the triples from a given project related to the given commits" in new TestCase {

      val project1                = projectPaths.generateOne
      val project1Commit1Outdated = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project1Commit2UpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val project2                = projectPaths.generateOne
      val project2OutdatedCommit  = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project1, commitId = project1Commit1Outdated.toCommitId),
          singleFileAndCommit(project1, commitId = project1Commit2UpToDate.toCommitId),
          singleFileAndCommit(project2, commitId = project2OutdatedCommit.toCommitId)
        )
      )

      val outdatedTriples = OutdatedTriples(
        projectResource = ProjectResource(FullProjectPath(renkuBaseUrl, project1).toString),
        commits         = Set(project1Commit1Outdated)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}").unsafeRunSync()
      leftTriples
        .map(row => row("subject"))
        .toSet
        .filter(triplesMatchingCommits(outdatedTriples.commits)) shouldBe empty
    }

    "remove all the triples for the given project and commits when project resource in the old format" in new TestCase {

      val project                = projectPaths.generateOne
      val projectResource        = ProjectResource((renkuBaseUrl / project).toString)
      val projectCommit1Outdated = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectCommit2UpToDate = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(
            project,
            commitId = projectCommit1Outdated.toCommitId
          ) map projectIdsToOldFormat(projectResource),
          singleFileAndCommit(
            project,
            commitId = projectCommit2UpToDate.toCommitId
          ) map projectIdsToOldFormat(projectResource)
        )
      )

      val outdatedTriples = OutdatedTriples(projectResource, Set(projectCommit1Outdated))

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}").unsafeRunSync()
      leftTriples
        .map(row => row("subject"))
        .toSet
        .filter(triplesMatchingCommits(outdatedTriples.commits)) shouldBe empty

      logger.loggedOnly(Warn(
        s"Removing outdated triples for '${outdatedTriples.projectResource}' finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "remove all the triples related to the given commits together with eventual dataset and authors triples" in new TestCase {

      val project                 = projectPaths.generateOne
      val outdatedProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val outdatedCommitDatasetId = datasetIds.generateOne
      val outdatedDatasetCreators = setOf(datasetCreators).generateOne
      val upToDateProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val upToDateCommitDatasetId = datasetIds.generateOne
      val upToDateDatasetCreators = setOf(datasetCreators).generateOne

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(
            project,
            commitId             = outdatedProjectCommit.toCommitId,
            datasetIdentifier    = outdatedCommitDatasetId,
            maybeDatasetCreators = outdatedDatasetCreators
          ),
          singleFileAndCommitWithDataset(
            project,
            commitId             = upToDateProjectCommit.toCommitId,
            datasetIdentifier    = upToDateCommitDatasetId,
            maybeDatasetCreators = upToDateDatasetCreators
          )
        )
      )

      val outdatedTriples = OutdatedTriples(
        projectResource = ProjectResource(FullProjectPath(renkuBaseUrl, project).toString),
        commits         = Set(outdatedProjectCommit)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatchingCommits(outdatedTriples.commits))    shouldBe empty
      leftTriples.filter(_ contains outdatedCommitDatasetId.value)           shouldBe empty
      leftTriples.filter(triplesMatchingCreators(outdatedDatasetCreators))   shouldBe empty
      leftTriples.filter(triplesMatchingCommits(Set(upToDateProjectCommit))) should not be empty
      leftTriples.filter(_ contains upToDateProjectCommit.value)             should not be empty
      leftTriples.filter(triplesMatchingCreators(upToDateDatasetCreators))   should not be empty
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

      val outdatedTriples = OutdatedTriples(
        projectResource = ProjectResource(FullProjectPath(renkuBaseUrl, project).toString),
        commits         = Set(projectCommitOutdated)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatchingCommits(outdatedTriples.commits)) shouldBe empty
      leftTriples.filter(_ contains outdatedSchemaVersion.value)          shouldBe empty
    }

    "remove all the triples related to the given commits together with orphan project triples" in new TestCase {

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

      val outdatedTriples = OutdatedTriples(
        projectResource = ProjectResource(FullProjectPath(renkuBaseUrl, project).toString),
        commits         = Set(projectCommitOutdated)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatchingCommits(outdatedTriples.commits)) shouldBe empty
      leftTriples.filter(_ contains project.value)                        shouldBe empty
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

      val outdatedTriples = OutdatedTriples(
        projectResource = ProjectResource(FullProjectPath(renkuBaseUrl, project1).toString),
        commits         = Set(project1OutdatedCommit)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val leftTriples = runQuery("SELECT ?subject ?o ?p WHERE {?subject ?o ?p}")
        .unsafeRunSync()
        .map(row => row("subject"))
        .toSet
      leftTriples.filter(triplesMatchingCommits(outdatedTriples.commits)) shouldBe empty
      leftTriples.filter(_ contains outdatedSchemaVersion.value)          should not be empty
    }
  }

  private trait TestCase {
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val triplesRemover        = new IOOutdatedTriplesRemover(rdfStoreConfig, executionTimeRecorder, logger)
  }

  private def triplesMatchingCommits(commits: Set[CommitIdResource])(subject: String) =
    commits.map(_.toCommitId.value) exists subject.contains

  private def triplesMatchingCreators(creators: Set[(UserName, Option[Email])])(subject: String) =
    creators.map { case (userName, _) => Person.Id(userName).value } exists subject.contains

  private implicit class CommitIdResouceOps(commitIdResource: CommitIdResource) {
    import cats.implicits._
    lazy val toCommitId = commitIdResource.as[Try, CommitId].fold(throw _, identity)
  }

  private val datasetCreators: Gen[(UserName, Option[Email])] = for {
    name       <- names
    maybeEmail <- Gen.option(emails)
  } yield (name, maybeEmail)
}
