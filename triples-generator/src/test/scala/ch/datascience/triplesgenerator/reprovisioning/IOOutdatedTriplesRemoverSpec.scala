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
import ch.datascience.generators.Generators.{nonEmptyList, nonEmptySet}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.Identifier
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FullProjectPath, ProjectPath}
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities._
import ch.datascience.rdfstore.triples.singleFileAndCommitWithDataset.datasetParts
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
        projectResource = project1.toProjectResource,
        commits         = Set(project1Commit1Outdated)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      findCommits should contain only (project1Commit2UpToDate.value, project2OutdatedCommit.value)
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

      findCommits should contain only projectCommit2UpToDate.value

      logger.loggedOnly(
        Warn(
          s"Removing outdated triples for '${outdatedTriples.projectResource}' finished${executionTimeRecorder.executionTimeInfo}"
        )
      )
    }

    "remove all the triples related to the given commits and do not touch the dataset " +
      "if there's at least one other link between dataset's and activity's isPartOf for that project" in new TestCase {

      val project                 = projectPaths.generateOne
      val outdatedProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val outdatedCommitDatasetId = datasetIds.generateOne
      val outdatedDatasetCreators = nonEmptySet(datasetCreators).generateOne
      val upToDateProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val upToDateCommitDatasetId = datasetIds.generateOne
      val upToDateDatasetCreators = nonEmptySet(datasetCreators).generateOne

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
        projectResource = project.toProjectResource,
        commits         = Set(outdatedProjectCommit)
      )

      findDatasetProjects(outdatedCommitDatasetId) shouldBe Set(project.toProjectResource.toString)

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      findCommits should contain only upToDateProjectCommit.value

      findDatasetProjects(outdatedCommitDatasetId) shouldBe Set(project.toProjectResource.toString)

      findDatasets shouldBe Set(outdatedCommitDatasetId.toString, upToDateCommitDatasetId.toString)
    }

    "remove all the triples related to the given commits and the relevant single dataset's and dataset parts' isPartOf triple " +
      "if there're no more links between dataset's and activity's isPartOf except the one under removal" in new TestCase {

      val outdatedProject         = projectPaths.generateOne
      val outdatedProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val outdatedCommitDatasetId = datasetIds.generateOne
      val outdatedDatasetCreators = nonEmptySet(datasetCreators).generateOne
      val outdatedDatasetParts    = nonEmptyList(datasetParts).generateOne.toList
      val upToDateProject         = projectPaths.generateOne
      val upToDateProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val upToDateCommitDatasetId = datasetIds.generateOne
      val upToDateDatasetCreators = nonEmptySet(datasetCreators).generateOne
      val upToDateDatasetParts    = nonEmptyList(datasetParts).generateOne.toList

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(
            outdatedProject,
            commitId             = outdatedProjectCommit.toCommitId,
            datasetIdentifier    = outdatedCommitDatasetId,
            maybeDatasetCreators = outdatedDatasetCreators,
            maybeDatasetParts    = outdatedDatasetParts
          ),
          singleFileAndCommitWithDataset(
            upToDateProject,
            commitId             = upToDateProjectCommit.toCommitId,
            datasetIdentifier    = upToDateCommitDatasetId,
            maybeDatasetCreators = upToDateDatasetCreators,
            maybeDatasetParts    = upToDateDatasetParts
          )
        )
      )

      val outdatedTriples = OutdatedTriples(
        projectResource = outdatedProject.toProjectResource,
        commits         = Set(outdatedProjectCommit)
      )

      findDatasetProjects(outdatedCommitDatasetId) shouldBe Set(outdatedProject.toProjectResource.toString)
      findDatasetProjects(upToDateCommitDatasetId) shouldBe Set(upToDateProject.toProjectResource.toString)

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      findCommits should contain only upToDateProjectCommit.value

      findDatasetProjects(outdatedCommitDatasetId)      shouldBe empty
      findDatasetPartsProjects(outdatedCommitDatasetId) shouldBe empty
      findDatasetProjects(upToDateCommitDatasetId)      shouldBe Set(upToDateProject.toProjectResource.toString)
      findDatasetPartsProjects(upToDateCommitDatasetId) shouldBe Set(upToDateProject.toProjectResource.toString)

      findDatasets shouldBe Set(outdatedCommitDatasetId.toString, upToDateCommitDatasetId.toString)

      findDatasetParts(outdatedCommitDatasetId) should have size outdatedDatasetParts.size
      findDatasetParts(upToDateCommitDatasetId) should have size upToDateDatasetParts.size
    }

    "remove all the triples related to the given commits and the relevant single dataset's and dataset parts' isPartOf triple" in new TestCase {

      val outdatedProject         = projectPaths.generateOne
      val outdatedProjectCommit   = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val outdatedCommitDatasetId = datasetIds.generateOne
      val outdatedDatasetCreators = nonEmptySet(datasetCreators).generateOne
      val outdatedDatasetParts    = nonEmptyList(datasetParts).generateOne.toList
      val projectCreator          = names.generateOne
      val committerName           = names.generateOne
      val schemaVersion           = schemaVersions.generateOne

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(
            outdatedProject,
            projectCreator       = projectCreator -> emails.generateOne,
            commitId             = outdatedProjectCommit.toCommitId,
            committerName        = committerName,
            datasetIdentifier    = outdatedCommitDatasetId,
            maybeDatasetCreators = outdatedDatasetCreators,
            maybeDatasetParts    = outdatedDatasetParts,
            schemaVersion        = schemaVersion
          )
        )
      )

      val outdatedTriples = OutdatedTriples(
        projectResource = outdatedProject.toProjectResource,
        commits         = Set(outdatedProjectCommit)
      )

      triplesRemover
        .removeOutdatedTriples(outdatedTriples)
        .unsafeRunSync() shouldBe ((): Unit)

      val allSubjects        = findAllSubjects
      val leftDatasetTriples = allSubjects.filter(_ contains Dataset.Id(outdatedCommitDatasetId).value)
      val leftDatasetPartTriples = allSubjects.filter(
        subject =>
          outdatedDatasetParts.map(_._2).map(DatasetPart.Id(outdatedProjectCommit.toCommitId, _).value) contains subject
      )
      val leftDatasetCreatorTriples = allSubjects.intersect(outdatedDatasetCreators.map(_._1).map(Person.Id(_).value))
      leftDatasetTriples        should not be empty
      leftDatasetPartTriples    should not be empty
      leftDatasetCreatorTriples should not be empty

      allSubjects
        .diff(leftDatasetTriples)
        .diff(leftDatasetPartTriples)
        .diff(leftDatasetCreatorTriples)
        .diff(Set(Agent.Id(schemaVersion).value))
        .diff(Set(Project.Id(renkuBaseUrl, outdatedProject).value, Person.Id(projectCreator).value))
        .diff(Set(Person.Id(committerName).value)) shouldBe empty
    }
  }

  private trait TestCase {
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val triplesRemover        = new IOOutdatedTriplesRemover(rdfStoreConfig, executionTimeRecorder, logger)
  }

  private implicit class CommitIdResouceOps(commitIdResource: CommitIdResource) {
    import cats.implicits._
    lazy val toCommitId = commitIdResource.as[Try, CommitId].fold(throw _, identity)
  }

  private implicit class ProjectPathOps(projectPath: ProjectPath) {
    lazy val toProjectResource = ProjectResource(FullProjectPath(renkuBaseUrl, projectPath).toString)
  }

  private val datasetCreators: Gen[(UserName, Option[Email])] = for {
    name       <- names
    maybeEmail <- Gen.option(emails)
  } yield (name, maybeEmail)

  private def findAllSubjects =
    runQuery(s"""|SELECT ?subject ?o ?p 
                 |WHERE { ?subject ?o ?p }
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("subject"))
      .toSet

  private def findCommits =
    runQuery(s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 |PREFIX schema: <http://schema.org/>
                 |
                 |SELECT ?commit ?o ?p 
                 |WHERE {
                 |  ?commit rdf:type prov:Activity
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("commit"))
      .toSet

  private def findDatasetProjects(datasetId: Identifier) =
    runQuery(
      s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX schema: <http://schema.org/>
          |
          |SELECT ?project
          |WHERE {
          |  ?dataset rdf:type <http://schema.org/Dataset> ;
          |           schema:identifier '$datasetId' ;
          |           schema:isPartOf ?project .
          |}
          |""".stripMargin
    ).unsafeRunSync()
      .map(row => row("project"))
      .toSet

  private def findDatasets =
    runQuery(
      s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX schema: <http://schema.org/>
          |
          |SELECT ?id
          |WHERE {
          |  ?dataset rdf:type <http://schema.org/Dataset> ;
          |           schema:identifier ?id .
          |}
          |""".stripMargin
    ).unsafeRunSync()
      .map(row => row("id"))
      .toSet

  private def findDatasetParts(datasetId: Identifier) =
    runQuery(
      s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX schema: <http://schema.org/>
          |
          |SELECT ?partName
          |WHERE {
          |  ?dataset rdf:type <http://schema.org/Dataset> ;
          |           schema:identifier '$datasetId' ;
          |           schema:hasPart ?part .
          |  ?part rdf:type <http://schema.org/DigitalDocument> ;
          |        schema:name ?partName .
          |}
          |""".stripMargin
    ).unsafeRunSync()
      .map(row => row("partName"))
      .toSet

  private def findDatasetPartsProjects(datasetId: Identifier) =
    runQuery(
      s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX schema: <http://schema.org/>
          |
          |SELECT ?project
          |WHERE {
          |  ?dataset rdf:type <http://schema.org/Dataset> ;
          |           schema:identifier '$datasetId' ;
          |           schema:hasPart ?part .
          |  ?part rdf:type <http://schema.org/DigitalDocument> ;
          |        schema:isPartOf ?project .
          |}
          |""".stripMargin
    ).unsafeRunSync()
      .map(row => row("project"))
      .toSet
}
