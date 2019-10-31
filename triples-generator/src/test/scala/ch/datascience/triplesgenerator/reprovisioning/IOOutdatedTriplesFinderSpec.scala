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
import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.committedDates
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FilePath, FullProjectPath, ProjectPath}
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.graph.model.{SchemaVersion, projects, users}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.triples.entities._
import ch.datascience.rdfstore.triples.{renkuBaseUrl, _}
import ch.datascience.rdfstore.{FusekiBaseUrl, InMemoryRdfStore}
import io.circe.Json
import org.scalacheck.Gen
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
        be(Some(OutdatedTriples(ProjectResource(FullProjectPath(renkuBaseUrl, project1).toString), Set(project1OutdatedCommit))))
        or
        be(Some(OutdatedTriples(ProjectResource(FullProjectPath(renkuBaseUrl, project2).toString), Set(project2OutdatedCommit))))
      )
      // format: on

      logger.loggedOnly(Warn(s"Searching for outdated triples finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "return single project's chunk of 10 commits having outdated triples in one go" in new TestCase {

      val project                  = projectPaths.generateOne
      val outdatedCommitsResources = Gen.listOfN(12, commitIdResources(Some(fusekiBaseUrl.toString))).generateOne

      loadToStore(
        triples(
          outdatedCommitsResources map (
              commitResource =>
                singleFileAndCommit(project,
                                    commitId      = commitResource.toCommitId,
                                    schemaVersion = schemaVersions.generateOne)): _*
        )
      )

      val Some(outdatedTriples) = triplesFinder.findOutdatedTriples.value.unsafeRunSync()

      outdatedTriples.projectResource shouldBe ProjectResource(FullProjectPath(renkuBaseUrl, project).toString)
      outdatedTriples.commits         should have size 10
    }

    "return all project's commits having triples related to agent with version different than the current one" in new TestCase {

      val project                = projectPaths.generateOne
      val projectOutdatedCommit1 = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectOutdatedCommit2 = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectUpToDateCommit3 = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(project,
                              commitId      = projectOutdatedCommit1.toCommitId,
                              schemaVersion = schemaVersions.generateOne),
          singleFileAndCommit(project,
                              commitId          = projectOutdatedCommit2.toCommitId,
                              schemaVersion     = schemaVersions.generateOne),
          singleFileAndCommit(project, commitId = projectUpToDateCommit3.toCommitId, schemaVersion = schemaVersion)
        )
      )

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe Some(
        OutdatedTriples(ProjectResource(FullProjectPath(renkuBaseUrl, project).toString),
                        Set(projectOutdatedCommit1, projectOutdatedCommit2))
      )
    }

    "return project's commits having outdated triples if the project resource is in the old format" in new TestCase {

      val project               = projectPaths.generateOne
      val projectResource       = ProjectResource((renkuBaseUrl / project).toString)
      val projectOutdatedCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectUpToDateCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommit(
            project,
            commitId      = projectOutdatedCommit.toCommitId,
            schemaVersion = schemaVersions.generateOne
          ) map projectIdsToOldFormat(projectResource),
          singleFileAndCommit(
            project,
            commitId      = projectUpToDateCommit.toCommitId,
            schemaVersion = schemaVersion
          ) map projectIdsToOldFormat(projectResource)
        )
      )

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe Some(
        OutdatedTriples(projectResource, Set(projectOutdatedCommit))
      )
    }

    "return project's commits having outdated triples if the project resource on the prov:Entity" in new TestCase {

      val project               = projectPaths.generateOne
      val projectOutdatedCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne
      val projectUpToDateCommit = commitIdResources(Some(fusekiBaseUrl.toString)).generateOne

      loadToStore(
        triples(
          singleFileAndCommitWithProjectOnEntityOnly(
            project,
            commitId      = projectOutdatedCommit.toCommitId,
            schemaVersion = schemaVersions.generateOne
          ),
          singleFileAndCommitWithProjectOnEntityOnly(
            project,
            commitId      = projectUpToDateCommit.toCommitId,
            schemaVersion = schemaVersion
          )
        )
      )

      triplesFinder.findOutdatedTriples.value.unsafeRunSync() shouldBe Some(
        OutdatedTriples(ProjectResource(FullProjectPath(renkuBaseUrl, project).toString), Set(projectOutdatedCommit))
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
    val schemaVersion         = schemaVersions.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val triplesFinder         = new IOOutdatedTriplesFinder(rdfStoreConfig, executionTimeRecorder, schemaVersion, logger)
  }

  private implicit class CommitIdResouceOps(commitIdResource: CommitIdResource) {
    import cats.implicits._
    lazy val toCommitId = commitIdResource.as[Try, CommitId].fold(throw _, identity)
  }

  object singleFileAndCommitWithProjectOnEntityOnly {

    def apply(projectPath:        ProjectPath,
              projectName:        projects.Name = projectNames.generateOne,
              projectDateCreated: projects.DateCreated = projectCreatedDates.generateOne,
              projectCreator:     (users.Name, Email) = names.generateOne -> emails.generateOne,
              commitId:           CommitId,
              committerName:      UserName = names.generateOne,
              committerEmail:     Email = emails.generateOne,
              schemaVersion:      SchemaVersion = schemaVersions.generateOne,
              renkuBaseUrl:       RenkuBaseUrl = renkuBaseUrl)(implicit fusekiBaseUrl: FusekiBaseUrl): List[Json] = {
      val filePath                                  = FilePath("README.md")
      val generationPath                            = FilePath("tree") / filePath
      val projectId                                 = Project.Id(renkuBaseUrl, projectPath)
      val (projectCreatorName, projectCreatorEmail) = projectCreator
      val projectCreatorId                          = Person.Id(projectCreatorName)
      val commitGenerationId                        = CommitGeneration.Id(commitId, generationPath)
      val commitEntityId                            = CommitEntity.Id(commitId, filePath)
      val commitActivityId                          = CommitActivity.Id(commitId)
      val committerPersonId                         = Person.Id(committerName)
      val agentId                                   = Agent.Id(schemaVersion)

      List(
        Project(projectId, projectName, projectDateCreated, projectCreatorId),
        Person(projectCreatorId, Some(projectCreatorEmail)),
        CommitEntity(commitEntityId, projectId, commitGenerationId),
        CommitActivity(commitActivityId,
                       projectId,
                       committedDates.generateOne,
                       agentId,
                       committerPersonId,
                       maybeInfluencedBy = List(commitEntityId)).hcursor
          .downField("schema:isPartOf")
          .delete
          .top
          .getOrElse(fail("CommitActivity Json cannot be created after removing isPartOf")),
        Person(committerPersonId, Some(committerEmail)),
        CommitGeneration(commitGenerationId, commitActivityId),
        Agent(agentId)
      )
    }
  }
}
