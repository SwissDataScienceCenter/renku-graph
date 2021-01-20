/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.projects

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{DateCreated, Path, Visibility}
import ch.datascience.graph.model.users
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError._
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.EventProcessingGenerators._
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.IOTriplesCurator.CurationRecoverableError
import eu.timepit.refined.auto._
import org.scalamock.handlers.CallHandler
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class UpdatesCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "create" should {

    "do nothing if no project in Gitlab" in new TestCase {

      given(
        gitLabProjects(projectPath = event.project.path, maybeParentPaths = emptyOptionOf[Path]).generateOne
      ).doesNotExistsInGitLab

      updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
        List.empty[SparqlQuery]
      )
    }

    "update the wasDerivedFrom triple, recreate dateCreated and update the project creator & visibility" +
      "if user with GitLabId exists in KG" in new TestCase {

        val gitLabId = userGitLabIds.generateOne

        val gitLabProject = given {
          gitLabProjects(event.project.path).generateOne.copy(
            maybeCreator = gitLabCreator(gitLabId).generateSome
          )
        }.existsInGitLab

        val creatorResourceId = userResourceIds(userEmails.generateOption).generateOne
        given(
          creatorId = creatorResourceId,
          forGitLabId = gitLabId
        ).existsInKG

        val wasDerivedFromUpdates = (updatesQueryCreator
          .updateWasDerivedFrom(_: Path, _: Option[Path]))
          .expects(gitLabProject.path, gitLabProject.maybeParentPath)
          .returningUpdates

        val creatorUpdates = (updatesQueryCreator
          .swapCreator(_: Path, _: users.ResourceId))
          .expects(gitLabProject.path, creatorResourceId)
          .returningUpdates

        val recreateDateCreated = (updatesQueryCreator
          .updateDateCreated(_: Path, _: DateCreated))
          .expects(gitLabProject.path, gitLabProject.dateCreated)
          .returningUpdates

        val visibilityUpsert = (updatesQueryCreator
          .upsertVisibility(_: Path, _: Visibility))
          .expects(gitLabProject.path, gitLabProject.visibility)
          .returningUpdates

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
          List(wasDerivedFromUpdates, creatorUpdates, recreateDateCreated, visibilityUpsert).flatten
        )
      }

    "update the wasDerivedFrom triple, recreate dateCreated, create a new creator and update the project creator & visibility" +
      "if Gitlab project's creator has an email and the user does not exist in KG" in new TestCase {

        val gitLabId = userGitLabIds.generateOne
        val creator  = gitLabCreator(gitLabId).generateOne
        val gitLabProject = given {
          gitLabProjects(event.project.path).generateOne.copy(maybeCreator = Some(creator))
        }.existsInGitLab

        givenNoUser(forGitLabId = gitLabId).existsInKG

        val wasDerivedFromUpdates = (updatesQueryCreator
          .updateWasDerivedFrom(_: Path, _: Option[Path]))
          .expects(gitLabProject.path, gitLabProject.maybeParentPath)
          .returningUpdates

        val recreateDateCreated = (updatesQueryCreator
          .updateDateCreated(_: Path, _: DateCreated))
          .expects(gitLabProject.path, gitLabProject.dateCreated)
          .returningUpdates

        val creatorUpdates = (updatesQueryCreator
          .addNewCreator(_: Path, _: GitLabCreator))
          .expects(gitLabProject.path, creator)
          .returningUpdates

        val visibilityUpsert = (updatesQueryCreator
          .upsertVisibility(_: Path, _: Visibility))
          .expects(gitLabProject.path, gitLabProject.visibility)
          .returningUpdates

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
          List(wasDerivedFromUpdates, creatorUpdates, recreateDateCreated, visibilityUpsert).flatten
        )
      }

    "update the wasDerivedFrom triple, upsert dateCreated & visibility and remove project creator" +
      "if Gitlab project does not have creator" in new TestCase {

        val gitLabProject = given {
          gitLabProjects(event.project.path).generateOne.copy(maybeCreator = None)
        }.existsInGitLab

        val wasDerivedFromUpdates = (updatesQueryCreator
          .updateWasDerivedFrom(_: Path, _: Option[Path]))
          .expects(gitLabProject.path, gitLabProject.maybeParentPath)
          .returningUpdates

        val recreateDateCreated = (updatesQueryCreator
          .updateDateCreated(_: Path, _: DateCreated))
          .expects(gitLabProject.path, gitLabProject.dateCreated)
          .returningUpdates

        val unlinkCreatorUpdates = (updatesQueryCreator
          .unlinkCreator(_: Path))
          .expects(gitLabProject.path)
          .returningUpdates

        val visibilityUpsert = (updatesQueryCreator
          .upsertVisibility(_: Path, _: Visibility))
          .expects(gitLabProject.path, gitLabProject.visibility)
          .returningUpdates

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
          List(wasDerivedFromUpdates, unlinkCreatorUpdates, recreateDateCreated, visibilityUpsert).flatten
        )
      }

    Set(
      BadRequestException(nonBlankStrings().generateOne),
      MappingException(nonBlankStrings().generateOne, exceptions.generateOne),
      UnauthorizedException
    ) foreach { exception =>
      s"fail if finding GitLab project fails with ${exception.getClass}" in new TestCase {

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

        intercept[Exception] {
          updatesCreator.create(event).generateUpdates().value.unsafeRunSync()
        } shouldBe exception
      }

      s"fail if finding Person with a GitLabId in the KG fails with ${exception.getClass}" in new TestCase {

        val gitLabId = userGitLabIds.generateOne
        given {
          gitLabProjects(projectPaths.generateOne).generateOne.copy(
            maybeCreator = gitLabCreator(gitLabId).generateSome
          )
        }.existsInGitLab

        (kgInfoFinder
          .findCreatorId(_: users.GitLabId))
          .expects(gitLabId)
          .returning(exception.raiseError[IO, Option[users.ResourceId]])

        intercept[Exception] {
          updatesCreator.create(event).generateUpdates().value.unsafeRunSync()
        } shouldBe exception
      }
    }

    Set(
      UnexpectedResponseException(nonBlankStrings().generateOne),
      ConnectivityException(nonBlankStrings().generateOne, exceptions.generateOne)
    ) foreach { exception =>
      s"return $CurationRecoverableError if finding GitLab project fails with ${exception.getClass.getSimpleName}" in new TestCase {

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Left {
          CurationRecoverableError("Problem with finding fork info", exception)
        }
      }

      s"return $CurationRecoverableError if finding Person with GitLabId in the KG fails with ${exception.getClass.getSimpleName}" in new TestCase {

        val gitLabId = userGitLabIds.generateOne
        given {
          gitLabProjects(projectPaths.generateOne).generateOne.copy(
            maybeCreator = gitLabCreator(gitLabId).generateSome
          )
        }.existsInGitLab

        (kgInfoFinder
          .findCreatorId(_: users.GitLabId))
          .expects(gitLabId)
          .returning(exception.raiseError[IO, Option[users.ResourceId]])

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Left {
          CurationRecoverableError("Problem with finding fork info", exception)
        }
      }
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val event = commitEvents.generateOne

    val gitLabInfoFinder    = mock[GitLabInfoFinder[IO]]
    val kgInfoFinder        = mock[KGInfoFinder[IO]]
    val updatesQueryCreator = mock[UpdatesQueryCreator]
    val updatesCreator      = new UpdatesCreatorImpl(gitLabInfoFinder, kgInfoFinder, updatesQueryCreator)

    def given(gitLabProject: GitLabProject) = new {
      lazy val existsInGitLab: GitLabProject = {
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option(gitLabProject).pure[IO])
        gitLabProject
      }

      lazy val doesNotExistsInGitLab =
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option.empty.pure[IO])
    }

    def given(creatorId: users.ResourceId, forGitLabId: users.GitLabId) = new {
      lazy val existsInKG =
        (kgInfoFinder
          .findCreatorId(_: users.GitLabId))
          .expects(forGitLabId)
          .returning(creatorId.some.pure[IO])
    }

    def givenNoUser(forGitLabId: users.GitLabId) = new {
      lazy val existsInKG =
        (kgInfoFinder
          .findCreatorId(_: users.GitLabId))
          .expects(forGitLabId)
          .returning(Option.empty.pure[IO])
    }

    implicit class CallHandlerOps(handler: CallHandler[List[SparqlQuery]]) {
      private val updates = listOf(sparqlQueries, maxElements = 3).generateOne

      lazy val returningUpdates: List[SparqlQuery] = {
        handler.returning(updates)
        updates
      }
    }
  }
}
