/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{DateCreated, Path}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError._
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
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

    "update the wasDerivedFrom triple, recreate dateCreated and update the project creator" +
      "if the user exists in KG" in new TestCase {

        val gitLabId = userGitLabIds.generateOne

        val gitLabProject = given {
          gitLabProjects(event.project.path).generateOne.copy(
            maybeCreator = gitLabCreator(gitLabId = gitLabId).generateSome
          )
        }.existsInGitLab

        val newCreatorId = userResourceIds(None).generateOne
        given(
          creatorId = newCreatorId,
          forGitLabId = gitLabId
        ).existsInKG

        val wasDerivedFromUpdates = (updatesQueryCreator
          .updateWasDerivedFrom(_: Path, _: Option[Path]))
          .expects(gitLabProject.path, gitLabProject.maybeParentPath)
          .returningUpdates

        val creatorUpdates = (updatesQueryCreator
          .swapCreator(_: Path, _: users.ResourceId))
          .expects(gitLabProject.path, newCreatorId)
          .returningUpdates

        val recreateDateCreated = (updatesQueryCreator
          .recreateDateCreated(_: Path, _: DateCreated))
          .expects(gitLabProject.path, gitLabProject.dateCreated)
          .returningUpdates

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
          List(wasDerivedFromUpdates, creatorUpdates, recreateDateCreated).flatten
        )
      }

    "update the wasDerivedFrom triple, recreate dateCreated and update the project creator" +
      "if Gitlab project's creator has an email and the user does not exist in KG" in new TestCase {

        val emailInGitLab = userEmails.generateOne
        val gitLabProject = given {
          gitLabProjects(event.project.path).generateOne.copy(
            maybeCreator = gitLabCreator(maybeEmail = Some(emailInGitLab)).generateSome
          )
        }.existsInGitLab

        val wasDerivedFromUpdates = (updatesQueryCreator
          .updateWasDerivedFrom(_: Path, _: Option[Path]))
          .expects(gitLabProject.path, gitLabProject.maybeParentPath)
          .returningUpdates

        givenNoUser(forEmail = emailInGitLab).existsInKG

        val creatorUpdates = (updatesQueryCreator
          .addNewCreator(_: Path, _: Option[Email], _: Option[users.Name]))
          .expects(gitLabProject.path, Some(emailInGitLab), gitLabProject.maybeCreator.map(_.name))
          .returningUpdates

        val recreateDateCreated = (updatesQueryCreator
          .recreateDateCreated(_: Path, _: DateCreated))
          .expects(gitLabProject.path, gitLabProject.dateCreated)
          .returningUpdates

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
          List(wasDerivedFromUpdates, creatorUpdates, recreateDateCreated).flatten
        )
      }

    "update the wasDerivedFrom triple, recreate dateCreated and update the project creator" +
      "if Gitlab project's creator does not have an email" in new TestCase {

        val gitLabProject = given {
          gitLabProjects(event.project.path).generateOne.copy(
            maybeCreator = gitLabCreator(maybeEmail = None).generateSome
          )
        }.existsInGitLab

        val wasDerivedFromUpdates = (updatesQueryCreator
          .updateWasDerivedFrom(_: Path, _: Option[Path]))
          .expects(gitLabProject.path, gitLabProject.maybeParentPath)
          .returningUpdates

        val creatorUpdates = (updatesQueryCreator
          .addNewCreator(_: Path, _: Option[Email], _: Option[users.Name]))
          .expects(gitLabProject.path, None, gitLabProject.maybeCreator.map(_.name))
          .returningUpdates

        val recreateDateCreated = (updatesQueryCreator
          .recreateDateCreated(_: Path, _: DateCreated))
          .expects(gitLabProject.path, gitLabProject.dateCreated)
          .returningUpdates

        updatesCreator.create(event).generateUpdates().value.unsafeRunSync() shouldBe Right(
          List(wasDerivedFromUpdates, creatorUpdates, recreateDateCreated).flatten
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

      s"fail if finding Person with an email in the KG fails with ${exception.getClass}" in new TestCase {

        val emailInGitLab = userEmails.generateOne
        given {
          gitLabProjects(projectPaths.generateOne).generateOne.copy(
            maybeCreator = gitLabCreator(maybeEmail = Some(emailInGitLab)).generateSome
          )
        }.existsInGitLab

        (kgInfoFinder
          .findCreatorId(_: Email))
          .expects(emailInGitLab)
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

      s"return $CurationRecoverableError if finding Person with an email in the KG fails with ${exception.getClass.getSimpleName}" in new TestCase {

        val emailInGitLab = userEmails.generateOne
        given {
          gitLabProjects(projectPaths.generateOne).generateOne.copy(
            maybeCreator = gitLabCreator(maybeEmail = Some(emailInGitLab)).generateSome
          )
        }.existsInGitLab

        (kgInfoFinder
          .findCreatorId(_: Email))
          .expects(emailInGitLab)
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

    def givenNoUser(forEmail: Email) = new {
      lazy val existsInKG =
        (kgInfoFinder
          .findCreatorId(_: Email))
          .expects(forEmail)
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
