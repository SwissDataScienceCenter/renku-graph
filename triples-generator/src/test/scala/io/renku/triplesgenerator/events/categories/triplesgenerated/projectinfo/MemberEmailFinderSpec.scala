/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated
package projectinfo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.ints
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths, userEmails, userGitLabIds, userNames}
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{GitLabUrl, projects, users}
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.http4s.Status.{BadGateway, Forbidden, ServiceUnavailable, Unauthorized}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.Random

class MemberEmailFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with MockFactory
    with TinyTypeEncoders {

  "findEmail" should {

    "iterate over project's push events " +
      "until commit info for the commit found on the push event lists an author with the same name" in new TestCase {
        val event  = pushEvents.generateOne.forMember(member).forProject(project)
        val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

        `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

        val authorEmail = userEmails.generateOne
        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            (event.maybeCommitTo orElse event.maybeCommitFrom).getOrElse(fail("At least one commit expected on Event")),
            maybeAccessToken
          )
          .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

        finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
      }

    "do nothing if email is already set on the given member" in new TestCase {
      val memberWithEmail = member add userEmails.generateOne
      finder.findMemberEmail(memberWithEmail, project).value.unsafeRunSync() shouldBe memberWithEmail.asRight
    }

    "take the email from commitTo and skip commitFrom if both commitIds exist on the event" in new TestCase {
      val commitFrom = commitIds.generateOne
      val commitTo   = commitIds.generateOne
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = Some(commitFrom), maybeCommitTo = Some(commitTo))
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

      val authorEmail = userEmails.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(project.path, commitTo, maybeAccessToken)
        .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
    }

    "take the email from commitFrom if commitTo does not exist" in new TestCase {
      val commitFrom = commitIds.generateOne
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = Some(commitFrom), maybeCommitTo = None)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

      val authorEmail = userEmails.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(project.path, commitFrom, maybeAccessToken)
        .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
    }

    "find the email if a matching commit exists on the second page" in new TestCase {
      val event       = pushEvents.generateOne.forMember(member).forProject(project)
      val eventsPage2 = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id, maybeNextPage = 2.some) returning okJson(
        pushEvents.generateNonEmptyList().toList.asJson.noSpaces
      )
      `/api/v4/projects/:id/events?action=pushed`(project.id, page = 2) returning okJson(
        eventsPage2.asJson.noSpaces
      )

      val authorEmail = userEmails.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          (event.maybeCommitTo orElse event.maybeCommitFrom).getOrElse(fail("At least one commit expected on Event")),
          maybeAccessToken
        )
        .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
    }

    "select 30 pages from the total number of pages (including the first and the last one) " +
      "if the total number of pages for the project is more than 30" in new TestCase {
        val maybeTotalPages @ Some(totalPages) = ints(min = 30 * 20 + 1, max = 1000000).generateSome

        val step = totalPages / 30

        `/api/v4/projects/:id/events?action=pushed`(project.id,
                                                    maybeNextPage = 2.some,
                                                    maybeTotalPages = maybeTotalPages
        ) returning okJson(
          pushEvents.generateNonEmptyList(minElements = 20, maxElements = 20).toList.asJson.noSpaces
        )

        step to (totalPages - 1, step) foreach { page =>
          `/api/v4/projects/:id/events?action=pushed`(project.id,
                                                      page,
                                                      maybeNextPage = (page + 1).some,
                                                      maybeTotalPages = maybeTotalPages
          ) returning okJson(
            pushEvents.generateNonEmptyList(minElements = 20, maxElements = 20).toList.asJson.noSpaces
          )
        }

        val event = pushEvents.generateOne.forMember(member).forProject(project)
        `/api/v4/projects/:id/events?action=pushed`(project.id,
                                                    totalPages,
                                                    maybeNextPage = None,
                                                    maybeTotalPages = maybeTotalPages
        ) returning okJson(
          (event :: pushEvents.generateNonEmptyList(maxElements = 19).toList.reverse).asJson.noSpaces
        )

        val authorEmail = userEmails.generateOne
        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            (event.maybeCommitTo orElse event.maybeCommitFrom).getOrElse(fail("At least one commit expected on Event")),
            maybeAccessToken
          )
          .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

        finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
      }

    "find the email if a matching commits exist on both pages, " +
      "however, the first matching commit has author with a different name" in new TestCase {
        val eventPage1 = pushEvents.generateOne
          .forMember(member)
          .forProject(project)
          .copy(maybeCommitFrom = commitIds.generateSome, maybeCommitTo = None)
        val eventsPage1 = Random.shuffle(eventPage1 :: pushEvents.generateNonEmptyList().toList)
        val eventPage2 = pushEvents.generateOne
          .forMember(member)
          .forProject(project)
          .copy(maybeCommitFrom = commitIds.generateSome, maybeCommitTo = None)
        val eventsPage2 = Random.shuffle(eventPage2 :: pushEvents.generateNonEmptyList().toList)

        `/api/v4/projects/:id/events?action=pushed`(project.id, maybeNextPage = 2.some) returning okJson(
          eventsPage1.asJson.noSpaces
        )
        `/api/v4/projects/:id/events?action=pushed`(project.id, page = 2) returning okJson(
          eventsPage2.asJson.noSpaces
        )

        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            eventPage1.maybeCommitFrom.getOrElse(fail("CommitFrom expected on Event")),
            maybeAccessToken
          )
          .returning(
            EitherT.rightT[IO, ProcessingRecoverableError]((userNames.generateOne -> userEmails.generateOne).some)
          )
        val authorEmail = userEmails.generateOne
        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            eventPage2.maybeCommitFrom.getOrElse(fail("CommitFrom expected on Event")),
            maybeAccessToken
          )
          .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

        finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
      }

    "return the given member back if a matching commit author cannot be found on any events" in new TestCase {
      val eventPage1 = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = commitIds.generateSome, maybeCommitTo = None)
      val eventsPage1 = Random.shuffle(eventPage1 :: pushEvents.generateNonEmptyList().toList)
      val eventPage2 = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = commitIds.generateSome, maybeCommitTo = None)
      val eventsPage2 = Random.shuffle(eventPage2 :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id, maybeNextPage = 2.some) returning okJson(
        eventsPage1.asJson.noSpaces
      )
      `/api/v4/projects/:id/events?action=pushed`(project.id, page = 2) returning okJson(
        eventsPage2.asJson.noSpaces
      )

      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          eventPage1.maybeCommitFrom.getOrElse(fail("CommitFrom expected on Event")),
          maybeAccessToken
        )
        .returning(
          EitherT.rightT[IO, ProcessingRecoverableError]((userNames.generateOne -> userEmails.generateOne).some)
        )
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          eventPage2.maybeCommitFrom.getOrElse(fail("CommitFrom expected on Event")),
          maybeAccessToken
        )
        .returning(
          EitherT.rightT[IO, ProcessingRecoverableError]((userNames.generateOne -> userEmails.generateOne).some)
        )

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe member.asRight
    }

    "return the given member back if no project events with the member as an author are found for the project" in new TestCase {

      `/api/v4/projects/:id/events?action=pushed`(project.id, maybeNextPage = 2.some) returning okJson(
        pushEvents.generateNonEmptyList().toList.asJson.noSpaces
      )
      `/api/v4/projects/:id/events?action=pushed`(project.id, page = 2) returning okJson(
        pushEvents.generateNonEmptyList().toList.asJson.noSpaces
      )

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe member.asRight
    }

    "return the given member back if no project events are found" in new TestCase {
      `/api/v4/projects/:id/events?action=pushed`(project.id, maybeNextPage = 2.some) returning notFound()

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe member.asRight
    }

    Set(
      "connection problem" -> aResponse().withFault(CONNECTION_RESET_BY_PEER),
      "client problem"     -> aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt),
      "BadGateway"         -> aResponse().withStatus(BadGateway.code),
      "ServiceUnavailable" -> aResponse().withStatus(ServiceUnavailable.code),
      "Forbidden"          -> aResponse().withStatus(Forbidden.code),
      "Unauthorized"       -> aResponse().withStatus(Unauthorized.code)
    ) foreach { case (problemName, response) =>
      s"return a Recoverable Failure for $problemName when fetching member's events" in new TestCase {
        `/api/v4/projects/:id/events?action=pushed`(project.id, maybeNextPage = 2.some) returning response

        val Left(failure) = finder.findMemberEmail(member, project).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }
    }

    s"return a Recoverable Failure when returned when fetching commit info" in new TestCase {
      val event  = pushEvents.generateOne.forMember(member).forProject(project)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

      val error = processingRecoverableErrors.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          (event.maybeCommitTo orElse event.maybeCommitFrom).getOrElse(fail("At least one commit expected on Event")),
          maybeAccessToken
        )
        .returning(EitherT.leftT[IO, Option[(users.Name, users.Email)]](error))

      val Left(failure) = finder.findMemberEmail(member, project).value.unsafeRunSync()
      failure shouldBe a[ProcessingRecoverableError]
    }
  }

  private lazy val requestTimeout = 2 seconds

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val project = Project(projectIds.generateOne, projectPaths.generateOne)
    val member  = projectMembersNoEmail.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl          = GitLabUrl(externalServiceBaseUrl).apiV4
    val commitAuthorFinder = mock[CommitAuthorFinder[IO]]
    val finder = new MemberEmailFinderImpl[IO](commitAuthorFinder,
                                               gitLabUrl,
                                               Throttler.noThrottling,
                                               retryInterval = 100 millis,
                                               maxRetries = 1,
                                               requestTimeoutOverride = Some(requestTimeout)
    )
  }

  private implicit lazy val pushEventEncoder: Encoder[PushEvent] = Encoder.instance { event =>
    json"""{
      "project_id": ${event.projectId},     
      "author": {
        "id":   ${event.authorId},
        "name": ${event.authorName}
      },
      "push_data": {
        "commit_from": ${event.maybeCommitFrom.map(_.asJson).getOrElse(Json.Null)},
        "commit_to":   ${event.maybeCommitTo.map(_.asJson).getOrElse(Json.Null)}
      }
    }"""
  }

  private def `/api/v4/projects/:id/events?action=pushed`(projectId:       projects.Id,
                                                          page:            Int = 1,
                                                          maybeNextPage:   Option[Int] = None,
                                                          maybeTotalPages: Option[Int] = None
  )(implicit maybeAccessToken:                                             Option[AccessToken]) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/$projectId/events?action=pushed&page=$page")
        .withAccessToken(maybeAccessToken)
        .willReturn(
          response
            .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
            .withHeader("X-Total-Pages", maybeTotalPages.map(_.show).getOrElse(maybeNextPage.map(_.show).getOrElse("")))
        )
    }
  }

  private case class PushEvent(projectId:       projects.Id,
                               maybeCommitFrom: Option[CommitId],
                               maybeCommitTo:   Option[CommitId],
                               authorId:        users.GitLabId,
                               authorName:      users.Name
  ) {
    def forMember(member: ProjectMember): PushEvent =
      copy(authorId = member.gitLabId, authorName = member.name)

    def forProject(project: Project): PushEvent = copy(projectId = project.id)
  }

  private lazy val pushEvents: Gen[PushEvent] = for {
    projectId <- projectIds
    commitIds <- commitIds.toGeneratorOfSet(minElements = 2, maxElements = 2)
    userId    <- userGitLabIds
    userName  <- userNames
  } yield PushEvent(projectId, commitIds.headOption, commitIds.tail.headOption, userId, userName)
}
