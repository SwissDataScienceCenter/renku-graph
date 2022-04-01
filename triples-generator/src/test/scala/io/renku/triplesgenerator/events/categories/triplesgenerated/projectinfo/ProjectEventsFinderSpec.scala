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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import org.http4s.{Header, Headers}
import org.typelevel.ci.CIStringSyntax
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.{personGitLabIds, personNames, projectIds, projectPaths}
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.generators.EntitiesGenerators.projectMembersNoEmail
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class ProjectEventsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with MockFactory
    with TinyTypeEncoders
    with GitLabClientTools[IO] {

  "find" should {
    "return a list of events for a given page" in new TestCase {
      val event  = pushEvents.generateOne.forMember(member).forProject(project)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)
      val endpointName: String Refined NonEmpty = "events"
      val page        = 1
      val expectation = (events.map(_.asNormalPushEvent), PagingInfo(None, None))

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, (List[PushEvent], PagingInfo)]
        )(_: Option[AccessToken]))
        .expects(
          GET,
          uri"projects" / project.id.show / "events" withQueryParams Map("action" -> "pushed", "page" -> page.toString),
          endpointName,
          *,
          maybeAccessToken
        )
        .returning(expectation.pure[IO])

      finder.find(project, page).value.unsafeRunSync() shouldBe (events.map(_.asNormalPushEvent),
                                                                 PagingInfo(None, None)
      ).asRight
    }

    // mapResponse Tests

    "take from commitTo and skip commitFrom if both commitIds exist on the event" in new TestCase {
      val commitFrom = commitIds.generateSome
      val commitTo   = commitIds.generateSome
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = commitFrom, maybeCommitTo = commitTo)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity(events.asJson.noSpaces))
        .unsafeRunSync() shouldBe (events.map(
        _.asNormalPushEvent
      ),
      PagingInfo(None, None))
    }

    "take commitFrom if commitTo doesn't exist on the event" in new TestCase {
      val commitFrom = commitIds.generateSome
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = commitFrom, maybeCommitTo = None)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity(events.asJson.noSpaces))
        .unsafeRunSync() shouldBe (events.map(
        _.asNormalPushEvent
      ),
      PagingInfo(None, None))
    }

    "parse the response headers for 'next page' and 'total pages'" in new TestCase {
      val events     = pushEvents.generateNonEmptyList().toList
      val nextPage   = nonNegativeInts().generateOne.value
      val totalPages = nonNegativeInts().generateOne.value
      val headers = Headers(
        List(Header.Raw(ci"X-Next-Page", nextPage.toString()), Header.Raw(ci"X-Total-Pages", totalPages.toString()))
      )

      mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity(events.asJson.noSpaces).withHeaders(headers))
        .unsafeRunSync() shouldBe (events.map(_.asNormalPushEvent), PagingInfo(nextPage.some, totalPages.some))
    }

    "return an empty List if project NotFound" in new TestCase {
      mapResponse(Status.NotFound, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe
        (List.empty[PushEvent], PagingInfo(None, None))
    }

    "fail the interpretation if response is Unauthorized" in new TestCase {

      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe
          (None, PagingInfo(None, None))
      }
    }
  }

  private trait TestCase {

    val project = Project(projectIds.generateOne, projectPaths.generateOne)
    val member  = projectMembersNoEmail.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient = mock[GitLabClient[IO]]
    val finder       = new ProjectEventsFinderImpl[IO](gitLabClient)

    val mapResponse =
      captureMapping(finder, gitLabClient)(
        _.find(Project(projectIds.generateOne, projectPaths.generateOne), nonNegativeInts().generateOne),
        Gen.const(EitherT(IO(Option.empty[(persons.Name, persons.Email)].asRight[ProcessingRecoverableError])))
      )
  }

  private implicit lazy val pushEventEncoder: Encoder[GitLabPushEvent] = Encoder.instance { event =>
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

  private case class GitLabPushEvent(projectId:       projects.Id,
                                     maybeCommitFrom: Option[CommitId],
                                     maybeCommitTo:   Option[CommitId],
                                     authorId:        persons.GitLabId,
                                     authorName:      persons.Name
  ) {
    def forMember(member: ProjectMember): GitLabPushEvent =
      copy(authorId = member.gitLabId, authorName = member.name)

    def forProject(project: Project): GitLabPushEvent = copy(projectId = project.id)

    def asNormalPushEvent: PushEvent =
      PushEvent(projectId, maybeCommitTo.orElse(maybeCommitFrom).get, authorId, authorName)
  }

  private lazy val pushEvents: Gen[GitLabPushEvent] = for {
    projectId <- projectIds
    commitIds <- commitIds.toGeneratorOfSet(minElements = 2, maxElements = 2)
    userId    <- personGitLabIds
    userName  <- personNames
  } yield GitLabPushEvent(projectId, commitIds.headOption, commitIds.tail.headOption, userId, userName)
}
