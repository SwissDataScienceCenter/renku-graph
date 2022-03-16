package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock.{get, okJson, stubFor}
import eu.timepit.refined.auto._
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.{personGitLabIds, personNames, projectIds, projectPaths}
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.generators.EntitiesGenerators.projectMembersNoEmail
import io.renku.graph.model.{GitLabUrl, persons, projects}
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.Random

class ProjectEventsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with MockFactory
    with TinyTypeEncoders {

  "find" should {
    "return a list of events" in new TestCase {
      val event  = pushEvents.generateOne.forMember(member).forProject(project)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)
      `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

      finder.find(project, 1).value.unsafeRunSync() shouldBe (events.map(_.asNormalPushEvent),
                                                              PagingInfo(None, None)
      ).asRight
    }

    "take from commitTo and skip commitFrom if both commitIds exist on the event" in new TestCase {
      val commitFrom = commitIds.generateSome
      val commitTo   = commitIds.generateSome
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = commitFrom, maybeCommitTo = commitTo)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

      finder.find(project, 1).value.unsafeRunSync() shouldBe (events.map(_.asNormalPushEvent),
                                                              PagingInfo(None, None)
      ).asRight
    }

    "take commitFrom if commitTo doesn't exist on the event" in new TestCase {
      val commitFrom = commitIds.generateSome
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(maybeCommitFrom = commitFrom, maybeCommitTo = None)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      `/api/v4/projects/:id/events?action=pushed`(project.id) returning okJson(events.asJson.noSpaces)

      finder.find(project, 1).value.unsafeRunSync() shouldBe (events.map(_.asNormalPushEvent),
                                                              PagingInfo(None, None)
      ).asRight
    }

  }

  private trait TestCase {
    val project = Project(projectIds.generateOne, projectPaths.generateOne)
    val member  = projectMembersNoEmail.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    val gitLabUrl                   = GitLabUrl(externalServiceBaseUrl).apiV4
    private lazy val requestTimeout = 2 seconds
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val finder = new ProjectEventsFinderImpl[IO](gitLabUrl,
                                                 Throttler.noThrottling,
                                                 retryInterval = 100 millis,
                                                 maxRetries = 1,
                                                 requestTimeoutOverride = Some(requestTimeout)
    )

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
