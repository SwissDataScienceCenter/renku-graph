package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import PersonDetailsGenerators._
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global

class GitLabProjectMembersFinderSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProjectMembers" should {

    "return a set of project members and users" in new TestCase {

      forAll { (gitLabProjectUsers: List[GitLabProjectMember], gitLabProjectMembers: List[GitLabProjectMember]) =>
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
            .willReturn(okJson(gitLabProjectUsers.asJson.noSpaces))
        }
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
            .willReturn(okJson(gitLabProjectMembers.asJson.noSpaces))
        }

        finder
          .findProjectMembers(path)
          .unsafeRunSync() shouldBe gitLabProjectUsers.toSet ++ gitLabProjectMembers.toSet
      }
    }

    "collect users from paged results" in new TestCase {
      val projectUsers   = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList
      val projectMembers = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList

      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
          .willReturn(okJson(List(projectUsers.head).asJson.noSpaces).withHeader("X-Next-Page", "2"))
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
          .willReturn(okJson(List(projectMembers.head).asJson.noSpaces).withHeader("X-Next-Page", "2"))
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/users?page=2")
          .willReturn(okJson(projectUsers.tail.asJson.noSpaces).withHeader("X-Next-Page", ""))
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/members?page=2")
          .willReturn(okJson(projectMembers.tail.asJson.noSpaces).withHeader("X-Next-Page", ""))
      }

      finder
        .findProjectMembers(path)
        .unsafeRunSync() shouldBe projectUsers.toSet ++ projectMembers.toSet
    }

    "return an empty list when service responds with NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
          .willReturn(notFound())
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
          .willReturn(notFound())
      }

      finder.findProjectMembers(path).unsafeRunSync() shouldBe Set.empty
    }
  }

  private trait TestCase {

    val path = projectPaths.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
    private implicit val timer: Timer[IO]        = IO.timer(global)

    private val gitLabUrl = GitLabUrl(externalServiceBaseUrl)
    val finder            = new IOGitLabProjectMembersFinder(gitLabUrl, Throttler.noThrottling, TestLogger())
  }

  private implicit val projectMemberEncoder: Encoder[GitLabProjectMember] = Encoder.instance[GitLabProjectMember] {
    member =>
      json"""{
              "id":        ${member.id.value},
              "username":  ${member.username.value},
              "name":      ${member.name.value}
             }"""
  }
}
