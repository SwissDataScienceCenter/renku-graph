package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, personalAccessTokens}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.PersonalAccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
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

    "return a list of project members if service responds with OK and a valid body - personal access token case" in new TestCase {
      forAll {
        (path:                 model.projects.Path,
         gitLabProjectMembers: List[GitLabProjectMember],
         accessToken:          PersonalAccessToken
        ) =>
          stubFor {
            get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
              .withHeader("PRIVATE-TOKEN", equalTo(accessToken.value))
              .willReturn(okJson(gitLabProjectMembers.asJson.noSpaces))
          }

          finder.findProjectMembers(path)(Some(accessToken)).unsafeRunSync() shouldBe gitLabProjectMembers
      }
    }

    "return no users when there's no project with the given path" in new TestCase {

      forAll {
        (path:                 model.projects.Path,
         gitLabProjectMembers: List[GitLabProjectMember],
         accessToken:          PersonalAccessToken
        ) =>
          stubFor {
            get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
              .withHeader("PRIVATE-TOKEN", equalTo(accessToken.value))
              .willReturn(notFound())
          }
          finder.findProjectMembers(path)(Some(accessToken)).unsafeRunSync() shouldBe Nil
      }
    }

  }

  private trait TestCase {

    val gitLabUrl = GitLabUrl(externalServiceBaseUrl)
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
    private implicit val timer: Timer[IO]        = IO.timer(global)

    val finder = new IOGitLabProjectMembersFinder(gitLabUrl, Throttler.noThrottling, TestLogger())
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
