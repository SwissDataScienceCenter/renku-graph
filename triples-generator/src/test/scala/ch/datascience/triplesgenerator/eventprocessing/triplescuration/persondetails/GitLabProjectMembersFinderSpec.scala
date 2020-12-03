package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, gitLabUrls, personalAccessTokens}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.PersonalAccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock.{equalTo, get, okJson, stubFor}
import io.circe.Encoder
import io.circe.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import PersonDetailsGenerators._
import ch.datascience.control.Throttler
import ch.datascience.interpreters.TestLogger
import io.circe.literal._

import scala.util.{Success, Try}

class GitLabProjectMembersFinderSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {
  "findProjectMembers" should {

    "return fetched project info if service responds with OK and a valid body - personal access token case" in new TestCase {
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

          val Success(actual) = projectMembersFinder.findProjectMembers(path)(Some(accessToken))
          actual shouldBe gitLabProjectMembers
      }
    }

  }

  private trait TestCase {
    val gitLabUrl                 = gitLabUrls.generateOne
    implicit val maybeAccessToken = accessTokens.generateOption

    val projectMembersFinder = GitLabProjectMembersFinder[Try](gitLabUrl, Throttler.noThrottling, TestLogger())
//    new GitLabProjectMembersFinder[IO] {
//      override def findProjectMembers(path: projects.Path)(implicit
//          maybeAccessToken:                 Option[AccessToken]
//      ): IO[List[GitLabProjectMember]] = ???
//    }
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
