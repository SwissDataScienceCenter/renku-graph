package io.renku.tokenrepository.repository
package refresh

import association.TokenDates.ExpiryDate
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Uri
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class ExpiredTokensFinderSpec
    extends AnyWordSpec
    with MockFactory
    with GitLabClientTools[IO]
    with IOSpec
    with should.Matchers {

  "findExpiredTokens" should {

    "do GET projects/:id/access_tokens" in new TestCase {
      val endpointName: String Refined NonEmpty = "create-project-access-token"

      val allTokens = List(
        tokenInfos(),
        tokenInfos(fixed(renkuTokenName), localDates(max = LocalDate.now().plusDays(1)).generateAs(ExpiryDate)),
        tokenInfos(fixed(renkuTokenName), fixed(ExpiryDate(LocalDate.now()))),
        tokenInfos(fixed(renkuTokenName), localDates(min = LocalDate.now().plusDays(1)).generateAs(ExpiryDate))
      ).map(_.generateOne)

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, List[TokenInfo]])(_: Option[AccessToken]))
        .expects(uri"projects" / projectId.value / "access_tokens", endpointName, *, Option(accessToken))
        .returning(allTokens.pure[IO])

      finder.findExpiredTokens(projectId, accessToken).unsafeRunSync() shouldBe allTokens
        .filter(_._2 == renkuTokenName)
        .filter(info => (info._3.value compareTo LocalDate.now()) <= 0)
        .map(_._1)
    }
  }

  private type TokenInfo = (TokenId, String, ExpiryDate)

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new ExpiredTokensFinderImpl[IO]
  }

  private def tokenInfos(names:       Gen[String] = nonEmptyStrings(),
                         expiryDates: Gen[ExpiryDate] = localDates.toGeneratorOf(ExpiryDate)
  ): Gen[TokenInfo] =
    (positiveInts().toGeneratorOf(TokenId), names, expiryDates)
      .mapN { case (id, name, expiry) => (id, name, expiry) }

  private implicit lazy val tokenInfoEncoder: Encoder[TokenInfo] = Encoder.instance { case (id, name, expiry) =>
    json"""{
      "id":         ${id.value},
      "name":       $name,
      "expires_at": ${expiry.value}
    }"""
  }
}
