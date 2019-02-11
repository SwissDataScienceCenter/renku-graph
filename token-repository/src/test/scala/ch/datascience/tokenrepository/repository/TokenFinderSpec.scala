package ch.datascience.tokenrepository.repository

import cats.MonadError
import cats.data.OptionT
import cats.implicits._
import ch.datascience.clients.AccessToken
import ch.datascience.clients.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.ProjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class TokenFinderSpec extends WordSpec with MockFactory {

  "findToken" should {

    "return Personal Access Token if found in the db" in new TestCase {

      val encryptedToken = nonEmptyStrings().generateOne
      val tokenType: TokenType = TokenType.Personal

      (tokenInRepoFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT(context.pure(Option(encryptedToken -> tokenType))))

      tokenFinder.findToken(projectId) shouldBe OptionT(context.pure(Option(PersonalAccessToken(encryptedToken))))
    }

    "return OAuth Access Token if found in the db" in new TestCase {

      val encryptedToken = nonEmptyStrings().generateOne
      val tokenType: TokenType = TokenType.OAuth

      (tokenInRepoFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT(context.pure(Option(encryptedToken -> tokenType))))

      tokenFinder.findToken(projectId) shouldBe OptionT(context.pure(Option(OAuthAccessToken(encryptedToken))))
    }

    "return None if no token was found in the db" in new TestCase {

      (tokenInRepoFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.none[Try, (String, TokenType)])

      tokenFinder.findToken(projectId) shouldBe OptionT.none[Try, AccessToken]
    }

    "fail if finding token in the db fails" in new TestCase {

      val exception = exceptions.generateOne
      (tokenInRepoFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.liftF[Try, (String, TokenType)](context.raiseError(exception)))

      tokenFinder.findToken(projectId).value shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val projectId = projectIds.generateOne

    val tokenInRepoFinder = mock[TryTokenInRepoFinder]
    val tokenFinder       = new TokenFinder[Try](tokenInRepoFinder)
  }

  private class TryTokenInRepoFinder(transactorProvider: TransactorProvider[Try])
      extends TokenInRepoFinder[Try](transactorProvider)
}
