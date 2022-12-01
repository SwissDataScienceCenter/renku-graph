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

package io.renku.tokenrepository.repository
package refresh

import AccessTokenCrypto.EncryptedAccessToken
import RepositoryGenerators.encryptedAccessTokens
import association.Generators._
import association.TokenStoringInfo.Project
import association._
import cats.data.OptionT
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.projectAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info, Warn}
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.deletion.TokenRemover
import org.scalacheck.Gen
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.duration._
import scala.language.reflectiveCalls

class TokensRefresherSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with Eventually
    with IntegrationPatience {

  "run" should {

    "start a process that recreates all tokens with expiry_date due in one day" in new TestCase {

      val event1 = events.generateOne
      givenSuccessfulRegenerationFor(event1)
      givenEventFinding(returning = event1)
      givenSuccessfulOldTokensCleanUp(event1)

      val event2 = events.generateOne
      givenSuccessfulRegenerationFor(event2)
      givenEventFinding(returning = event2)
      givenSuccessfulOldTokensCleanUp(event2)

      refresher.run().unsafeRunAndForget()

      eventually {
        verifyTokenStored(event1.project)
        verifyExpiredTokensRevoked(event1.project)

        verifyTokenStored(event2.project)
        verifyExpiredTokensRevoked(event1.project)

        logger.loggedOnly(
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix ${event1.project} token recreated"),
          Info(show"$logPrefix ${event2.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh")
        )
      }
    }

    "start the process that is rerun infinitely with the configured cadence" in new TestCase {

      val event1 = events.generateOne
      givenSuccessfulRegenerationFor(event1)
      givenEventFinding(returning = event1)

      refresher.run().unsafeRunAndForget()

      eventually {
        verifyTokenStored(event1.project)
      }

      sleep(checkInterval.toMillis + 100)

      val event2 = events.generateOne
      givenSuccessfulRegenerationFor(event2)
      givenEventFinding(returning = event2)

      eventually {
        verifyTokenStored(event2.project)

        logger.loggedOnly(
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix ${event1.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh"),
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix no more tokens to refresh"),
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix ${event2.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh")
        )
      }
    }

    "delete expiring token if it is invalid" in new TestCase {

      val event1      = events.generateOne
      val event1Token = projectAccessTokens.generateOne
      givenDecryption(event1.encryptedToken, returning = event1Token)
      givenTokenValidation(event1Token, returning = false)
      givenSuccessfulTokenDeletion(event1.project)
      givenEventFinding(returning = event1)

      val event2 = events.generateOne
      givenSuccessfulRegenerationFor(event2)
      givenEventFinding(returning = event2)
      givenSuccessfulOldTokensCleanUp(event2)

      refresher.run().unsafeRunAndForget()

      eventually {
        verifyTokenDeleted(event1.project)
        verifyTokenStored(event2.project)
      }

      val event3 = events.generateOne
      givenSuccessfulRegenerationFor(event3)
      givenEventFinding(returning = event3)

      eventually {
        verifyTokenStored(event3.project)

        logger.loggedOnly(
          Info(show"$logPrefix looking for tokens to refresh"),
          Warn(show"$logPrefix ${event1.project} token invalid; deleting"),
          Info(show"$logPrefix ${event2.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh"),
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix ${event3.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh")
        )
      }
    }

    "log an exception and continue in case of a failure on event fetching" in new TestCase {

      val exception = exceptions.generateOne
      givenEventFinding(throwing = exception)

      val event1 = events.generateOne
      givenSuccessfulRegenerationFor(event1)
      givenEventFinding(returning = event1)

      refresher.run().unsafeRunAndForget()

      eventually {
        verifyTokenStored(event1.project)
      }

      val event2 = events.generateOne
      givenSuccessfulRegenerationFor(event2)
      givenEventFinding(returning = event2)

      eventually {
        verifyTokenStored(event2.project)

        logger.loggedOnly(
          Info(show"$logPrefix looking for tokens to refresh"),
          Error(show"$logPrefix processing failure", exception),
          Info(show"$logPrefix ${event1.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh"),
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix ${event2.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh")
        )
      }
    }

    "log an exception and continue in case of a failure on some step" in new TestCase {

      val event1      = events.generateOne
      val event1Token = projectAccessTokens.generateOne
      givenDecryption(event1.encryptedToken, returning = event1Token)

      val exception = exceptions.generateOne
      givenTokenValidation(event1Token, throwing = exception)

      givenEventFinding(returning = event1)

      val event2 = events.generateOne
      givenSuccessfulRegenerationFor(event2)
      givenEventFinding(returning = event2)

      refresher.run().unsafeRunAndForget()

      eventually {
        verifyTokenStored(event2.project)
      }

      val event3 = events.generateOne
      givenSuccessfulRegenerationFor(event3)
      givenEventFinding(returning = event3)

      eventually {
        verifyTokenStored(event3.project)

        logger.loggedOnly(
          Info(show"$logPrefix looking for tokens to refresh"),
          Error(show"$logPrefix failure at 'validation' for ${event1.project}", exception),
          Info(show"$logPrefix ${event2.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh"),
          Info(show"$logPrefix looking for tokens to refresh"),
          Info(show"$logPrefix ${event3.project} token recreated"),
          Info(show"$logPrefix no more tokens to refresh")
        )
      }
    }
  }

  private trait TestCase {

    val logPrefix     = "project token refresh:"
    val checkInterval = 500 millis

    val eventFinder = new EventsFinder[IO] {
      val eventsQueue = Queue.unbounded[IO, IO[TokenCloseExpiration]].unsafeRunSync()

      override def findEvent() = eventsQueue.tryTake.flatMap(_.sequence)
    }

    val tokenCrypto = new AccessTokenCrypto[IO] {
      val state = Ref.unsafe[IO, Set[(AccessToken, EncryptedAccessToken)]](Set.empty)

      override def encrypt(token: AccessToken): IO[EncryptedAccessToken] = OptionT {
        state.get.map(_.collectFirst { case (t, encT) if t == token => encT })
      }.getOrElseF(new Exception(s"Stub not prepared to encrypt $token").raiseError[IO, EncryptedAccessToken])

      override def decrypt(encryptedToken: EncryptedAccessToken): IO[AccessToken] = OptionT {
        state.get.map(_.collectFirst { case (t, encT) if encT == encryptedToken => t })
      }.getOrElseF(new Exception(s"Stub not prepared to decrypt $encryptedToken").raiseError[IO, AccessToken])
    }

    val tokenValidator = new TokenValidator[IO] {
      val state = Ref.unsafe[IO, Map[AccessToken, IO[Boolean]]](Map.empty)

      override def checkValid(token: AccessToken) = state.get.flatMap(_.getOrElse(token, false.pure[IO]))
    }

    val tokenRemover = new TokenRemover[IO] {
      val state = Ref.unsafe[IO, Set[projects.Id]](Set.empty)

      override def delete(projectId: projects.Id) = OptionT {
        state.get.map(_.find(_ == projectId))
      }.cataF(new Exception(show"Token deletion for $projectId not expected").raiseError[IO, Unit],
              id => state.update(_ - id)
      )
    }

    val tokenCreator = new ProjectAccessTokenCreator[IO] {
      val state = Ref.unsafe[IO, Map[projects.Id, TokenCreationInfo]](Map.empty)

      override def createPersonalAccessToken(projectId: projects.Id, accessToken: AccessToken) =
        state.get.map(_.get(projectId))
    }

    val associationPersister = new AssociationPersister[IO] {
      val state = Ref.unsafe[IO, Set[TokenStoringInfo]](Set.empty)

      override def persistAssociation(storingInfo: TokenStoringInfo) = OptionT {
        state.get.map(_.find(_ == storingInfo))
      }.cataF(new Exception(show"Storing token for ${storingInfo.project.id} not expected").raiseError[IO, Unit],
              info => state.update(_ - info)
      )

      override def updatePath(project: Project) = ().pure[IO]
    }

    val expiredTokensFinder = new ExpiredTokensFinder[IO] {
      val state = Ref.unsafe[IO, Map[projects.Id, List[AccessTokenId]]](Map.empty)

      override def findExpiredTokens(projectId: projects.Id, accessToken: AccessToken): IO[List[AccessTokenId]] =
        state.modify { map =>
          map.get(projectId).map((map - projectId) -> _).getOrElse(map -> Nil)
        }
    }

    val expiredTokensRevoker = new ExpiredTokensRevoker[IO] {
      val state = Ref.unsafe[IO, List[(projects.Id, AccessTokenId)]](List.empty)

      override def revokeToken(projectId: projects.Id, tokenId: AccessTokenId, accessToken: AccessToken) =
        state.modify(_.filterNot(_ == (projectId -> tokenId)) -> ())
    }

    implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
    val refresher = new TokensRefresherImpl[IO](eventFinder,
                                                tokenCrypto,
                                                tokenValidator,
                                                tokenRemover,
                                                tokenCreator,
                                                associationPersister,
                                                expiredTokensFinder,
                                                expiredTokensRevoker,
                                                checkInterval
    )

    def givenSuccessfulRegenerationFor(event: TokenCloseExpiration): Unit = {
      val eventToken = projectAccessTokens.generateOne
      givenDecryption(event.encryptedToken, returning = eventToken)

      givenTokenValidation(eventToken, returning = true)

      val newTokenInfo = tokenCreationInfos.generateOne
      givenTokenCreation(event.project, returning = newTokenInfo)

      val newTokenEnc = encryptedAccessTokens.generateOne
      givenEncryption(newTokenInfo.token, returning = newTokenEnc)

      val newTokenStoringInfo = TokenStoringInfo(event.project, newTokenEnc, newTokenInfo.dates)
      givenTokenStoring(newTokenStoringInfo)
    }

    def givenSuccessfulOldTokensCleanUp(event: TokenCloseExpiration): Unit = {

      val tokenIds = accessTokenIds.generateList()
      givenExpiredTokens(event.project, tokenIds)

      tokenIds foreach (givenExpiredTokensRevoke(event.project, _))
    }

    def givenEventFinding(returning: TokenCloseExpiration): Unit =
      (eventFinder.eventsQueue offer returning.pure[IO]).unsafeRunSync()

    def givenEventFinding(throwing: Exception): Unit =
      (eventFinder.eventsQueue offer throwing.raiseError[IO, TokenCloseExpiration]).unsafeRunSync()

    def givenDecryption(encToken: EncryptedAccessToken, returning: AccessToken): Unit =
      tokenCrypto.state.update(_ + (returning -> encToken)).unsafeRunSync()

    def givenEncryption(token: AccessToken, returning: EncryptedAccessToken): Unit =
      tokenCrypto.state.update(_ + (token -> returning)).unsafeRunSync()

    def givenTokenValidation(token: ProjectAccessToken, returning: Boolean): Unit =
      tokenValidator.state.update(_ + (token -> returning.pure[IO])).unsafeRunSync()

    def givenTokenValidation(token: ProjectAccessToken, throwing: Throwable): Unit =
      tokenValidator.state.update(_ + (token -> throwing.raiseError[IO, Boolean])).unsafeRunSync()

    def givenSuccessfulTokenDeletion(project: Project): Unit =
      tokenRemover.state.update(_ + project.id).unsafeRunSync()

    def givenTokenCreation(project: Project, returning: TokenCreationInfo): Unit =
      tokenCreator.state.update(_ + (project.id -> returning)).unsafeRunSync()

    def givenTokenStoring(storingInfo: TokenStoringInfo): Unit =
      associationPersister.state.update(_ + storingInfo).unsafeRunSync()

    def givenExpiredTokens(project: Project, tokenIds: List[AccessTokenId]): Unit =
      expiredTokensFinder.state.update(_ + (project.id -> tokenIds)).unsafeRunSync()

    def givenExpiredTokensRevoke(project: Project, tokenId: AccessTokenId): Unit =
      expiredTokensRevoker.state.update((project.id -> tokenId) :: _).unsafeRunSync()

    def verifyTokenDeleted(project: Project): Unit =
      tokenRemover.state.get
        .map(_.find(_ == project.id))
        .unsafeRunSync() match {
        case Some(projectId) => fail(show"Token for project $projectId hasn't been removed")
        case None            => ()
      }

    def verifyTokenStored(project: Project): Unit =
      associationPersister.state.get
        .map(_.find(_.project == project))
        .unsafeRunSync() match {
        case Some(storingInfo) => fail(show"Token for project ${storingInfo.project.id} hasn't been stored")
        case None              => ()
      }

    def verifyExpiredTokensRevoked(project: Project): Unit =
      expiredTokensRevoker.state.get
        .map(_.collect { case (project.id, tokenId) =>
          tokenId
        })
        .unsafeRunSync() match {
        case Nil        => ()
        case notRevoked => fail(show"${notRevoked.size} tokens not revoked for $project")
      }
  }

  private lazy val events: Gen[TokenCloseExpiration] =
    (projectObjects -> encryptedAccessTokens).mapN(TokenCloseExpiration.apply)
}
