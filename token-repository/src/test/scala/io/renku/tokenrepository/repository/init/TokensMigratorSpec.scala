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
package init

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.{accessTokens, projectAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, localDates, timestampsNotInTheFuture}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info, Warn}
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.AccessTokenCrypto
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import io.renku.tokenrepository.repository.association.TokenDates.{CreatedAt, ExpiryDate}
import io.renku.tokenrepository.repository.association.TokenStoringInfo.Project
import io.renku.tokenrepository.repository.association._
import io.renku.tokenrepository.repository.deletion.TokenRemover
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

import java.time.{Instant, LocalDate}
import scala.concurrent.duration._

class TokensMigratorSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers with MockFactory {

  protected override lazy val migrationsToRun: List[DBMigration[IO]] = allMigrations.takeWhile {
    case _: TokensMigrator[IO] => false
    case _ => true
  }

  "run" should {

    "go through all the stored tokens that does not have the 'expiry_date' " +
      "and generate and store a new Project Access Token if the existing token is valid" in new TestCase {

        insert(validTokenProject, validTokenEncrypted)

        insertNonMigrated(oldTokenProject, oldTokenEncrypted)

        val oldToken = accessTokens.generateOne
        givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])

        givenTokenValidation(oldToken, returning = true.pure[IO])

        val projectToken = projectAccessTokens.generateOne
        val creationInfo = TokenCreationInfo(projectToken,
                                             TokenDates(CreatedAt(Instant.now()),
                                                        localDates(min = LocalDate.now()).generateAs(ExpiryDate)
                                             )
        )

        givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = creationInfo.pure[IO])

        val projectTokenEncrypted = encryptedAccessTokens.generateOne
        givenEncryption(projectToken, returning = projectTokenEncrypted.pure[IO])

        migration.run().unsafeRunSync() shouldBe ()

        findToken(validTokenProject.id)    shouldBe validTokenEncrypted.value.some
        findToken(oldTokenProject.id)      shouldBe projectTokenEncrypted.value.some
        findExpiryDate(oldTokenProject.id) shouldBe creationInfo.dates.expiryDate.some

        logger.loggedOnly(Info(show"$logPrefix $oldTokenProject token created"))
      }

    "remove the existing token if found token is invalid" in new TestCase {

      insert(validTokenProject, validTokenEncrypted)

      insertNonMigrated(oldTokenProject, oldTokenEncrypted)

      val oldToken = accessTokens.generateOne
      givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])

      givenTokenValidation(oldToken, returning = false.pure[IO])

      migration.run().unsafeRunSync() shouldBe ()

      findToken(validTokenProject.id) shouldBe validTokenEncrypted.value.some
      findToken(oldTokenProject.id)   shouldBe None

      logger.loggedOnly(Warn(show"$logPrefix $oldTokenProject token invalid; deleting"))
    }

    "log an error and remove the token if decryption fails" in new TestCase {

      insertNonMigrated(oldTokenProject, oldTokenEncrypted)

      val exception = exceptions.generateOne
      givenDecryption(oldTokenEncrypted, returning = exception.raiseError[IO, AccessToken])

      migration.run().unsafeRunSync() shouldBe ()

      findToken(oldTokenProject.id) shouldBe None

      logger.loggedOnly(Error(show"$logPrefix $oldTokenProject token decryption failed; deleting", exception))
    }

    "retry for exceptions on token validation check" in new TestCase {

      insertNonMigrated(oldTokenProject, oldTokenEncrypted)

      val oldToken = accessTokens.generateOne
      givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])

      val exception = exceptions.generateOne
      givenTokenValidation(oldToken, returning = exception.raiseError[IO, Boolean])
      givenTokenValidation(oldToken, returning = true.pure[IO])

      val projectToken = projectAccessTokens.generateOne
      val creationInfo =
        TokenCreationInfo(projectToken,
                          TokenDates(CreatedAt(Instant.now()), localDates(min = LocalDate.now()).generateAs(ExpiryDate))
        )

      givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = creationInfo.pure[IO])

      val projectTokenEncrypted = encryptedAccessTokens.generateOne
      givenEncryption(projectToken, returning = projectTokenEncrypted.pure[IO])

      migration.run().unsafeRunSync() shouldBe ()

      findToken(oldTokenProject.id) shouldBe projectTokenEncrypted.value.some

      logger.loggedOnly(
        Error(show"$logPrefix $oldTokenProject failure; retrying", exception),
        Info(show"$logPrefix $oldTokenProject token created")
      )
    }

    "retry for exceptions on token creation" in new TestCase {

      insertNonMigrated(oldTokenProject, oldTokenEncrypted)

      val oldToken = accessTokens.generateOne
      givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])

      givenTokenValidation(oldToken, returning = true.pure[IO])

      val exception = exceptions.generateOne
      givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = exception.raiseError[IO, TokenCreationInfo])
      val projectToken = projectAccessTokens.generateOne
      val creationInfo =
        TokenCreationInfo(projectToken,
                          TokenDates(CreatedAt(Instant.now()), localDates(min = LocalDate.now()).generateAs(ExpiryDate))
        )

      givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = creationInfo.pure[IO])

      val projectTokenEncrypted = encryptedAccessTokens.generateOne
      givenEncryption(projectToken, returning = projectTokenEncrypted.pure[IO])

      migration.run().unsafeRunSync() shouldBe ()

      findToken(oldTokenProject.id) shouldBe projectTokenEncrypted.value.some

      logger.loggedOnly(
        Error(show"$logPrefix $oldTokenProject failure; retrying", exception),
        Info(show"$logPrefix $oldTokenProject token created")
      )
    }
  }

  private trait TestCase {

    val validTokenProject   = Project(projectIds.generateOne, projectPaths.generateOne)
    val validTokenEncrypted = encryptedAccessTokens.generateOne

    val oldTokenProject   = Project(projectIds.generateOne, projectPaths.generateOne)
    val oldTokenEncrypted = encryptedAccessTokens.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tokenCrypto               = mock[AccessTokenCrypto[IO]]
    val tokenValidator            = mock[TokenValidator[IO]]
    val tokenRemover              = TokenRemover[IO](queriesExecTimes)
    val projectAccessTokenCreator = mock[ProjectAccessTokenCreator[IO]]
    val associationPersister      = AssociationPersister[IO](queriesExecTimes)
    val migration = new TokensMigrator[IO](tokenCrypto,
                                           tokenValidator,
                                           tokenRemover,
                                           projectAccessTokenCreator,
                                           associationPersister,
                                           queriesExecTimes,
                                           retryInterval = 500 millis
    )

    val logPrefix = "token migration:"

    def insert(project: Project, encryptedToken: EncryptedAccessToken) =
      associationPersister
        .persistAssociation(
          TokenStoringInfo(project,
                           encryptedToken,
                           TokenDates(timestampsNotInTheFuture.generateAs(CreatedAt), localDates.generateAs(ExpiryDate))
          )
        )
        .unsafeRunSync()

    def insertNonMigrated(project: Project, encryptedToken: EncryptedAccessToken) = execute[Unit] {
      Kleisli[IO, Session[IO], Unit] { session =>
        val command: Command[projects.Id ~ projects.Path ~ EncryptedAccessToken] =
          sql"""
          INSERT INTO projects_tokens (project_id, project_path, token)
          VALUES ($projectIdEncoder, $projectPathEncoder, $encryptedAccessTokenEncoder)
        """.command
        session
          .prepare(command)
          .use(_.execute(project.id ~ project.path ~ encryptedToken))
          .void
      }
    }

    def givenDecryption(encryptedToken: EncryptedAccessToken, returning: IO[AccessToken]) =
      (tokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(encryptedToken)
        .returning(returning)

    def givenEncryption(token: ProjectAccessToken, returning: IO[EncryptedAccessToken]) =
      (tokenCrypto.encrypt _)
        .expects(token)
        .returning(returning)

    def givenTokenValidation(token: AccessToken, returning: IO[Boolean]) =
      (tokenValidator.checkValid _)
        .expects(token)
        .returning(returning)

    def givenProjectTokenCreator(projectId:       projects.Id,
                                 userAccessToken: AccessToken,
                                 returning:       IO[TokenCreationInfo]
    ) = (projectAccessTokenCreator.createPersonalAccessToken _)
      .expects(projectId, userAccessToken)
      .returning(returning)
  }
}
