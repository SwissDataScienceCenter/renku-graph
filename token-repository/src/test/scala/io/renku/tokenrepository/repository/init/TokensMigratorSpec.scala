/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import AccessTokenCrypto.EncryptedAccessToken
import RepositoryGenerators.{accessTokenIds, encryptedAccessTokens}
import cats.data.OptionT
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import creation.TokenDates.{CreatedAt, ExpiryDate}
import creation._
import deletion.PersistedTokenRemover
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.generators.CommonGraphGenerators.{accessTokens, projectAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, localDates, timestampsNotInTheFuture}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.tokenrepository.repository.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.implicits._

import java.time.{Instant, LocalDate}
import scala.concurrent.duration._

class TokensMigratorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with DbInitSpec
    with should.Matchers
    with AsyncMockFactory {

  protected override lazy val runMigrationsUpTo: Class[_ <: DBMigration[IO]] =
    classOf[TokensMigrator[IO]]

  it should "for stored tokens that does not have the 'expiry_date' " +
    "generate and store a new Project Access Token if the existing token is valid" in testDBResource.use {
      implicit cfg =>
        val validTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
        val validTokenEncrypted = encryptedAccessTokens.generateOne
        val oldTokenProject     = Project(projectIds.generateOne, projectSlugs.generateOne)
        val oldTokenEncrypted   = encryptedAccessTokens.generateOne

        for {
          _ <- insert(validTokenProject, validTokenEncrypted).assertNoException
          _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException
          (projectTokenEncrypted, creationInfo) =
            givenSuccessfulTokenReplacement(oldTokenProject, oldTokenEncrypted)

          _ <- migration.run.assertNoException

          _ <- findToken(validTokenProject.id).asserting(_ shouldBe validTokenEncrypted.value.some)
          _ <- findToken(oldTokenProject.id).asserting(_ shouldBe projectTokenEncrypted.value.some)
          _ <- findExpiryDate(oldTokenProject.id).asserting(_ shouldBe creationInfo.dates.expiryDate.some)
          _ <- logger.loggedOnlyF(Info(show"$logPrefix $oldTokenProject token created"))
        } yield Succeeded
    }

  it should "go through all the stored tokens without the 'expiry_date'" in testDBResource.use { implicit cfg =>
    val validTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
    val validTokenEncrypted = encryptedAccessTokens.generateOne
    val oldTokenProject     = Project(projectIds.generateOne, projectSlugs.generateOne)
    val oldTokenEncrypted   = encryptedAccessTokens.generateOne

    for {
      _ <- insert(validTokenProject, validTokenEncrypted).assertNoException
      _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException
      (projectTokenEncrypted, creationInfo) =
        givenSuccessfulTokenReplacement(oldTokenProject, oldTokenEncrypted)

      oldTokenProject1   = Project(projectIds.generateOne, projectSlugs.generateOne)
      oldTokenEncrypted1 = encryptedAccessTokens.generateOne
      _ <- insertNonMigrated(oldTokenProject1, oldTokenEncrypted1).assertNoException
      (projectTokenEncrypted1, creationInfo1) =
        givenSuccessfulTokenReplacement(oldTokenProject1, oldTokenEncrypted1)

      _ <- migration.run.assertNoException

      _ <- findToken(validTokenProject.id).asserting(_ shouldBe validTokenEncrypted.value.some)
      _ <- findToken(oldTokenProject.id).asserting(_ shouldBe projectTokenEncrypted.value.some)
      _ <- findExpiryDate(oldTokenProject.id).asserting(_ shouldBe creationInfo.dates.expiryDate.some)
      _ <- findToken(oldTokenProject1.id).asserting(_ shouldBe projectTokenEncrypted1.value.some)
      _ <- findExpiryDate(oldTokenProject1.id).asserting(_ shouldBe creationInfo1.dates.expiryDate.some)
      _ <- logger.loggedOnlyF(Info(show"$logPrefix $oldTokenProject token created"),
                              Info(show"$logPrefix $oldTokenProject1 token created")
           )
    } yield Succeeded
  }

  it should "remove the existing token if found token is invalid" in testDBResource.use { implicit cfg =>
    val validTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
    val validTokenEncrypted = encryptedAccessTokens.generateOne
    val oldTokenProject     = Project(projectIds.generateOne, projectSlugs.generateOne)
    val oldTokenEncrypted   = encryptedAccessTokens.generateOne

    for {
      _ <- insert(validTokenProject, validTokenEncrypted).assertNoException
      _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException

      oldToken = accessTokens.generateOne
      _        = givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])
      _        = givenTokenValidation(oldTokenProject.id, oldToken, returning = false.pure[IO])

      _ <- migration.run.assertNoException

      _ <- findToken(validTokenProject.id).asserting(_ shouldBe validTokenEncrypted.value.some)
      _ <- findToken(oldTokenProject.id).asserting(_ shouldBe None)
    } yield Succeeded
  }

  it should "remove the existing token if new token creation failed with Forbidden" in testDBResource.use {
    implicit cfg =>
      val validTokenEncrypted = encryptedAccessTokens.generateOne
      val validTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
      val oldTokenProject     = Project(projectIds.generateOne, projectSlugs.generateOne)
      val oldTokenEncrypted   = encryptedAccessTokens.generateOne

      for {
        _ <- insert(validTokenProject, validTokenEncrypted).assertNoException
        _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException
        oldToken = accessTokens.generateOne
        _        = givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])
        _        = givenTokenValidation(oldTokenProject.id, oldToken, returning = true.pure[IO])
        _        = givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = OptionT.none)

        _ <- migration.run.assertNoException

        _ <- findToken(validTokenProject.id).asserting(_ shouldBe validTokenEncrypted.value.some)
        _ <- findToken(oldTokenProject.id).asserting(_ shouldBe None)
      } yield Succeeded
  }

  it should "log an error and remove the token if decryption fails" in testDBResource.use { implicit cfg =>
    val oldTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
    val oldTokenEncrypted = encryptedAccessTokens.generateOne

    for {
      _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException
      exception = exceptions.generateOne
      _         = givenDecryption(oldTokenEncrypted, returning = exception.raiseError[IO, AccessToken])

      _ <- migration.run.assertNoException

      _ <- findToken(oldTokenProject.id).asserting(_ shouldBe None)
    } yield Succeeded
  }

  it should "retry for exceptions on token validation check" in testDBResource.use { implicit cfg =>
    val oldTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
    val oldTokenEncrypted = encryptedAccessTokens.generateOne

    for {
      _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException
      oldToken = accessTokens.generateOne
      _        = givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])

      exception = exceptions.generateOne
      _         = givenTokenValidation(oldTokenProject.id, oldToken, returning = exception.raiseError[IO, Boolean])
      _         = givenTokenValidation(oldTokenProject.id, oldToken, returning = true.pure[IO])

      projectToken = projectAccessTokens.generateOne
      creationInfo =
        TokenCreationInfo(accessTokenIds.generateOne,
                          projectToken,
                          TokenDates(CreatedAt(Instant.now()), localDates(min = LocalDate.now()).generateAs(ExpiryDate))
        )

      _ = givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = OptionT.some(creationInfo))

      projectTokenEncrypted = encryptedAccessTokens.generateOne
      _                     = givenEncryption(projectToken, returning = projectTokenEncrypted.pure[IO])

      _ <- migration.run.assertNoException

      _ <- findToken(oldTokenProject.id).asserting(_ shouldBe projectTokenEncrypted.value.some)
      _ <- logger.loggedOnlyF(
             Error(show"$logPrefix $oldTokenProject failure; retrying", exception),
             Info(show"$logPrefix $oldTokenProject token created")
           )
    } yield Succeeded
  }

  it should "retry for exceptions on token creation" in testDBResource.use { implicit cfg =>
    val oldTokenProject   = Project(projectIds.generateOne, projectSlugs.generateOne)
    val oldTokenEncrypted = encryptedAccessTokens.generateOne

    for {
      _ <- insertNonMigrated(oldTokenProject, oldTokenEncrypted).assertNoException
      oldToken  = accessTokens.generateOne
      _         = givenDecryption(oldTokenEncrypted, returning = oldToken.pure[IO])
      _         = givenTokenValidation(oldTokenProject.id, oldToken, returning = true.pure[IO])
      exception = exceptions.generateOne
      _ = givenProjectTokenCreator(oldTokenProject.id,
                                   oldToken,
                                   returning = OptionT.liftF(exception.raiseError[IO, TokenCreationInfo])
          )
      projectToken = projectAccessTokens.generateOne
      creationInfo =
        TokenCreationInfo(accessTokenIds.generateOne,
                          projectToken,
                          TokenDates(CreatedAt(Instant.now()), localDates(min = LocalDate.now()).generateAs(ExpiryDate))
        )
      _ = givenProjectTokenCreator(oldTokenProject.id, oldToken, returning = OptionT.some(creationInfo))
      projectTokenEncrypted = encryptedAccessTokens.generateOne
      _                     = givenEncryption(projectToken, returning = projectTokenEncrypted.pure[IO])

      _ <- migration.run.assertNoException

      _ <- findToken(oldTokenProject.id).asserting(_ shouldBe projectTokenEncrypted.value.some)
      _ <- logger.loggedOnlyF(
             Error(show"$logPrefix $oldTokenProject failure; retrying", exception),
             Info(show"$logPrefix $oldTokenProject token created")
           )
    } yield Succeeded
  }

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private val tokenCrypto    = mock[AccessTokenCrypto[IO]]
  private val tokenValidator = mock[TokenValidator[IO]]
  private val tokensCreator  = mock[NewTokensCreator[IO]]
  private def migration(implicit cfg: DBConfig[ProjectsTokensDB]) =
    new TokensMigrator[IO](tokenCrypto,
                           tokenValidator,
                           PersistedTokenRemover[IO],
                           tokensCreator,
                           retryInterval = 500 millis
    )

  val logPrefix = "token migration:"

  private def insert(project: Project, encryptedToken: EncryptedAccessToken)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[Unit] =
    sessionResource(cfg).use { session =>
      val command: Command[
        projects.GitLabId *: projects.Slug *: EncryptedAccessToken *: CreatedAt *: ExpiryDate *: EmptyTuple
      ] = sql""" INSERT INTO projects_tokens (project_id, project_path, token, created_at, expiry_date)
                     VALUES ($projectIdEncoder, $projectSlugEncoder, $encryptedAccessTokenEncoder, $createdAtEncoder, $expiryDateEncoder)""".command
      session
        .prepare(command)
        .flatMap(
          _.execute(
            project.id *: project.slug *: encryptedToken *:
              timestampsNotInTheFuture.generateAs(CreatedAt) *: localDates.generateAs(ExpiryDate) *: EmptyTuple
          )
        )
        .void
    }

  private def insertNonMigrated(project: Project, encryptedToken: EncryptedAccessToken)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[Unit] =
    sessionResource(cfg).use { session =>
      val command: Command[projects.GitLabId *: projects.Slug *: EncryptedAccessToken *: EmptyTuple] =
        sql"""
          INSERT INTO projects_tokens (project_id, project_path, token)
          VALUES ($projectIdEncoder, $projectSlugEncoder, $encryptedAccessTokenEncoder)""".command
      session
        .prepare(command)
        .flatMap(_.execute(project.id *: project.slug *: encryptedToken *: EmptyTuple))
        .void
    }

  private def givenSuccessfulTokenReplacement(project:     Project,
                                              oldTokenEnc: EncryptedAccessToken
  ): (EncryptedAccessToken, TokenCreationInfo) = {

    val oldToken = accessTokens.generateOne
    givenDecryption(oldTokenEnc, returning = oldToken.pure[IO])

    givenTokenValidation(project.id, oldToken, returning = true.pure[IO])

    val projectToken = projectAccessTokens.generateOne
    val creationInfo = TokenCreationInfo(
      accessTokenIds.generateOne,
      projectToken,
      TokenDates(CreatedAt(Instant.now()), localDates(min = LocalDate.now()).generateAs(ExpiryDate))
    )

    givenProjectTokenCreator(project.id, oldToken, returning = OptionT.some(creationInfo))

    val projectTokenEncrypted = encryptedAccessTokens.generateOne
    givenEncryption(projectToken, returning = projectTokenEncrypted.pure[IO])

    projectTokenEncrypted -> creationInfo
  }

  private def givenDecryption(encryptedToken: EncryptedAccessToken, returning: IO[AccessToken]) =
    (tokenCrypto
      .decrypt(_: EncryptedAccessToken))
      .expects(encryptedToken)
      .returning(returning)

  private def givenEncryption(token: ProjectAccessToken, returning: IO[EncryptedAccessToken]) =
    (tokenCrypto.encrypt _)
      .expects(token)
      .returning(returning)

  private def givenTokenValidation(projectId: projects.GitLabId, token: AccessToken, returning: IO[Boolean]) =
    (tokenValidator.checkValid _)
      .expects(projectId, token)
      .returning(returning)

  private def givenProjectTokenCreator(projectId:       projects.GitLabId,
                                       userAccessToken: AccessToken,
                                       returning:       OptionT[IO, TokenCreationInfo]
  ) = (tokensCreator.createProjectAccessToken _)
    .expects(projectId, userAccessToken)
    .returning(returning)
}
