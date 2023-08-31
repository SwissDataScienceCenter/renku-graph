/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
package creation

import cats.data.OptionT
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.RepositoryGenerators._
import io.renku.tokenrepository.repository.creation.Generators._
import io.renku.tokenrepository.repository.deletion.{DeletionResult, TokenRemover, TokensRevoker}
import io.renku.tokenrepository.repository.fetching.PersistedTokensFinder
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class TokensCreatorSpec extends AsyncFlatSpec with CustomAsyncIOSpec with AsyncMockFactory with should.Matchers {

  it should "do nothing if the stored Access Token is valid, is not due for refresh " +
    "and the project slug has not changed" in {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

      val storedAccessToken = accessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[IO])

      givenTokenValidation(of = storedAccessToken, returning = true.pure[IO])

      givenSlugHasNotChanged(projectId, storedAccessToken)

      givenTokenDueCheck(projectId, returning = false.pure[IO])

      tokensCreator.create(projectId, userAccessToken).assertNoException
    }

  it should "replace the stored token when it's invalid" in {

    val encryptedToken = encryptedAccessTokens.generateOne
    givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

    val projectAccessToken = projectAccessTokens.generateOne
    givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[IO])

    givenTokenValidation(of = projectAccessToken, returning = false.pure[IO])
    givenTokenRemoval(projectId, userAccessToken, returning = deletionResults.generateOne.pure[IO])

    givenTokenValidation(userAccessToken, returning = true.pure[IO])
    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])
    val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
    givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "leave the stored Access Token untouched but update the slug if " +
    "the token is valid, it's not due for refresh " +
    "but the slug has changed" in {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[IO])

      givenTokenValidation(of = projectAccessToken, returning = true.pure[IO])

      val newProjectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, projectAccessToken, newProjectSlug.some.pure[IO])
      givenStoredSlugFinder(projectId, returning = projectSlugs.generateOne.some.pure[IO])

      givenSlugUpdate(Project(projectId, newProjectSlug), returning = ().pure[IO])

      givenTokenDueCheck(projectId, returning = false.pure[IO])

      tokensCreator.create(projectId, userAccessToken).assertNoException
    }

  it should "create a new Access Token if " +
    "the token found in the DB is valid, it's not due for refresh " +
    "but got removed in the meanwhile" in {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[IO])

      givenTokenValidation(of = projectAccessToken, returning = true.pure[IO])

      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, projectAccessToken, projectSlug.some.pure[IO])
      givenStoredSlugFinder(projectId, returning = None.pure[IO])

      givenTokenValidation(userAccessToken, returning = true.pure[IO])
      givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])
      val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken).assertNoException
    }

  it should "replace the stored token if it's due for refresh" in {

    val encryptedToken = encryptedAccessTokens.generateOne
    givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

    val storedAccessToken = projectAccessTokens.generateOne
    givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[IO])

    givenTokenValidation(of = storedAccessToken, returning = true.pure[IO])

    givenSlugHasNotChanged(projectId, storedAccessToken)

    givenTokenDueCheck(projectId, returning = true.pure[IO])

    givenTokenValidation(userAccessToken, returning = true.pure[IO])
    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])
    val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
    givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "store a new Project Access Token if there's none stored" in {

    givenStoredTokenFinder(projectId, returning = OptionT.none)

    givenTokenValidation(userAccessToken, returning = true.pure[IO])
    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])
    val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
    givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "remove the stored token if project with the given id does not exist" in {

    val encryptedToken = encryptedAccessTokens.generateOne
    givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

    val storedToken = userAccessTokens.generateOne
    givenTokenDecryption(of = encryptedToken, returning = storedToken.pure[IO])
    givenTokenValidation(storedToken, returning = false.pure[IO])
    givenTokenRemoval(projectId, userAccessToken, returning = deletionResults.generateOne.pure[IO])

    givenTokenValidation(userAccessToken, returning = false.pure[IO])

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "do nothing if new token creation failed with NotFound or Forbidden" in {

    val encryptedToken = encryptedAccessTokens.generateOne
    givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

    val storedAccessToken = projectAccessTokens.generateOne
    givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[IO])

    givenTokenValidation(of = storedAccessToken, returning = true.pure[IO])

    givenSlugHasNotChanged(projectId, storedAccessToken)

    givenTokenDueCheck(projectId, returning = true.pure[IO])

    givenTokenValidation(userAccessToken, returning = true.pure[IO])
    val newProjectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = newProjectSlug.some.pure[IO])

    givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.none)

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "retry if the token couldn't be decrypted after storing (token sanity check failed)" in {

    givenStoredTokenFinder(projectId, returning = OptionT.none)

    givenTokenValidation(userAccessToken, returning = true.pure[IO])

    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])

    val tokenCreationInfo = tokenCreationInfos.generateOne
    givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

    val newTokenEncrypted = encryptedAccessTokens.generateOne
    givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[IO])

    givenTokenStoring(Project(projectId, projectSlug),
                      newTokenEncrypted,
                      tokenCreationInfo.dates,
                      returning = ().pure[IO]
    ).twice()

    val invalidEncryptedAfterStoring = encryptedAccessTokens.generateOne
    givenStoredTokenFinder(projectId, returning = OptionT.some(invalidEncryptedAfterStoring))
    val exception = exceptions.generateOne
    givenTokenDecryption(of = invalidEncryptedAfterStoring, returning = exception.raiseError[IO, AccessToken])

    givenIntegrityCheckPasses(projectId, tokenCreationInfo.token, newTokenEncrypted)

    givenSuccessfulTokensRevoking(projectId, tokenCreationInfo, userAccessToken)

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "retry if the token after storing couldn't be found" in {

    givenStoredTokenFinder(projectId, returning = OptionT.none)

    givenTokenValidation(userAccessToken, returning = true.pure[IO])

    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])

    val tokenCreationInfo = tokenCreationInfos.generateOne
    givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

    val newTokenEncrypted = encryptedAccessTokens.generateOne
    givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[IO])

    givenTokenStoring(Project(projectId, projectSlug),
                      newTokenEncrypted,
                      tokenCreationInfo.dates,
                      returning = ().pure[IO]
    ).twice()

    givenStoredTokenFinder(projectId, returning = OptionT.none)

    givenIntegrityCheckPasses(projectId, tokenCreationInfo.token, newTokenEncrypted)

    givenSuccessfulTokensRevoking(projectId, tokenCreationInfo, userAccessToken)

    tokensCreator.create(projectId, userAccessToken).assertNoException
  }

  it should "fail if finding stored token fails" in {

    val exception = exceptions.generateOne
    givenStoredTokenFinder(projectId, returning = OptionT.liftF(exception.raiseError[IO, EncryptedAccessToken]))

    tokensCreator.create(projectId, userAccessToken).assertThrowsError[Exception](_ shouldBe exception)
  }

  it should "fail if retry process hit the max number of attempts" in {

    givenStoredTokenFinder(projectId, returning = OptionT.none)

    givenTokenValidation(userAccessToken, returning = true.pure[IO])

    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, userAccessToken, returning = projectSlug.some.pure[IO])

    val tokenCreationInfo = tokenCreationInfos.generateOne
    givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

    val newTokenEncrypted = encryptedAccessTokens.generateOne
    givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[IO])

    givenTokenStoring(Project(projectId, projectSlug),
                      newTokenEncrypted,
                      tokenCreationInfo.dates,
                      returning = ().pure[IO]
    ).repeated(3)

    givenStoredTokenFinder(projectId, returning = OptionT.none).repeated(3)

    tokensCreator
      .create(projectId, userAccessToken)
      .assertThrowsWithMessage[Exception](
        show"Token associator - just saved token cannot be found for project: $projectId"
      )
  }

  private lazy val projectId       = projectIds.generateOne
  private lazy val userAccessToken = userAccessTokens.generateOne

  private implicit lazy val logger: TestLogger[IO]          = TestLogger[IO]()
  private val maxRetries:           Int Refined NonNegative = 2
  private val projectSlugFinder   = mock[ProjectSlugFinder[IO]]
  private val accessTokenCrypto   = mock[AccessTokenCrypto[IO]]
  private val tokenValidator      = mock[TokenValidator[IO]]
  private val tokenDueChecker     = mock[TokenDueChecker[IO]]
  private val newTokensCreator    = mock[NewTokensCreator[IO]]
  private val tokensPersister     = mock[TokensPersister[IO]]
  private val persistedSlugFinder = mock[PersistedSlugFinder[IO]]
  private val tokenRemover        = mock[TokenRemover[IO]]
  private val tokensFinder        = mock[PersistedTokensFinder[IO]]
  private val tokensRevoker       = mock[TokensRevoker[IO]]
  private lazy val tokensCreator = new TokensCreatorImpl[IO](
    projectSlugFinder,
    accessTokenCrypto,
    tokenValidator,
    tokenDueChecker,
    newTokensCreator,
    tokensPersister,
    persistedSlugFinder,
    tokenRemover,
    tokensFinder,
    tokensRevoker,
    maxRetries
  )

  private def givenStoredTokenFinder(projectId: projects.GitLabId, returning: OptionT[IO, EncryptedAccessToken]) =
    (tokensFinder
      .findStoredToken(_: GitLabId))
      .expects(projectId)
      .returning(returning)
      .noMoreThanOnce()

  private def givenTokenDecryption(of: EncryptedAccessToken, returning: IO[AccessToken]) =
    (accessTokenCrypto
      .decrypt(_: EncryptedAccessToken))
      .expects(of)
      .returning(returning)

  private def givenTokenEncryption(projectAccessToken: ProjectAccessToken, returning: IO[EncryptedAccessToken]) =
    (accessTokenCrypto.encrypt _)
      .expects(projectAccessToken)
      .returning(returning)

  private def givenTokenValidation(of: AccessToken, returning: IO[Boolean]) =
    (tokenValidator.checkValid _)
      .expects(projectId, of)
      .returning(returning)

  private def givenTokenDueCheck(projectId: projects.GitLabId, returning: IO[Boolean]) =
    (tokenDueChecker.checkTokenDue _)
      .expects(projectId)
      .returning(returning)

  private def givenStoredSlugFinder(projectId: projects.GitLabId, returning: IO[Option[projects.Slug]]) =
    (persistedSlugFinder.findPersistedProjectSlug _)
      .expects(projectId)
      .returning(returning)

  private def givenSlugFinder(projectId:   projects.GitLabId,
                              accessToken: AccessToken,
                              returning:   IO[Option[projects.Slug]]
  ) =
    (projectSlugFinder.findProjectSlug _)
      .expects(projectId, accessToken)
      .returning(returning)

  private def givenSlugHasNotChanged(projectId: projects.GitLabId, accessToken: AccessToken) = {
    val projectSlug = projectSlugs.generateOne
    givenSlugFinder(projectId, accessToken, projectSlug.some.pure[IO])
    givenStoredSlugFinder(projectId, returning = projectSlug.some.pure[IO])
  }

  private def givenProjectTokenCreator(projectId:       projects.GitLabId,
                                       userAccessToken: UserAccessToken,
                                       returning:       OptionT[IO, TokenCreationInfo]
  ) = (newTokensCreator.createProjectAccessToken _)
    .expects(projectId, userAccessToken)
    .returning(returning)

  private def givenTokenStoring(project:        Project,
                                encryptedToken: EncryptedAccessToken,
                                dates:          TokenDates,
                                returning:      IO[Unit]
  ) = (tokensPersister.persistToken _)
    .expects(TokenStoringInfo(project, encryptedToken, dates))
    .returning(returning)

  private def givenSlugUpdate(project: Project, returning: IO[Unit]) =
    (tokensPersister.updateSlug _)
      .expects(project)
      .returning(returning)

  private def givenTokenRemoval(projectId:       projects.GitLabId,
                                userAccessToken: UserAccessToken,
                                returning:       IO[DeletionResult]
  ) = (tokenRemover.delete _)
    .expects(projectId, userAccessToken.some)
    .returning(returning)

  private def givenIntegrityCheckPasses(projectId:            projects.GitLabId,
                                        token:                ProjectAccessToken,
                                        encryptedAccessToken: EncryptedAccessToken
  ) = {
    givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedAccessToken))
    givenTokenDecryption(of = encryptedAccessToken, returning = token.pure[IO])
  }

  private def givenSuccessfulTokenCreation(projectSlug: projects.Slug): TokenCreationInfo = {

    val tokenCreationInfo = tokenCreationInfos.generateOne
    givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

    val newTokenEncrypted = encryptedAccessTokens.generateOne
    givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[IO])

    givenTokenStoring(Project(projectId, projectSlug),
                      newTokenEncrypted,
                      tokenCreationInfo.dates,
                      returning = ().pure[IO]
    )

    givenIntegrityCheckPasses(projectId, tokenCreationInfo.token, newTokenEncrypted)

    tokenCreationInfo
  }

  private def givenSuccessfulTokensRevoking(projectId:   projects.GitLabId,
                                            tokenInfo:   TokenCreationInfo,
                                            accessToken: AccessToken
  ) = (tokensRevoker.revokeAllTokens _)
    .expects(projectId, tokenInfo.tokenId.some, accessToken)
    .returning(().pure[IO])
}
