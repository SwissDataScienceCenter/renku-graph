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
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.RepositoryGenerators._
import io.renku.tokenrepository.repository.creation.Generators._
import io.renku.tokenrepository.repository.deletion.{TokenRemover, TokensRevoker}
import io.renku.tokenrepository.repository.fetching.PersistedTokensFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TokensCreatorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "create" should {

    "do nothing if the stored Access Token is valid, is not due for refresh " +
      "and the project slug has not changed" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

        val storedAccessToken = accessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[IO])

        givenTokenValidation(of = storedAccessToken, returning = true.pure[IO])

        givenSlugHasNotChanged(projectId, storedAccessToken)

        givenTokenDueCheck(projectId, returning = false.pure[IO])

        tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
      }

    "replace the stored token when it's invalid" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[IO])

      givenTokenValidation(of = projectAccessToken, returning = false.pure[IO])
      givenTokenRemoval(projectId, userAccessToken, returning = ().pure[IO])

      givenTokenValidation(userAccessToken, returning = true.pure[IO])
      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(projectSlug))
      val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "leave the stored Access Token untouched but update the slug if " +
      "the token is valid, it's not due for refresh " +
      "but the slug has changed" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

        val projectAccessToken = projectAccessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[IO])

        givenTokenValidation(of = projectAccessToken, returning = true.pure[IO])

        val newProjectSlug = projectSlugs.generateOne
        givenSlugFinder(projectId, projectAccessToken, OptionT.some[IO](newProjectSlug))
        givenStoredSlugFinder(projectId, returning = projectSlugs.generateOne.pure[IO])

        givenSlugUpdate(Project(projectId, newProjectSlug), returning = ().pure[IO])

        givenTokenDueCheck(projectId, returning = false.pure[IO])

        tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
      }

    "replace the stored token if it's due for refresh" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val storedAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[IO])

      givenTokenValidation(of = storedAccessToken, returning = true.pure[IO])

      givenSlugHasNotChanged(projectId, storedAccessToken)

      givenTokenDueCheck(projectId, returning = true.pure[IO])

      givenTokenValidation(userAccessToken, returning = true.pure[IO])
      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(projectSlug))
      val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "store a new Project Access Token if there's none stored" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[IO])
      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(projectSlug))
      val tokenInfo = givenSuccessfulTokenCreation(projectSlug)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "remove the stored token if project with the given id does not exist" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

      val storedToken = userAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedToken.pure[IO])
      givenTokenValidation(storedToken, returning = false.pure[IO])
      givenTokenRemoval(projectId, userAccessToken, returning = ().pure[IO])

      givenTokenValidation(userAccessToken, returning = false.pure[IO])

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "do nothing if new token creation failed with NotFound or Forbidden" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[IO](encryptedToken))

      val storedAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[IO])

      givenTokenValidation(of = storedAccessToken, returning = true.pure[IO])

      givenSlugHasNotChanged(projectId, storedAccessToken)

      givenTokenDueCheck(projectId, returning = true.pure[IO])

      givenTokenValidation(userAccessToken, returning = true.pure[IO])
      val newProjectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(newProjectSlug))

      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.none)

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "retry if the token couldn't be decrypted after storing (token sanity check failed)" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[IO])

      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(projectSlug))

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

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "retry if the token after storing couldn't be found" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[IO])

      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(projectSlug))

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

      tokensCreator.create(projectId, userAccessToken).unsafeRunSync() shouldBe ()
    }

    "fail if finding stored token fails" in new TestCase {

      val exception = exceptions.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.liftF(exception.raiseError[IO, EncryptedAccessToken]))

      val ex =
        intercept[Exception] {
          tokensCreator.create(projectId, userAccessToken).unsafeRunSync()
        }

      ex shouldBe exception
    }

    "fail if retry process hit the max number of attempts" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[IO])

      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, userAccessToken, returning = OptionT.some(projectSlug))

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

      val exception = intercept[Exception] {
        tokensCreator
          .create(projectId, userAccessToken)
          .unsafeRunSync()
      }

      exception.getMessage shouldBe show"Token associator - just saved token cannot be found for project: $projectId"
    }
  }

  private trait TestCase {
    val projectId       = projectIds.generateOne
    val userAccessToken = userAccessTokens.generateOne

    implicit val logger:    TestLogger[IO]          = TestLogger[IO]()
    private val maxRetries: Int Refined NonNegative = 2
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
    val tokensCreator = new TokensCreatorImpl[IO](
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

    def givenStoredTokenFinder(projectId: projects.GitLabId, returning: OptionT[IO, EncryptedAccessToken]) =
      (tokensFinder
        .findStoredToken(_: GitLabId))
        .expects(projectId)
        .returning(returning)
        .noMoreThanOnce()

    def givenTokenDecryption(of: EncryptedAccessToken, returning: IO[AccessToken]) =
      (accessTokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(of)
        .returning(returning)

    def givenTokenEncryption(projectAccessToken: ProjectAccessToken, returning: IO[EncryptedAccessToken]) =
      (accessTokenCrypto.encrypt _)
        .expects(projectAccessToken)
        .returning(returning)

    def givenTokenValidation(of: AccessToken, returning: IO[Boolean]) =
      (tokenValidator.checkValid _)
        .expects(projectId, of)
        .returning(returning)

    def givenTokenDueCheck(projectId: projects.GitLabId, returning: IO[Boolean]) =
      (tokenDueChecker.checkTokenDue _)
        .expects(projectId)
        .returning(returning)

    def givenStoredSlugFinder(projectId: projects.GitLabId, returning: IO[projects.Slug]) =
      (persistedSlugFinder.findPersistedProjectSlug _)
        .expects(projectId)
        .returning(returning)

    def givenSlugFinder(projectId: projects.GitLabId, accessToken: AccessToken, returning: OptionT[IO, projects.Slug]) =
      (projectSlugFinder.findProjectSlug _)
        .expects(projectId, accessToken)
        .returning(returning)

    def givenSlugHasNotChanged(projectId: projects.GitLabId, accessToken: AccessToken) = {
      val projectSlug = projectSlugs.generateOne
      givenSlugFinder(projectId, accessToken, OptionT.some[IO](projectSlug))
      givenStoredSlugFinder(projectId, returning = projectSlug.pure[IO])
    }

    def givenProjectTokenCreator(projectId:       projects.GitLabId,
                                 userAccessToken: UserAccessToken,
                                 returning:       OptionT[IO, TokenCreationInfo]
    ) = (newTokensCreator.createProjectAccessToken _)
      .expects(projectId, userAccessToken)
      .returning(returning)

    def givenTokenStoring(project:        Project,
                          encryptedToken: EncryptedAccessToken,
                          dates:          TokenDates,
                          returning:      IO[Unit]
    ) = (tokensPersister.persistToken _)
      .expects(TokenStoringInfo(project, encryptedToken, dates))
      .returning(returning)

    def givenSlugUpdate(project: Project, returning: IO[Unit]) =
      (tokensPersister.updateSlug _)
        .expects(project)
        .returning(returning)

    def givenTokenRemoval(projectId: projects.GitLabId, userAccessToken: UserAccessToken, returning: IO[Unit]) =
      (tokenRemover.delete _)
        .expects(projectId, userAccessToken.some)
        .returning(returning)

    def givenIntegrityCheckPasses(projectId:            projects.GitLabId,
                                  token:                ProjectAccessToken,
                                  encryptedAccessToken: EncryptedAccessToken
    ) = {
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedAccessToken))
      givenTokenDecryption(of = encryptedAccessToken, returning = token.pure[IO])
    }

    def givenSuccessfulTokenCreation(projectSlug: projects.Slug): TokenCreationInfo = {

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

    def givenSuccessfulTokensRevoking(projectId:   projects.GitLabId,
                                      tokenInfo:   TokenCreationInfo,
                                      accessToken: AccessToken
    ) = (tokensRevoker.revokeAllTokens _)
      .expects(projectId, tokenInfo.tokenId.some, accessToken)
      .returning(().pure[IO])
  }
}
