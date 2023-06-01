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
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.RepositoryGenerators._
import io.renku.tokenrepository.repository.creation.Generators._
import io.renku.tokenrepository.repository.deletion.{TokenRemover, TokensRevoker}
import io.renku.tokenrepository.repository.fetching.PersistedTokensFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TokensCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers with TryValues {

  "create" should {

    "do nothing if the stored Access Token is valid, is not due for refresh " +
      "and the project path has not changed" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

        val storedAccessToken = accessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])

        givenTokenValidation(of = storedAccessToken, returning = true.pure[Try])

        givenPathHasNotChanged(projectId, storedAccessToken)

        givenTokenDueCheck(projectId, returning = false.pure[Try])

        tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
      }

    "replace the stored token when it's invalid" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

      givenTokenValidation(of = projectAccessToken, returning = false.pure[Try])
      givenTokenRemoval(projectId, userAccessToken, returning = ().pure[Try])

      givenTokenValidation(userAccessToken, returning = true.pure[Try])
      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))
      val tokenInfo = givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "leave the stored Access Token untouched but update the path if " +
      "the token is valid, it's not due for refresh " +
      "but the path has changed" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

        val projectAccessToken = projectAccessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

        givenTokenValidation(of = projectAccessToken, returning = true.pure[Try])

        val newProjectPath = projectPaths.generateOne
        givenPathFinder(projectId, projectAccessToken, OptionT.some[Try](newProjectPath))
        givenStoredPathFinder(projectId, returning = projectPaths.generateOne.pure[Try])

        givenPathUpdate(Project(projectId, newProjectPath), returning = ().pure[Try])

        givenTokenDueCheck(projectId, returning = false.pure[Try])

        tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
      }

    "replace the stored token if it's due for refresh" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val storedAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])

      givenTokenValidation(of = storedAccessToken, returning = true.pure[Try])

      givenPathHasNotChanged(projectId, storedAccessToken)

      givenTokenDueCheck(projectId, returning = true.pure[Try])

      givenTokenValidation(userAccessToken, returning = true.pure[Try])
      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))
      val tokenInfo = givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "store a new Project Access Token if there's none stored" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[Try])
      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))
      val tokenInfo = givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulTokensRevoking(projectId, tokenInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "remove the stored token if project with the given id does not exist" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

      val storedToken = userAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedToken.pure[Try])
      givenTokenValidation(storedToken, returning = false.pure[Try])
      givenTokenRemoval(projectId, userAccessToken, returning = ().pure[Try])

      givenTokenValidation(userAccessToken, returning = false.pure[Try])

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "do nothing if new token creation failed with NotFound or Forbidden" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

      val storedAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])

      givenTokenValidation(of = storedAccessToken, returning = true.pure[Try])

      givenPathHasNotChanged(projectId, storedAccessToken)

      givenTokenDueCheck(projectId, returning = true.pure[Try])

      givenTokenValidation(userAccessToken, returning = true.pure[Try])
      val newProjectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(newProjectPath))

      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.none)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "retry if the token couldn't be decrypted after storing (token sanity check failed)" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val tokenCreationInfo = tokenCreationInfos.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

      val newTokenEncrypted = encryptedAccessTokens.generateOne
      givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[Try])

      givenTokenStoring(Project(projectId, projectPath),
                        newTokenEncrypted,
                        tokenCreationInfo.dates,
                        returning = ().pure[Try]
      ).twice()

      val invalidEncryptedAfterStoring = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(invalidEncryptedAfterStoring))
      val exception = exceptions.generateOne
      givenTokenDecryption(of = invalidEncryptedAfterStoring, returning = exception.raiseError[Try, AccessToken])

      givenIntegrityCheckPasses(projectId, tokenCreationInfo.token, newTokenEncrypted)

      givenSuccessfulTokensRevoking(projectId, tokenCreationInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "retry if the token after storing couldn't be found" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val tokenCreationInfo = tokenCreationInfos.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

      val newTokenEncrypted = encryptedAccessTokens.generateOne
      givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[Try])

      givenTokenStoring(Project(projectId, projectPath),
                        newTokenEncrypted,
                        tokenCreationInfo.dates,
                        returning = ().pure[Try]
      ).twice()

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenIntegrityCheckPasses(projectId, tokenCreationInfo.token, newTokenEncrypted)

      givenSuccessfulTokensRevoking(projectId, tokenCreationInfo, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "fail if finding stored token fails" in new TestCase {

      val exception = exceptions.generateOne.raiseError[Try, EncryptedAccessToken]
      givenStoredTokenFinder(projectId, returning = OptionT.liftF(exception))

      tokensCreator.create(projectId, userAccessToken) shouldBe exception
    }

    "fail if retry process hit the max number of attempts" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenTokenValidation(userAccessToken, returning = true.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val tokenCreationInfo = tokenCreationInfos.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

      val newTokenEncrypted = encryptedAccessTokens.generateOne
      givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[Try])

      givenTokenStoring(Project(projectId, projectPath),
                        newTokenEncrypted,
                        tokenCreationInfo.dates,
                        returning = ().pure[Try]
      ).repeated(3)

      givenStoredTokenFinder(projectId, returning = OptionT.none).repeated(3)

      tokensCreator
        .create(projectId, userAccessToken)
        .failure
        .exception
        .getMessage shouldBe show"Token associator - just saved token cannot be found for project: $projectId"
    }
  }

  private trait TestCase {
    val projectId       = projectIds.generateOne
    val userAccessToken = userAccessTokens.generateOne

    implicit val logger:    TestLogger[Try]         = TestLogger[Try]()
    private val maxRetries: Int Refined NonNegative = 2
    private val projectPathFinder   = mock[ProjectPathFinder[Try]]
    private val accessTokenCrypto   = mock[AccessTokenCrypto[Try]]
    private val tokenValidator      = mock[TokenValidator[Try]]
    private val tokenDueChecker     = mock[TokenDueChecker[Try]]
    private val newTokensCreator    = mock[NewTokensCreator[Try]]
    private val tokensPersister     = mock[TokensPersister[Try]]
    private val persistedPathFinder = mock[PersistedPathFinder[Try]]
    private val tokenRemover        = mock[TokenRemover[Try]]
    private val tokensFinder        = mock[PersistedTokensFinder[Try]]
    private val tokensRevoker       = mock[TokensRevoker[Try]]
    val tokensCreator = new TokensCreatorImpl[Try](
      projectPathFinder,
      accessTokenCrypto,
      tokenValidator,
      tokenDueChecker,
      newTokensCreator,
      tokensPersister,
      persistedPathFinder,
      tokenRemover,
      tokensFinder,
      tokensRevoker,
      maxRetries
    )

    def givenStoredTokenFinder(projectId: projects.GitLabId, returning: OptionT[Try, EncryptedAccessToken]) =
      (tokensFinder
        .findStoredToken(_: GitLabId))
        .expects(projectId)
        .returning(returning)
        .noMoreThanOnce()

    def givenTokenDecryption(of: EncryptedAccessToken, returning: Try[AccessToken]) =
      (accessTokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(of)
        .returning(returning)

    def givenTokenEncryption(projectAccessToken: ProjectAccessToken, returning: Try[EncryptedAccessToken]) =
      (accessTokenCrypto.encrypt _)
        .expects(projectAccessToken)
        .returning(returning)

    def givenTokenValidation(of: AccessToken, returning: Try[Boolean]) =
      (tokenValidator.checkValid _)
        .expects(projectId, of)
        .returning(returning)

    def givenTokenDueCheck(projectId: projects.GitLabId, returning: Try[Boolean]) =
      (tokenDueChecker.checkTokenDue _)
        .expects(projectId)
        .returning(returning)

    def givenStoredPathFinder(projectId: projects.GitLabId, returning: Try[projects.Path]) =
      (persistedPathFinder.findPersistedProjectPath _)
        .expects(projectId)
        .returning(returning)

    def givenPathFinder(projectId:   projects.GitLabId,
                        accessToken: AccessToken,
                        returning:   OptionT[Try, projects.Path]
    ) = (projectPathFinder.findProjectPath _)
      .expects(projectId, accessToken)
      .returning(returning)

    def givenPathHasNotChanged(projectId: projects.GitLabId, accessToken: AccessToken) = {
      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, accessToken, OptionT.some[Try](projectPath))
      givenStoredPathFinder(projectId, returning = projectPath.pure[Try])
    }

    def givenProjectTokenCreator(projectId:       projects.GitLabId,
                                 userAccessToken: UserAccessToken,
                                 returning:       OptionT[Try, TokenCreationInfo]
    ) = (newTokensCreator.createProjectAccessToken _)
      .expects(projectId, userAccessToken)
      .returning(returning)

    def givenTokenStoring(project:        Project,
                          encryptedToken: EncryptedAccessToken,
                          dates:          TokenDates,
                          returning:      Try[Unit]
    ) = (tokensPersister.persistToken _)
      .expects(TokenStoringInfo(project, encryptedToken, dates))
      .returning(returning)

    def givenPathUpdate(project: Project, returning: Try[Unit]) =
      (tokensPersister.updatePath _)
        .expects(project)
        .returning(returning)

    def givenTokenRemoval(projectId: projects.GitLabId, userAccessToken: UserAccessToken, returning: Try[Unit]) =
      (tokenRemover.delete _)
        .expects(projectId, userAccessToken.some)
        .returning(returning)

    def givenIntegrityCheckPasses(projectId:            projects.GitLabId,
                                  token:                ProjectAccessToken,
                                  encryptedAccessToken: EncryptedAccessToken
    ) = {
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedAccessToken))
      givenTokenDecryption(of = encryptedAccessToken, returning = token.pure[Try])
    }

    def givenSuccessfulTokenCreation(projectPath: projects.Path): TokenCreationInfo = {

      val tokenCreationInfo = tokenCreationInfos.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.some(tokenCreationInfo))

      val newTokenEncrypted = encryptedAccessTokens.generateOne
      givenTokenEncryption(tokenCreationInfo.token, returning = newTokenEncrypted.pure[Try])

      givenTokenStoring(Project(projectId, projectPath),
                        newTokenEncrypted,
                        tokenCreationInfo.dates,
                        returning = ().pure[Try]
      )

      givenIntegrityCheckPasses(projectId, tokenCreationInfo.token, newTokenEncrypted)

      tokenCreationInfo
    }

    def givenSuccessfulTokensRevoking(projectId:   projects.GitLabId,
                                      tokenInfo:   TokenCreationInfo,
                                      accessToken: AccessToken
    ) = (tokensRevoker.revokeAllTokens _)
      .expects(projectId, tokenInfo.tokenId.some, accessToken)
      .returning(().pure[Try])
  }
}
