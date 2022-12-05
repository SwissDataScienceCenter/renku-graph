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
package creation

import AccessTokenCrypto.EncryptedAccessToken
import Generators._
import RepositoryGenerators._
import cats.data.OptionT
import cats.syntax.all._
import deletion.TokenRemover
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import fetching.PersistedTokensFinder
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.interpreters.TestLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class TokensCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "create" should {

    "do nothing if Project Access Token from the DB is valid, is not due for refresh " +
      "and project path has not changed" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

        val projectAccessToken = projectAccessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

        givenTokenValidation(of = projectAccessToken, returning = true.pure[Try])

        givenTokenDueCheck(projectId, returning = false.pure[Try])

        givenPathHasNotChanged(projectId, projectAccessToken)

        tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
      }

    "replace the stored token if it's not a Project Access Token" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      givenTokenDecryption(of = encryptedToken, returning = userAccessTokens.generateOne.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulOldTokenRevoking(projectId, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "replace the stored token if it's invalid" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

      givenTokenValidation(of = projectAccessToken, returning = false.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulOldTokenRevoking(projectId, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "replace the stored token if it's due for refresh" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

      givenTokenValidation(of = projectAccessToken, returning = true.pure[Try])

      givenTokenDueCheck(projectId, returning = true.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulOldTokenRevoking(projectId, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "leave the stored Project Access Token untouched but update the path if " +
      "the token is valid, it's not due for refresh " +
      "but the path has not changed" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

        val projectAccessToken = projectAccessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

        givenTokenValidation(of = projectAccessToken, returning = true.pure[Try])

        givenTokenDueCheck(projectId, returning = false.pure[Try])

        val newProjectPath = projectPaths.generateOne
        givenPathFinder(projectId, projectAccessToken, OptionT.some[Try](newProjectPath))
        givenStoredPathFinder(projectId, returning = projectPaths.generateOne.pure[Try])

        givenPathUpdate(Project(projectId, newProjectPath), returning = ().pure[Try])

        tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
      }

    "store a new Project Access Token if there's none stored" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      givenSuccessfulTokenCreation(projectPath)
      givenSuccessfulOldTokenRevoking(projectId, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "remove the stored token (non Project Access Token) if project with the given id does not exist" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

      givenTokenDecryption(of = encryptedToken, returning = userAccessTokens.generateOne.pure[Try])

      givenPathFinder(projectId, userAccessToken, returning = OptionT.none)

      givenTokenRemoval(projectId, returning = ().pure[Try])

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "delete token if stored token is valid and not due for refresh " +
      "but current project path cannot be found in GL" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

        val storedAccessToken = projectAccessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])
        givenTokenValidation(of = storedAccessToken, returning = true.pure[Try])
        givenTokenDueCheck(projectId, returning = false.pure[Try])

        givenPathFinder(projectId, storedAccessToken, returning = OptionT.none)

        givenTokenRemoval(projectId, returning = ().pure[Try])

        tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
      }

    "delete token if new token creation failed with NotFound or Forbidden" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

      val storedAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])

      givenTokenValidation(of = storedAccessToken, returning = false.pure[Try])

      val newProjectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(newProjectPath))

      givenProjectTokenCreator(projectId, userAccessToken, returning = OptionT.none)

      givenTokenRemoval(projectId, returning = ().pure[Try])

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "retry if the token after storing couldn't be decrypted" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

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

      givenSuccessfulOldTokenRevoking(projectId, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "retry if the token after storing couldn't be found" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

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

      givenSuccessfulOldTokenRevoking(projectId, userAccessToken)

      tokensCreator.create(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "fail if finding stored token fails" in new TestCase {

      val exception = exceptions.generateOne.raiseError[Try, EncryptedAccessToken]
      givenStoredTokenFinder(projectId, returning = OptionT.liftF(exception))

      tokensCreator.create(projectId, userAccessToken) shouldBe exception
    }

    "fail if retry process hit the max number of attempts" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

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

      val Failure(exception) = tokensCreator.create(projectId, userAccessToken)

      exception.getMessage shouldBe show"Token associator - just saved token cannot be found for project: $projectId"
    }
  }

  private trait TestCase {
    val projectId       = projectIds.generateOne
    val userAccessToken = userAccessTokens.generateOne

    implicit val logger:    TestLogger[Try]         = TestLogger[Try]()
    private val maxRetries: Int Refined NonNegative = 2
    val projectPathFinder      = mock[ProjectPathFinder[Try]]
    val accessTokenCrypto      = mock[AccessTokenCrypto[Try]]
    val tokenValidator         = mock[TokenValidator[Try]]
    val tokenDueChecker        = mock[TokenDueChecker[Try]]
    val newTokensCreator       = mock[NewTokensCreator[Try]]
    val tokensPersister        = mock[TokensPersister[Try]]
    val persistedPathFinder    = mock[PersistedPathFinder[Try]]
    val tokenRemover           = mock[TokenRemover[Try]]
    val tokensFinder           = mock[PersistedTokensFinder[Try]]
    val revokeCandidatesFinder = mock[RevokeCandidatesFinder[Try]]
    val tokensRevoker          = mock[TokensRevoker[Try]]
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
      revokeCandidatesFinder,
      tokensRevoker,
      maxRetries
    )

    def givenStoredTokenFinder(projectId: projects.Id, returning: OptionT[Try, EncryptedAccessToken]) =
      (tokensFinder
        .findStoredToken(_: Id))
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

    def givenTokenValidation(of: ProjectAccessToken, returning: Try[Boolean]) =
      (tokenValidator.checkValid _)
        .expects(of)
        .returning(returning)

    def givenTokenDueCheck(projectId: projects.Id, returning: Try[Boolean]) =
      (tokenDueChecker.checkTokenDue _)
        .expects(projectId)
        .returning(returning)

    def givenStoredPathFinder(projectId: projects.Id, returning: Try[projects.Path]) =
      (persistedPathFinder.findPersistedProjectPath _)
        .expects(projectId)
        .returning(returning)

    def givenPathFinder(projectId: projects.Id, accessToken: AccessToken, returning: OptionT[Try, projects.Path]) =
      (projectPathFinder.findProjectPath _)
        .expects(projectId, accessToken)
        .returning(returning)

    def givenPathHasNotChanged(projectId: projects.Id, accessToken: AccessToken) = {
      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, accessToken, OptionT.some[Try](projectPath))
      givenStoredPathFinder(projectId, returning = projectPath.pure[Try])
    }

    def givenProjectTokenCreator(projectId:       projects.Id,
                                 userAccessToken: UserAccessToken,
                                 returning:       OptionT[Try, TokenCreationInfo]
    ) = (newTokensCreator.createPersonalAccessToken _)
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

    def givenTokenRemoval(projectId: projects.Id, returning: Try[Unit]) =
      (tokenRemover.delete _)
        .expects(projectId)
        .returning(returning)

    def givenIntegrityCheckPasses(projectId:            projects.Id,
                                  token:                ProjectAccessToken,
                                  encryptedAccessToken: EncryptedAccessToken
    ) = {
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedAccessToken))
      givenTokenDecryption(of = encryptedAccessToken, returning = token.pure[Try])
    }

    def givenSuccessfulTokenCreation(projectPath: projects.Path) = {
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
    }

    def givenSuccessfulOldTokenRevoking(projectId: projects.Id, accessToken: AccessToken) = {
      val tokensToRevoke = accessTokenIds.generateList()
      (revokeCandidatesFinder.findTokensToRemove _)
        .expects(projectId, accessToken)
        .returning(tokensToRevoke.pure[Try])

      tokensToRevoke foreach { tokenId =>
        (tokensRevoker.revokeToken _)
          .expects(projectId, tokenId, accessToken)
          .returning(().pure[Try])
      }
    }
  }
}
