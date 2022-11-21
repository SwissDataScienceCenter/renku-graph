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
package association

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
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.RepositoryGenerators._
import io.renku.tokenrepository.repository.deletion.TokenRemover
import io.renku.tokenrepository.repository.fetching.PersistedTokensFinder
import io.renku.tokenrepository.repository.AccessTokenCrypto
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class TokenAssociatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "associate" should {

    "do nothing if there's a Project Access Token in the DB for the project " +
      "and it's valid" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne
        givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

        val projectAccessToken = projectAccessTokens.generateOne
        givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

        givenTokenValidation(of = projectAccessToken, returning = true.pure[Try])

        tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
      }

    "replace the stored token if it's invalid even if it's a Project Access Token" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(encryptedToken))

      val projectAccessToken = projectAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = projectAccessToken.pure[Try])

      givenTokenValidation(of = projectAccessToken, returning = false.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val newProjectAccessToken = projectAccessTokens.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = newProjectAccessToken.pure[Try])

      val encryptedNewAccessToken = encryptedAccessTokens.generateOne
      givenTokenEncryption(newProjectAccessToken, returning = encryptedNewAccessToken.pure[Try])

      givenTokenStoring(projectId, projectPath, encryptedNewAccessToken, returning = ().pure[Try])

      givenIntegrityCheckPasses(projectId, newProjectAccessToken, encryptedNewAccessToken)

      tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "replace the stored token if it's not a Project Access Token" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

      val storedAccessToken = userAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val newProjectAccessToken = projectAccessTokens.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = newProjectAccessToken.pure[Try])

      val encryptedNewAccessToken = encryptedAccessTokens.generateOne
      givenTokenEncryption(newProjectAccessToken, returning = encryptedNewAccessToken.pure[Try])

      givenTokenStoring(projectId, projectPath, encryptedNewAccessToken, returning = ().pure[Try])

      givenIntegrityCheckPasses(projectId, newProjectAccessToken, encryptedNewAccessToken)

      tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "store a new Project Access Token if there's none stored" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val newProjectAccessToken = projectAccessTokens.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = newProjectAccessToken.pure[Try])

      val encryptedNewAccessToken = encryptedAccessTokens.generateOne
      givenTokenEncryption(newProjectAccessToken, returning = encryptedNewAccessToken.pure[Try])

      givenTokenStoring(projectId, projectPath, encryptedNewAccessToken, returning = ().pure[Try])

      givenIntegrityCheckPasses(projectId, newProjectAccessToken, encryptedNewAccessToken)

      tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "remove the stored token (non Project Access Token) if project with the given Id does not exist" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some[Try](encryptedToken))

      val storedAccessToken = userAccessTokens.generateOne
      givenTokenDecryption(of = encryptedToken, returning = storedAccessToken.pure[Try])

      givenPathFinder(projectId, userAccessToken, returning = OptionT.none)

      givenTokenRemoval(projectId, returning = ().pure[Try])

      tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "retry if the token after storing couldn't be decrypted" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val newProjectAccessToken = projectAccessTokens.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = newProjectAccessToken.pure[Try])

      val encryptedNewAccessToken = encryptedAccessTokens.generateOne
      givenTokenEncryption(newProjectAccessToken, returning = encryptedNewAccessToken.pure[Try])

      givenTokenStoring(projectId, projectPath, encryptedNewAccessToken, returning = ().pure[Try]).twice()

      val invalidEncryptedAfterStoring = encryptedAccessTokens.generateOne
      givenStoredTokenFinder(projectId, returning = OptionT.some(invalidEncryptedAfterStoring))
      val exception = exceptions.generateOne
      givenTokenDecryption(of = invalidEncryptedAfterStoring, returning = exception.raiseError[Try, AccessToken])

      givenIntegrityCheckPasses(projectId, newProjectAccessToken, encryptedNewAccessToken)

      tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "retry if the token after storing couldn't be found" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val newProjectAccessToken = projectAccessTokens.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = newProjectAccessToken.pure[Try])

      val encryptedNewAccessToken = encryptedAccessTokens.generateOne
      givenTokenEncryption(newProjectAccessToken, returning = encryptedNewAccessToken.pure[Try])

      givenTokenStoring(projectId, projectPath, encryptedNewAccessToken, returning = ().pure[Try]).twice()

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      givenIntegrityCheckPasses(projectId, newProjectAccessToken, encryptedNewAccessToken)

      tokenAssociator.associate(projectId, userAccessToken) shouldBe ().pure[Try]
    }

    "fail if finding stored token fails" in new TestCase {

      val exception = exceptions.generateOne.raiseError[Try, EncryptedAccessToken]
      givenStoredTokenFinder(projectId, returning = OptionT.liftF(exception))

      tokenAssociator.associate(projectId, userAccessToken) shouldBe exception
    }

    "fail if retry process hit the max number of attempts" in new TestCase {

      givenStoredTokenFinder(projectId, returning = OptionT.none)

      val projectPath = projectPaths.generateOne
      givenPathFinder(projectId, userAccessToken, returning = OptionT.some(projectPath))

      val newProjectAccessToken = projectAccessTokens.generateOne
      givenProjectTokenCreator(projectId, userAccessToken, returning = newProjectAccessToken.pure[Try])

      val encryptedNewAccessToken = encryptedAccessTokens.generateOne
      givenTokenEncryption(newProjectAccessToken, returning = encryptedNewAccessToken.pure[Try])

      givenTokenStoring(projectId, projectPath, encryptedNewAccessToken, returning = ().pure[Try]).repeated(3)

      givenStoredTokenFinder(projectId, returning = OptionT.none).repeated(3)

      val Failure(exception) = tokenAssociator.associate(projectId, userAccessToken)

      exception.getMessage shouldBe show"Token associator - just saved token cannot be found for project: $projectId"
    }
  }

  private trait TestCase {
    val projectId       = projectIds.generateOne
    val userAccessToken = userAccessTokens.generateOne

    private val maxRetries: Int Refined NonNegative = 2
    val projectPathFinder         = mock[ProjectPathFinder[Try]]
    val accessTokenCrypto         = mock[AccessTokenCrypto[Try]]
    val tokenValidator            = mock[TokenValidator[Try]]
    val projectAccessTokenCreator = mock[ProjectAccessTokenCreator[Try]]
    val associationPersister      = mock[AssociationPersister[Try]]
    val tokenRemover              = mock[TokenRemover[Try]]
    val tokenFinder               = mock[PersistedTokensFinder[Try]]
    val tokenAssociator = new TokenAssociatorImpl[Try](projectPathFinder,
                                                       accessTokenCrypto,
                                                       tokenValidator,
                                                       projectAccessTokenCreator,
                                                       associationPersister,
                                                       tokenRemover,
                                                       tokenFinder,
                                                       maxRetries
    )

    def givenStoredTokenFinder(projectId: projects.Id, returning: OptionT[Try, EncryptedAccessToken]) =
      (tokenFinder
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

    def givenPathFinder(projectId:       projects.Id,
                        userAccessToken: UserAccessToken,
                        returning:       OptionT[Try, projects.Path]
    ) = (projectPathFinder.findProjectPath _)
      .expects(projectId, userAccessToken)
      .returning(returning)

    def givenProjectTokenCreator(projectId:       projects.Id,
                                 userAccessToken: UserAccessToken,
                                 returning:       Try[ProjectAccessToken]
    ) = (projectAccessTokenCreator.createPersonalAccessToken _)
      .expects(projectId, userAccessToken)
      .returning(returning)

    def givenTokenStoring(projectId:      projects.Id,
                          projectPath:    projects.Path,
                          encryptedToken: EncryptedAccessToken,
                          returning:      Try[Unit]
    ) = (associationPersister.persistAssociation _)
      .expects(projectId, projectPath, encryptedToken)
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
  }
}
