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

package io.renku.webhookservice.hookvalidation

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.projects.GitLabId
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.hookvalidation.HookValidator.NoAccessTokenException
import io.renku.webhookservice.model.HookIdentifier
import io.renku.webhookservice.tokenrepository.{AccessTokenAssociator, AccessTokenRemover}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.TryValues

import scala.util.Try

class HookValidatorSpec extends AnyWordSpec with MockFactory with should.Matchers with TryValues {
  import AccessTokenFinder.Implicits._

  "validateHook - finding access token" should {

    "fail if finding stored access token fails when no access token given" in new TestCase {

      val exception = exceptions.generateOne
      givenAccessTokenFinding(returning = exception.raiseError[Try, Option[AccessToken]])

      val result = validator.validateHook(projectId, maybeAccessToken = None)

      result.failure.exception.getMessage shouldBe show"Finding stored access token for $projectId failed"
      result.failure.exception.getCause   shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", result.failure.exception))
    }

    "fail if finding stored access token return None when no access token given" in new TestCase {

      givenAccessTokenFinding(returning = Option.empty[AccessToken].pure[Try])

      val noAccessTokenException = NoAccessTokenException(s"No stored access token found for projectId $projectId")
      validator.validateHook(projectId, maybeAccessToken = None) shouldBe noAccessTokenException
        .raiseError[Try, Nothing]

      logger.loggedOnly(Info(s"Hook validation failed: ${noAccessTokenException.getMessage}"))
    }
  }

  "validateHook - given access token" should {

    "succeed with HookExists and re-associate access token if there's a hook" in new TestCase {

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), givenAccessToken, returning = true.pure[Try])

      givenTokenAssociation(givenAccessToken, returning = ().pure[Try])

      validator.validateHook(projectId, givenAccessToken.some) shouldBe HookExists.pure[Try]

      logger.expectNoLogs()
    }

    "succeed with HookMissing and delete the access token if there's no hook" in new TestCase {

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), givenAccessToken, returning = false.pure[Try])

      givenTokenRemoving(returning = ().pure[Try])

      validator.validateHook(projectId, givenAccessToken.some) shouldBe HookMissing.pure[Try]

      logger.expectNoLogs()
    }

    "fail if hook verification step fails" in new TestCase {

      val exception = exceptions.generateOne
      val error     = exception.raiseError[Try, Nothing]
      givenHookVerification(HookIdentifier(projectId, projectHookUrl), givenAccessToken, returning = error)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token re-association fails" in new TestCase {

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), givenAccessToken, returning = true.pure[Try])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[Try, Nothing]
      givenTokenAssociation(givenAccessToken, returning = error)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token removal fails" in new TestCase {

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), givenAccessToken, returning = false.pure[Try])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[Try, Nothing]
      givenTokenRemoving(returning = error)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }
  }

  "validateHook - given valid stored access token" should {

    "succeed with HookExists if there's a hook" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[Try])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = true.pure[Try])

      validator.validateHook(projectId, None) shouldBe HookExists.pure[Try]

      logger.expectNoLogs()
    }

    "succeed with HookMissing and delete the stored access token if there's no hook" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[Try])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = false.pure[Try])

      givenTokenRemoving(returning = ().pure[Try])

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe HookMissing.pure[Try]

      logger.expectNoLogs()
    }

    "fail if verifying hook validity fails" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[Try])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[Try, Nothing]
      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = error)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token removal fails" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[Try])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = false.pure[Try])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[Try, Nothing]
      givenTokenRemoving(returning = error)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }
  }

  private trait TestCase {
    val givenAccessToken = accessTokens.generateOne
    val projectHookUrl   = projectHookUrls.generateOne
    val projectId        = projectIds.generateOne

    private val projectHookVerifier   = mock[ProjectHookVerifier[Try]]
    private val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    private val accessTokenAssociator = mock[AccessTokenAssociator[Try]]
    private val accessTokenRemover    = mock[AccessTokenRemover[Try]]
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val validator = new HookValidatorImpl[Try](
      projectHookUrl,
      projectHookVerifier,
      accessTokenFinder,
      accessTokenAssociator,
      accessTokenRemover
    )

    def givenAccessTokenFinding(returning: Try[Option[AccessToken]]) =
      (accessTokenFinder
        .findAccessToken(_: GitLabId)(_: GitLabId => String))
        .expects(projectId, projectIdToPath)
        .returning(returning)

    def givenHookVerification(identifier: HookIdentifier, accessToken: AccessToken, returning: Try[Boolean]) =
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(identifier, accessToken)
        .returning(returning)

    def givenTokenAssociation(accessToken: AccessToken, returning: Try[Unit]) =
      (accessTokenAssociator
        .associate(_: GitLabId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(returning)

    def givenTokenRemoving(returning: Try[Unit]) =
      (accessTokenRemover
        .removeAccessToken(_: GitLabId))
        .expects(projectId)
        .returning(returning)
  }
}
