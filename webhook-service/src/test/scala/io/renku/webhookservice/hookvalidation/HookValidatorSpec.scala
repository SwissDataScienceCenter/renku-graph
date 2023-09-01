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

import cats.effect.IO
import cats.syntax.all._
import io.renku.cache.Cache
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects.GitLabId
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.model.HookIdentifier
import io.renku.webhookservice.tokenrepository.{AccessTokenAssociator, AccessTokenRemover}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookValidatorSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {
  import AccessTokenFinder.Implicits._

  "validateHook - finding access token" should {

    "return None if finding stored access token returns None when no access token given" in new TestCase {

      givenAccessTokenFinding(returning = Option.empty[AccessToken].pure[IO])

      validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync() shouldBe None

      logger.expectNoLogs()
    }

    "fail if finding stored access token fails and no access token given" in new TestCase {

      val exception = exceptions.generateOne
      givenAccessTokenFinding(returning = exception.raiseError[IO, Option[AccessToken]])

      val result =
        intercept[Exception] {
          validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync()
        }

      result.getMessage shouldBe show"Finding stored access token for $projectId failed"
      result.getCause   shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", result))
    }
  }

  "validateHook - given access token" should {

    "re-associate access token and succeed with HookExists if there's a valid hook" in new TestCase {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = true.some.pure[IO]
      )

      validator.validateHook(projectId, givenAccessToken.some).unsafeRunSync() shouldBe HookExists.some

      logger.expectNoLogs()
    }

    "re-associate access token, delete the access token and succeed with HookMissing if there's no hook" in new TestCase {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = false.some.pure[IO]
      )

      givenTokenRemoving(givenAccessToken.some, returning = ().pure[IO])

      validator.validateHook(projectId, givenAccessToken.some).unsafeRunSync() shouldBe HookMissing.some

      logger.loggedOnly(Info(show"Token removed for projectId = $projectId as hook doesn't exist"))
    }

    "return None if hook verification cannot determine hook existence" in new TestCase {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = None.pure[IO])

      validator.validateHook(projectId, givenAccessToken.some).unsafeRunSync() shouldBe None

      logger.expectNoLogs()
    }

    "fail if hook verification step fails" in new TestCase {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[IO, Nothing]
      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = error)

      intercept[Exception](validator.validateHook(projectId, givenAccessToken.some).unsafeRunSync()) shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token re-association fails" in new TestCase {

      val exception = exceptions.generateOne
      val error     = exception.raiseError[IO, Nothing]
      givenTokenAssociation(givenAccessToken, returning = error)

      intercept[Exception](validator.validateHook(projectId, givenAccessToken.some).unsafeRunSync()) shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token removal fails" in new TestCase {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = false.some.pure[IO]
      )

      val exception = exceptions.generateOne
      val error     = exception.raiseError[IO, Nothing]
      givenTokenRemoving(givenAccessToken.some, returning = error)

      intercept[Exception](validator.validateHook(projectId, givenAccessToken.some).unsafeRunSync()) shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }
  }

  "validateHook - given stored access token but no given token" should {

    "succeed with HookExists if there's a hook" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = true.some.pure[IO]
      )

      validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync() shouldBe HookExists.some

      logger.expectNoLogs()
    }

    "succeed with HookMissing and delete the stored access token if there's no hook" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = false.some.pure[IO]
      )

      givenTokenRemoving(accessToken = None, returning = ().pure[IO])

      validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync() shouldBe HookMissing.some

      logger.loggedOnly(Info(show"Token removed for projectId = $projectId as hook doesn't exist"))
    }

    "return None if hook verification cannot determine hook existence" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = None.pure[IO])

      validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync() shouldBe None

      logger.expectNoLogs()
    }

    "fail if verifying hook validity fails" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[IO, Nothing]
      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = error)

      intercept[Exception](
        validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync()
      ) shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token removal fails" in new TestCase {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = false.some.pure[IO]
      )

      val exception = exceptions.generateOne
      val error     = exception.raiseError[IO, Nothing]
      givenTokenRemoving(accessToken = None, returning = error)

      intercept[Exception](
        validator.validateHook(projectId, maybeAccessToken = None).unsafeRunSync()
      ) shouldBe exception

      logger.loggedOnly(Error(s"Hook validation failed for projectId $projectId", exception))
    }
  }

  private trait TestCase {
    val givenAccessToken = accessTokens.generateOne
    val projectHookUrl   = projectHookUrls.generateOne
    val projectId        = projectIds.generateOne

    private val projectHookVerifier   = mock[ProjectHookVerifier[IO]]
    private val accessTokenFinder     = mock[AccessTokenFinder[IO]]
    private val accessTokenAssociator = mock[AccessTokenAssociator[IO]]
    private val accessTokenRemover    = mock[AccessTokenRemover[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val validator = new HookValidatorImpl[IO](
      projectHookUrl,
      projectHookVerifier,
      accessTokenFinder,
      accessTokenAssociator,
      accessTokenRemover,
      Cache.noop[IO, GitLabId, HookValidationResult]
    )

    def givenAccessTokenFinding(returning: IO[Option[AccessToken]]) =
      (accessTokenFinder
        .findAccessToken(_: GitLabId)(_: GitLabId => String))
        .expects(projectId, projectIdToPath)
        .returning(returning)

    def givenHookVerification(identifier: HookIdentifier, accessToken: AccessToken, returning: IO[Option[Boolean]]) =
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(identifier, accessToken)
        .returning(returning)

    def givenTokenAssociation(accessToken: AccessToken, returning: IO[Unit]) =
      (accessTokenAssociator
        .associate(_: GitLabId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(returning)

    def givenTokenRemoving(accessToken: Option[AccessToken], returning: IO[Unit]) =
      (accessTokenRemover.removeAccessToken _)
        .expects(projectId, accessToken)
        .returning(returning)
  }
}
