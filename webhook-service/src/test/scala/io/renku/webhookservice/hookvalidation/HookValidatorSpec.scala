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
import cats.effect.testing.scalatest.AsyncIOSpec
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
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.model.HookIdentifier
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class HookValidatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with BeforeAndAfterEach {
  import AccessTokenFinder.Implicits._

  "validateHook - finding access token" should {

    "return None if finding stored access token returns None when no access token given" in {

      givenAccessTokenFinding(returning = Option.empty[AccessToken].pure[IO])

      validator.validateHook(projectId, maybeAccessToken = None).asserting(_ shouldBe None) >>
        logger.expectNoLogsF()
    }

    "fail if finding stored access token fails and no access token given" in {

      val exception = exceptions.generateOne
      givenAccessTokenFinding(returning = exception.raiseError[IO, Option[AccessToken]])

      validator
        .validateHook(projectId, maybeAccessToken = None)
        .assertThrowsWithMessage[Exception](show"Finding stored access token for $projectId failed") >>
        logger
          .getMessagesF(Error)
          .asserting(_.map(_.message) shouldBe List(s"Hook validation failed for projectId $projectId"))
    }
  }

  "validateHook - given access token" should {

    "re-associate access token and succeed with HookExists if there's a valid hook" in {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = true.some.pure[IO]
      )

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      validator.validateHook(projectId, givenAccessToken.some).asserting(_ shouldBe HookExists.some) >>
        logger.expectNoLogsF()
    }

    "re-associate access token and succeed with HookMissing if there's no hook" in {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = false.some.pure[IO]
      )

      validator.validateHook(projectId, givenAccessToken.some).asserting(_ shouldBe HookMissing.some) >>
        logger.expectNoLogsF()
    }

    "return None if hook verification cannot determine hook existence" in {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = None.pure[IO])

      validator.validateHook(projectId, givenAccessToken.some).asserting(_ shouldBe None) >>
        logger.expectNoLogsF()
    }

    "fail if hook verification step fails" in {

      givenTokenAssociation(givenAccessToken, returning = ().pure[IO])

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      val exception = exceptions.generateOne
      val error     = exception.raiseError[IO, Nothing]
      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = error)

      validator.validateHook(projectId, givenAccessToken.some).assertThrowsError[Exception](_ shouldBe exception) >>
        logger.loggedOnlyF(Error(s"Hook validation failed for projectId $projectId", exception))
    }

    "fail if access token re-association fails" in {

      val exception = exceptions.generateOne
      givenTokenAssociation(givenAccessToken, returning = exception.raiseError[IO, Nothing])

      validator.validateHook(projectId, givenAccessToken.some).assertThrowsError[Exception](_ shouldBe exception) >>
        logger.loggedOnlyF(Error(s"Hook validation failed for projectId $projectId", exception))
    }
  }

  "validateHook - given stored access token but no given token" should {

    "succeed with HookExists if there's a hook" in {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = true.some.pure[IO]
      )

      validator.validateHook(projectId, maybeAccessToken = None).asserting(_ shouldBe HookExists.some) >>
        logger.expectNoLogsF()
    }

    "succeed with HookMissing if there's no hook" in {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = false.some.pure[IO]
      )

      validator.validateHook(projectId, maybeAccessToken = None).asserting(_ shouldBe HookMissing.some) >>
        logger.expectNoLogsF()
    }

    "return None if hook verification cannot determine hook existence" in {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      givenHookVerification(HookIdentifier(projectId, projectHookUrl), storedAccessToken, returning = None.pure[IO])

      validator.validateHook(projectId, maybeAccessToken = None).asserting(_ shouldBe None) >>
        logger.expectNoLogsF()
    }

    "fail if verifying hook validity fails" in {

      val storedAccessToken = accessTokens.generateOne
      givenAccessTokenFinding(returning = storedAccessToken.some.pure[IO])

      val exception = exceptions.generateOne
      givenHookVerification(HookIdentifier(projectId, projectHookUrl),
                            storedAccessToken,
                            returning = exception.raiseError[IO, Nothing]
      )

      validator.validateHook(projectId, maybeAccessToken = None).assertThrowsError[Exception](_ shouldBe exception) >>
        logger.loggedOnlyF(Error(s"Hook validation failed for projectId $projectId", exception))
    }
  }

  private lazy val givenAccessToken = accessTokens.generateOne
  private lazy val projectHookUrl   = projectHookUrls.generateOne
  private lazy val projectId        = projectIds.generateOne

  private val projectHookVerifier   = mock[ProjectHookVerifier[IO]]
  private val accessTokenFinder     = mock[AccessTokenFinder[IO]]
  private val accessTokenAssociator = mock[AccessTokenAssociator[IO]]
  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val validator = new HookValidatorImpl[IO](
    projectHookUrl,
    projectHookVerifier,
    accessTokenFinder,
    accessTokenAssociator,
    Cache.noop[IO, GitLabId, HookValidationResult]
  )

  private def givenAccessTokenFinding(returning: IO[Option[AccessToken]]) =
    (accessTokenFinder
      .findAccessToken(_: GitLabId)(_: GitLabId => String))
      .expects(projectId, projectIdToPath)
      .returning(returning)

  private def givenHookVerification(identifier:  HookIdentifier,
                                    accessToken: AccessToken,
                                    returning:   IO[Option[Boolean]]
  ) = (projectHookVerifier
    .checkHookPresence(_: HookIdentifier, _: AccessToken))
    .expects(identifier, accessToken)
    .returning(returning)

  private def givenTokenAssociation(accessToken: AccessToken, returning: IO[Unit]) =
    (accessTokenAssociator
      .associate(_: GitLabId, _: AccessToken))
      .expects(projectId, accessToken)
      .returning(returning)

  protected override def beforeEach() = {
    super.beforeEach()
    logger.reset()
  }
}
