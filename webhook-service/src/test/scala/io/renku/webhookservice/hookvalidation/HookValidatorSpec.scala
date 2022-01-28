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

package io.renku.webhookservice.hookvalidation

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.projects.Id
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.AccessToken
import io.renku.http.client.RestClientError.UnauthorizedException
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

import scala.util.{Failure, Try}

class HookValidatorSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import AccessTokenFinder._

  s"validateHook - finding access token" should {

    "fail if finding stored access token fails when no access token given" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = exception.raiseError[Try, Nothing]
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(error)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }

    "fail if finding stored access token return NOT_FOUND when no access token given" in new TestCase {

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(Option.empty[AccessToken].pure[Try])

      val noAccessTokenException = NoAccessTokenException(s"No access token found for projectId $projectId")
      validator.validateHook(projectId, maybeAccessToken = None) shouldBe noAccessTokenException
        .raiseError[Try, Nothing]

      logger.loggedOnly(Info(s"Hook validation failed: ${noAccessTokenException.getMessage}"))
    }

    "fail if finding stored access token fails with UnauthorizedException" in new TestCase {

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(UnauthorizedException.raiseError[Try, Option[AccessToken]])

      val Failure(exception) = validator.validateHook(projectId, maybeAccessToken = None)

      exception            shouldBe an[Exception]
      exception.getMessage shouldBe s"Stored access token for $projectId is invalid"

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }
  }

  s"validateHook - given access token" should {

    "succeed with HookExists and re-associate access token if there's a hook" in new TestCase {
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(true.pure[Try])

      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(().pure[Try])

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe HookExists.pure[Try]

      logger.expectNoLogs()
    }

    "succeed with HookMissing and delete the access token if there's no hook" in new TestCase {

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(false.pure[Try])

      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(().pure[Try])

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe HookMissing.pure[Try]

      logger.expectNoLogs()
    }

    "fail if finding hook verification fails" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = exception.raiseError[Try, Nothing]
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(error)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }

    "fail if access token re-association fails" in new TestCase {
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(true.pure[Try])

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = exception.raiseError[Try, Nothing]
      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(error)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }

    "fail if access token removal fails" in new TestCase {
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(false.pure[Try])

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = exception.raiseError[Try, Nothing]
      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(error)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }
  }

  s"validateHook - given valid stored access token" should {

    "succeed with HookExists if there's a hook" in new TestCase {

      val storedAccessToken = assumeGivenAccessTokenInvalid()

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
        .returning(true.pure[Try])

      validator.validateHook(projectId, None) shouldBe HookExists.pure[Try]

      logger.expectNoLogs()
    }
    "succeed with HookMissing and delete the stored access token if there's no hook" in new TestCase {

      val storedAccessToken = assumeGivenAccessTokenInvalid()

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
        .returning(false.pure[Try])

      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(().pure[Try])

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe HookMissing.pure[Try]

      logger.expectNoLogs()
    }

    "fail if finding hook verification fails" in new TestCase {

      val storedAccessToken = assumeGivenAccessTokenInvalid()

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = exception.raiseError[Try, Nothing]
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
        .returning(error)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }

    "fail if access token removal fails" in new TestCase {

      val storedAccessToken = assumeGivenAccessTokenInvalid()

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
        .returning(false.pure[Try])

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = exception.raiseError[Try, Nothing]
      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(error)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }
  }

  private trait TestCase {
    val givenAccessToken = accessTokens.generateOne
    val projectHookUrl   = projectHookUrls.generateOne

    val projectInfo = projectInfos.generateOne
    val projectId   = projectInfo.id

    val projectHookVerifier   = mock[ProjectHookVerifier[Try]]
    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val accessTokenAssociator = mock[AccessTokenAssociator[Try]]
    val accessTokenRemover    = mock[AccessTokenRemover[Try]]
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val validator = new HookValidatorImpl[Try](
      projectHookUrl,
      projectHookVerifier,
      accessTokenFinder,
      accessTokenAssociator,
      accessTokenRemover
    )

    def assumeGivenAccessTokenInvalid(): AccessToken = {
      val storedAccessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(Some(storedAccessToken).pure[Try])
      storedAccessToken
    }
  }
}
