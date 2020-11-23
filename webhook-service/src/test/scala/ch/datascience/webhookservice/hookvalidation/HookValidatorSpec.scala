/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookvalidation

import cats.MonadError
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import ch.datascience.webhookservice.hookvalidation.HookValidator.NoAccessTokenException
import ch.datascience.webhookservice.hookvalidation.ProjectHookVerifier.HookIdentifier
import ch.datascience.webhookservice.project._
import ch.datascience.webhookservice.tokenrepository.{AccessTokenAssociator, AccessTokenRemover}
import io.chrisdavenport.log4cats.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class HookValidatorSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import IOAccessTokenFinder._

  s"validateHook - finding access token" should {

    "fail if finding stored access token fails when no access token given" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
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
        .returning(context.pure(None))

      val noAccessTokenException = NoAccessTokenException(s"No access token found for projectId $projectId")
      validator.validateHook(projectId, maybeAccessToken = None) shouldBe context.raiseError(noAccessTokenException)

      logger.loggedOnly(Info(s"Hook validation failed: ${noAccessTokenException.getMessage}"))
    }

    "fail if finding stored access token fails with UnauthorizedException" in new TestCase {

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(context.raiseError(UnauthorizedException))

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
        .returning(context.pure(true))

      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.unit)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe context.pure(HookExists)

      logger.expectNoLogs()
    }

    "succeed with HookMissing and delete the access token if there's no hook" in new TestCase {

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(context.pure(false))

      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(context.unit)

      validator.validateHook(projectId, Some(givenAccessToken)) shouldBe context.pure(HookMissing)

      logger.expectNoLogs()
    }

    "fail if finding hook verification fails" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
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
        .returning(context.pure(true))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
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
        .returning(context.pure(false))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
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
        .returning(context.pure(true))

      validator.validateHook(projectId, None) shouldBe context.pure(HookExists)

      logger.expectNoLogs()
    }
    "succeed with HookMissing and delete the stored access token if there's no hook" in new TestCase {

      val storedAccessToken = assumeGivenAccessTokenInvalid()

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
        .returning(context.pure(false))

      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(context.unit)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe context.pure(HookMissing)

      logger.expectNoLogs()
    }

    "fail if finding hook verification fails" in new TestCase {

      val storedAccessToken = assumeGivenAccessTokenInvalid()

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
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
        .returning(context.pure(false))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (accessTokenRemover
        .removeAccessToken(_: Id))
        .expects(projectId)
        .returning(error)

      validator.validateHook(projectId, maybeAccessToken = None) shouldBe error

      logger.loggedOnly(Error(s"Hook validation failed for project with id $projectId", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val givenAccessToken = accessTokens.generateOne
    val projectHookUrl   = projectHookUrls.generateOne

    val projectInfo = projectInfos.generateOne
    val projectId   = projectInfo.id

    val projectHookVerifier   = mock[ProjectHookVerifier[Try]]
    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val accessTokenAssociator = mock[AccessTokenAssociator[Try]]
    val accessTokenRemover    = mock[AccessTokenRemover[Try]]
    val logger                = TestLogger[Try]()
    val validator = new HookValidator[Try](
      projectHookUrl,
      projectHookVerifier,
      accessTokenFinder,
      accessTokenAssociator,
      accessTokenRemover,
      logger
    )

    def assumeGivenAccessTokenInvalid(): AccessToken = {
      val storedAccessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(context.pure(Some(storedAccessToken)))
      storedAccessToken
    }
  }
}

class TryHookValidator(
    projectHookUrl:        ProjectHookUrl,
    projectHookVerifier:   ProjectHookVerifier[Try],
    accessTokenFinder:     AccessTokenFinder[Try],
    accessTokenAssociator: AccessTokenAssociator[Try],
    accessTokenRemover:    AccessTokenRemover[Try],
    logger:                Logger[Try]
) extends HookValidator[Try](projectHookUrl,
                             projectHookVerifier,
                             accessTokenFinder,
                             accessTokenAssociator,
                             accessTokenRemover,
                             logger
    )
