/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import ProjectHookVerifier.HookIdentifier
import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import ch.datascience.webhookservice.project.ProjectVisibility._
import ch.datascience.webhookservice.project._
import ch.datascience.webhookservice.tokenrepository.{AccessTokenAssociator, AccessTokenRemover}
import io.chrisdavenport.log4cats.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Try}

class HookValidatorSpec extends WordSpec with MockFactory {

  "validateHook - finding project visibility and project access token" should {

    "fail if finding project info with the given access token fails" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Internal)
      val projectId   = projectInfo.id

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(error)

      validator.validateHook(projectId, givenAccessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }

    "fail if finding stored access token fails" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Internal)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.raiseError(UnauthorizedException))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      val storedAccessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(projectId)
        .returning(error)

      validator.validateHook(projectId, givenAccessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }

    "fail if finding project info with stored access token fails" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Internal)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.raiseError(UnauthorizedException))

      val storedAccessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(projectId)
        .returning(context.pure(Some(storedAccessToken)))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, storedAccessToken)
        .returning(error)

      validator.validateHook(projectId, givenAccessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }

    "fail if finding project info with stored access token fails with UnauthorizedException" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Internal)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.raiseError(UnauthorizedException))

      val storedAccessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(projectId)
        .returning(context.pure(Some(storedAccessToken)))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, storedAccessToken)
        .returning(context.raiseError(UnauthorizedException))

      val Failure(exception) = validator.validateHook(projectId, givenAccessToken)

      exception            shouldBe an[Exception]
      exception.getMessage shouldBe s"Stored access token for $projectId is invalid"

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }

    "fail if finding stored access token return None" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Internal)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.raiseError(UnauthorizedException))

      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(projectId)
        .returning(context.pure(None))

      val Failure(exception) = validator.validateHook(projectId, givenAccessToken)

      exception            shouldBe an[Exception]
      exception.getMessage shouldBe s"No access token found for projectId $projectId"

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }
  }

  "validateHook - Public project" should {

    "succeed with HookExists if there's a hook" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Public)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.pure(projectInfo))

      val projectHookUrl = projectHookUrls.generateOne
      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(context.pure(true))

      validator.validateHook(projectId, givenAccessToken) shouldBe context.pure(HookExists)

      logger.loggedOnly(Info(s"Hook exists for project with id $projectId"))
    }

    "succeed with HookMissing if there's no hook" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Public)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.pure(projectInfo))

      val projectHookUrl = projectHookUrls.generateOne
      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(context.pure(false))

      validator.validateHook(projectId, givenAccessToken) shouldBe context.pure(HookMissing)

      logger.loggedOnly(Info(s"Hook missing for project with id $projectId"))
    }

    "fail if finding project hook url fails" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Public)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.pure(projectInfo))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(error)

      validator.validateHook(projectId, givenAccessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }

    "fail if finding hook verification fails" in new TestCase {

      val projectInfo = projectInfos.generateOne.copy(visibility = Public)
      val projectId   = projectInfo.id

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.pure(projectInfo))

      val projectHookUrl = projectHookUrls.generateOne
      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectHookVerifier
        .checkHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
        .returning(error)

      validator.validateHook(projectId, givenAccessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
    }
  }

  Private +: Internal +: Nil foreach { visibility =>
    s"validateHook - $visibility project and valid given access token" should {

      "succeed with HookExists and re-associate access token if there's a hook" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
          .returning(context.pure(true))

        (accessTokenAssociator
          .associate(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.unit)

        validator.validateHook(projectId, givenAccessToken) shouldBe context.pure(HookExists)

        logger.loggedOnly(Info(s"Hook exists for project with id $projectId"))
      }

      "succeed with HookMissing and delete the access token if there's no hook" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
          .returning(context.pure(false))

        (accessTokenRemover
          .removeAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.unit)

        validator.validateHook(projectId, givenAccessToken) shouldBe context.pure(HookMissing)

        logger.loggedOnly(Info(s"Hook missing for project with id $projectId"))
      }

      "fail if finding project hook url fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.pure(projectInfo))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }

      "fail if finding hook verification fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }

      "fail if access token re-association fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
          .returning(context.pure(true))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (accessTokenAssociator
          .associate(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }

      "fail if access token removal fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, givenAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), givenAccessToken)
          .returning(context.pure(false))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (accessTokenRemover
          .removeAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }
    }

    s"validateHook - $visibility project and valid stored access token" should {

      "succeed with HookExists if there's a hook" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        assumeGivenAccessTokenInvalid(projectId)

        val storedAccessToken = accessTokens.generateOne
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.pure(Some(storedAccessToken)))

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, storedAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
          .returning(context.pure(true))

        validator.validateHook(projectId, givenAccessToken) shouldBe context.pure(HookExists)

        logger.loggedOnly(Info(s"Hook exists for project with id $projectId"))
      }

      "succeed with HookMissing and delete the access token if there's no hook" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        assumeGivenAccessTokenInvalid(projectId)

        val storedAccessToken = accessTokens.generateOne
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.pure(Some(storedAccessToken)))

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, storedAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
          .returning(context.pure(false))

        (accessTokenRemover
          .removeAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.unit)

        validator.validateHook(projectId, givenAccessToken) shouldBe context.pure(HookMissing)

        logger.loggedOnly(Info(s"Hook missing for project with id $projectId"))
      }

      "fail if finding project hook url fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        assumeGivenAccessTokenInvalid(projectId)

        val storedAccessToken = accessTokens.generateOne
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.pure(Some(storedAccessToken)))

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, storedAccessToken)
          .returning(context.pure(projectInfo))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }

      "fail if finding hook verification fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        assumeGivenAccessTokenInvalid(projectId)

        val storedAccessToken = accessTokens.generateOne
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.pure(Some(storedAccessToken)))

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, storedAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }

      "fail if access token removal fails" in new TestCase {

        val projectInfo = projectInfos.generateOne.copy(visibility = visibility)
        val projectId   = projectInfo.id

        assumeGivenAccessTokenInvalid(projectId)

        val storedAccessToken = accessTokens.generateOne
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(context.pure(Some(storedAccessToken)))

        (projectInfoFinder
          .findProjectInfo(_: ProjectId, _: AccessToken))
          .expects(projectId, storedAccessToken)
          .returning(context.pure(projectInfo))

        val projectHookUrl = projectHookUrls.generateOne
        (projectHookUrlFinder.findProjectHookUrl _)
          .expects()
          .returning(context.pure(projectHookUrl))

        (projectHookVerifier
          .checkHookPresence(_: HookIdentifier, _: AccessToken))
          .expects(HookIdentifier(projectId, projectHookUrl), storedAccessToken)
          .returning(context.pure(false))

        val exception: Exception    = exceptions.generateOne
        val error:     Try[Nothing] = context.raiseError(exception)
        (accessTokenRemover
          .removeAccessToken(_: ProjectId))
          .expects(projectId)
          .returning(error)

        validator.validateHook(projectId, givenAccessToken) shouldBe error

        logger.loggedOnly(Error(s"Hook validation fails for project with id $projectId", exception))
      }
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val givenAccessToken = accessTokens.generateOne

    val projectInfoFinder     = mock[ProjectInfoFinder[Try]]
    val projectHookUrlFinder  = mock[TryProjectHookUrlFinder]
    val projectHookVerifier   = mock[ProjectHookVerifier[Try]]
    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val accessTokenAssociator = mock[AccessTokenAssociator[Try]]
    val accessTokenRemover    = mock[AccessTokenRemover[Try]]
    val logger                = TestLogger[Try]()
    val validator = new HookValidator[Try](
      projectInfoFinder,
      projectHookUrlFinder,
      projectHookVerifier,
      accessTokenFinder,
      accessTokenAssociator,
      accessTokenRemover,
      logger
    )

    def assumeGivenAccessTokenInvalid(projectId: ProjectId): Unit =
      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, givenAccessToken)
        .returning(context.raiseError(UnauthorizedException))
  }
}

class TryHookValidator(
    projectInfoFinder:     ProjectInfoFinder[Try],
    projectHookUrlFinder:  ProjectHookUrlFinder[Try],
    projectHookVerifier:   ProjectHookVerifier[Try],
    accessTokenFinder:     AccessTokenFinder[Try],
    accessTokenAssociator: AccessTokenAssociator[Try],
    accessTokenRemover:    AccessTokenRemover[Try],
    logger:                Logger[Try]
) extends HookValidator[Try](projectInfoFinder,
                               projectHookUrlFinder,
                               projectHookVerifier,
                               accessTokenFinder,
                               accessTokenAssociator,
                               accessTokenRemover,
                               logger)
