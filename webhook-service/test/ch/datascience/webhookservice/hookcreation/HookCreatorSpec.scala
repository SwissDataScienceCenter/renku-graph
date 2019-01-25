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

package ch.datascience.webhookservice.hookcreation

import cats._
import cats.implicits._
import ch.datascience.clients.AccessToken
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events.ProjectId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.interpreters.TestLogger.Matcher.NotRefEqual
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.crypto.HookTokenCrypto.{Secret, SerializedHookToken}
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.model.{HookToken, ProjectInfo}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.higherKinds
import scala.util.Try

class HookCreatorSpec extends WordSpec with MockFactory {

  "createHook" should {

    "succeed if both hook's access token and the hook itself were successfully created" in new TestCase {

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookAccessTokenVerifier
        .checkHookAccessTokenPresence(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(false))

      (hookAccessTokenCreator
        .createHookAccessToken(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(hookAccessToken))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId, hookAccessToken))
        .returning(context.pure(serializedHookToken))

      (gitLabHookCreation
        .createHook(_: ProjectId, _: AccessToken, _: SerializedHookToken))
        .expects(projectId, accessToken, serializedHookToken)
        .returning(context.pure(()))

      hookCreation.createHook(projectId, accessToken) shouldBe context.pure(())

      logger.loggedOnly(Info(s"Hook created for project with id $projectId"))
    }

    "log an error if hook's access token was already created" in new TestCase {

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookAccessTokenVerifier
        .checkHookAccessTokenPresence(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(true))

      val result = hookCreation.createHook(projectId, accessToken)

      val expectationException = new RuntimeException(s"Hook already created for the project $projectId")
      result.failed.foreach { exception =>
        exception            shouldBe a[RuntimeException]
        exception.getMessage shouldBe expectationException.getMessage
      }

      logger.loggedOnly(
        Error(s"Hook creation failed for project with id $projectId", NotRefEqual(expectationException))
      )
    }

    "log an error if project info fetching fails" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook access token fetching fails" in new TestCase {

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (hookAccessTokenVerifier
        .checkHookAccessTokenPresence(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook access token creation fails" in new TestCase {

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookAccessTokenVerifier
        .checkHookAccessTokenPresence(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(false))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (hookAccessTokenCreator
        .createHookAccessToken(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook token encryption fails" in new TestCase {

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookAccessTokenVerifier
        .checkHookAccessTokenPresence(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(false))

      (hookAccessTokenCreator
        .createHookAccessToken(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(hookAccessToken))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId, hookAccessToken))
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook creation fails" in new TestCase {

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookAccessTokenVerifier
        .checkHookAccessTokenPresence(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(false))

      (hookAccessTokenCreator
        .createHookAccessToken(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(context.pure(hookAccessToken))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId, hookAccessToken))
        .returning(context.pure(serializedHookToken))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (gitLabHookCreation
        .createHook(_: ProjectId, _: AccessToken, _: SerializedHookToken))
        .expects(projectId, accessToken, serializedHookToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }
  }

  private trait TestCase {
    val projectInfo         = projectInfos.generateOne
    val projectId           = projectInfo.id
    val accessToken         = accessTokens.generateOne
    val hookAccessToken     = hookAccessTokens.generateOne
    val serializedHookToken = serializedHookTokens.generateOne

    val context = MonadError[Try, Throwable]

    val logger                  = TestLogger[Try]()
    val projectInfoFinder       = mock[ProjectInfoFinder[Try]]
    val hookAccessTokenVerifier = mock[HookAccessTokenVerifier[Try]]
    val hookAccessTokenCreator  = mock[HookAccessTokenCreator[Try]]
    val gitLabHookCreation      = mock[HookCreationRequestSender[Try]]

    class TryHookTokenCrypt(secret: Secret) extends HookTokenCrypto[Try](secret)
    val hookTokenCrypto = mock[TryHookTokenCrypt]

    val hookCreation = new HookCreator[Try](
      projectInfoFinder,
      hookAccessTokenVerifier,
      hookAccessTokenCreator,
      gitLabHookCreation,
      logger,
      hookTokenCrypto
    )
  }
}
