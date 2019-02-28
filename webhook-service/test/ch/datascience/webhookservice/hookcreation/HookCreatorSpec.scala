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
import ch.datascience.http.client.AccessToken
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.pushevent.PushEventSender
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult.{HookCreated, HookExisted}
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import ch.datascience.webhookservice.hookvalidation.TryHookValidator
import ch.datascience.webhookservice.model.{HookToken, ProjectInfo}
import ch.datascience.webhookservice.project._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.higherKinds
import scala.util.Try

class HookCreatorSpec extends WordSpec with MockFactory {

  "createHook" should {

    "return HookCreated if hook does not exists and it was successfully created" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.pure(()))

      expectEventsHistoryLoader(returning = context.pure(()))

      hookCreation.createHook(projectId, accessToken) shouldBe context.pure(HookCreated)

      logger.loggedOnly(Info(s"Hook created for project with id $projectId"))
    }

    "return HookExisted if hook was already created for that project" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookExists))

      hookCreation.createHook(projectId, accessToken) shouldBe context.pure(HookExisted)

      logger.loggedOnly(Info(s"Hook already created for projectId: $projectId, url: $projectHookUrl"))
    }

    "log an error if finding project hook url fails" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook validation fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if project info fetching fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookMissing))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook token encryption fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook creation fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if loading all events fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.pure(()))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      expectEventsHistoryLoader(returning = error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }
  }

  private trait TestCase {
    val projectInfo         = projectInfos.generateOne
    val projectId           = projectInfo.id
    val projectHookUrl      = projectHookUrls.generateOne
    val serializedHookToken = serializedHookTokens.generateOne
    val accessToken         = accessTokens.generateOne

    val context = MonadError[Try, Throwable]

    val logger               = TestLogger[Try]()
    val projectInfoFinder    = mock[ProjectInfoFinder[Try]]
    val projectHookValidator = mock[TryHookValidator]
    val projectHookCreator   = mock[ProjectHookCreator[Try]]
    val projectHookUrlFinder = mock[TryProjectHookUrlFinder]

    class TryHookTokenCrypt(secret: Secret) extends HookTokenCrypto[Try](secret)
    val hookTokenCrypto = mock[TryHookTokenCrypt]
    class TryEventsHistoryLoader(latestPushEventFetcher: LatestPushEventFetcher[Try],
                                 userInfoFinder:         UserInfoFinder[Try],
                                 pushEventSender:        PushEventSender[Try])
        extends EventsHistoryLoader[Try](latestPushEventFetcher, userInfoFinder, pushEventSender, logger)
    val eventsHistoryLoader = mock[TryEventsHistoryLoader]

    val hookCreation = new HookCreator[Try](
      projectHookUrlFinder,
      projectHookValidator,
      projectInfoFinder,
      hookTokenCrypto,
      projectHookCreator,
      eventsHistoryLoader,
      logger
    )

    def expectEventsHistoryLoader(returning: Try[Unit]): Unit =
      (eventsHistoryLoader
        .loadAllEvents(_: ProjectInfo, _: AccessToken))
        .expects(projectInfo, accessToken)
        .returning(returning)
  }
}
