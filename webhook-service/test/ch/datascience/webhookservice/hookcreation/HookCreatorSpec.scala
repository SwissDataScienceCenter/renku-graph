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
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events.ProjectId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.pushevent.PushEventSender
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.hookcreation.HookCreationGenerators._
import ch.datascience.webhookservice.hookcreation.HookCreator.HookAlreadyCreated
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookcreation.ProjectHookVerifier.HookIdentifier
import ch.datascience.webhookservice.model.{HookToken, ProjectInfo}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.higherKinds
import scala.util.Try

class HookCreatorSpec extends WordSpec with MockFactory {

  "createHook" should {

    "succeed if hook does not exists and was successfully created" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(context.pure(false))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .createHook(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.pure(()))

      expectEventsHistoryLoader(returning = context.pure(()))

      hookCreation.createHook(projectId, accessToken) shouldBe context.pure(())

      logger.loggedOnly(Info(s"Hook created for project with id $projectId"))
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

    "log an Error and fail with HookAlreadyCreated if hook is already created for that project" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(context.pure(true))

      val expectedException = HookAlreadyCreated(projectId, projectHookUrl)
      hookCreation.createHook(projectId, accessToken) shouldBe context.raiseError(expectedException)

      logger.loggedOnly(Error(expectedException.getMessage))
    }

    "log an error if hook presence verification fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if project info fetching fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(context.pure(false))

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

      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(context.pure(false))

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

      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(context.pure(false))

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
        .createHook(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if loading all events fails" in new TestCase {

      (projectHookUrlFinder.findProjectHookUrl _)
        .expects()
        .returning(context.pure(projectHookUrl))

      (projectHookVerifier
        .checkProjectHookPresence(_: HookIdentifier, _: AccessToken))
        .expects(HookIdentifier(projectId, projectHookUrl), accessToken)
        .returning(context.pure(false))

      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .createHook(_: ProjectHook, _: AccessToken))
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

    val logger              = TestLogger[Try]()
    val projectInfoFinder   = mock[ProjectInfoFinder[Try]]
    val projectHookVerifier = mock[ProjectHookVerifier[Try]]
    val projectHookCreator  = mock[ProjectHookCreator[Try]]

    class TryProjectHookUrlFinder(
        selfUrlConfig: SelfUrlConfig[Try]
    )(implicit ME:     MonadError[Try, Throwable])
        extends ProjectHookUrlFinder[Try](selfUrlConfig)
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
      projectHookVerifier,
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
