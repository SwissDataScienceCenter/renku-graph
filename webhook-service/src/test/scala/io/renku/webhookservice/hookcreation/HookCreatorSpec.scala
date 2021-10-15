/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.hookcreation

import cats._
import cats.effect.{ConcurrentEffect, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.renku.webhookservice.CommitSyncRequestSender
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.crypto.IOHookTokenCrypto
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.hookcreation.project.ProjectInfoFinder
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.model.{CommitSyncRequest, HookToken, Project}
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class HookCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createHook" should {

    "return HookCreated if hook does not exists and it was successfully created" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.unit)

      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.unit)

      (commitSyncRequestSender.sendCommitSyncRequest _)
        .expects(CommitSyncRequest(Project(projectInfo.id, projectInfo.path)))
        .returning(().pure[IO])

      hookCreation.createHook(projectId, accessToken).unsafeRunSync() shouldBe HookCreated

      logger.expectNoLogs()
    }

    "return HookExisted if hook was already created for that project" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookExists))

      hookCreation.createHook(projectId, accessToken).unsafeRunSync() shouldBe HookExisted

      logger.expectNoLogs()
    }

    "log an error if hook validation fails" in new TestCase {

      val exception = exceptions.generateOne
      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.raiseError(exception))

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if project info fetching fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookMissing))

      val exception = exceptions.generateOne
      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.raiseError(exception))

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook token encryption fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(projectInfo))

      val exception = exceptions.generateOne
      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.raiseError(exception))

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook creation fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      val exception = exceptions.generateOne
      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.raiseError(exception))

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if associating projectId with accessToken fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.unit)

      val exception = exceptions.generateOne
      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.raiseError(exception))

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "fail return either HookExisted/HookCreated if loading all events fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(HookMissing))

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(projectInfo))

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(context.pure(serializedHookToken))

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(context.unit)

      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.unit)

      (commitSyncRequestSender.sendCommitSyncRequest _)
        .expects(CommitSyncRequest(Project(projectInfo.id, projectInfo.path)))
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      hookCreation.createHook(projectId, accessToken).unsafeRunSync() shouldBe HookCreated

      logger.expectNoLogs()
    }
  }

  private implicit val contextShift: ContextShift[IO]     = IO.contextShift(ExecutionContext.global)
  private implicit val concurrent:   ConcurrentEffect[IO] = IO.ioConcurrentEffect

  private trait TestCase {
    val projectInfo         = projectInfos.generateOne
    val projectId           = projectInfo.id
    val projectHookUrl      = projectHookUrls.generateOne
    val serializedHookToken = serializedHookTokens.generateOne
    val accessToken         = accessTokens.generateOne

    val context: MonadError[IO, Throwable] = MonadError[IO, Throwable]

    val logger                  = TestLogger[IO]()
    val projectInfoFinder       = mock[ProjectInfoFinder[IO]]
    val projectHookValidator    = mock[HookValidator[IO]]
    val projectHookCreator      = mock[ProjectHookCreator[IO]]
    val hookTokenCrypto         = mock[IOHookTokenCrypto]
    val accessTokenAssociator   = mock[AccessTokenAssociator[IO]]
    val commitSyncRequestSender = mock[CommitSyncRequestSender[IO]]

    val hookCreation = new HookCreatorImpl[IO](
      projectHookUrl,
      projectHookValidator,
      projectInfoFinder,
      hookTokenCrypto,
      projectHookCreator,
      accessTokenAssociator,
      commitSyncRequestSender,
      logger
    )
  }
}
